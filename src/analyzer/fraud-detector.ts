/**
 * Fraud Detection Analyzer - Consolidated
 * Detects structuring, duplicates, and other fraud patterns
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Fraud detection thresholds
const STRUCTURING_THRESHOLD = 10000; 
const NEAR_THRESHOLD_PERCENT = 0.9;  

interface Payment {
  id: number;
  provider_master_id: number;
  provider_name?: string;
  amount: number;
  fiscal_year: number;
  payment_date?: string;
}

interface Expenditure {
  id: number;
  provider_master_id?: number;
  contractor_id?: number;
  vendor_name: string;
  amount: number;
  fiscal_year: number;
  payment_date?: string;
}

interface StructuringFlag {
  type: 'split_payments' | 'near_threshold';
  vendor: string;
  vendor_id?: number;
  total_amount: number;
  transaction_count: number;
  details: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
}

interface DuplicateFlag {
  vendor: string;
  amount: number;
  date: string;
  count: number;
  transaction_ids: number[];
}

/**
 * Detect potential structuring
 */
export async function detectStructuring(threshold = STRUCTURING_THRESHOLD): Promise<StructuringFlag[]> {
  await initializeDb();
  const flags: StructuringFlag[] = [];
  
  const payments = await query<Payment>(`
    SELECT 
      p.id, p.provider_master_id, p.amount, p.fiscal_year,
      pr.name_display as provider_name
    FROM payments p
    JOIN provider_master pr ON p.provider_master_id = pr.id
    ORDER BY p.provider_master_id, p.payment_date
  `);
  
  const byProvider = new Map<number, Payment[]>();
  for (const p of payments) {
    if (!byProvider.has(p.provider_master_id)) byProvider.set(p.provider_master_id, []);
    byProvider.get(p.provider_master_id)!.push(p);
  }
  
  for (const [providerId, providerPayments] of byProvider) {
    if (providerPayments.length < 2) continue;
    const providerName = providerPayments[0].provider_name || `Provider ${providerId}`;
    
    // Simple logic: total per FY
    const byFY = new Map<number, number>();
    for (const p of providerPayments) {
      byFY.set(p.fiscal_year, (byFY.get(p.fiscal_year) || 0) + p.amount);
    }

    for (const [fy, total] of byFY) {
      if (total >= threshold) {
        flags.push({
          type: 'split_payments',
          vendor: providerName,
          vendor_id: providerId,
          total_amount: total,
          transaction_count: providerPayments.filter(p => p.fiscal_year === fy).length,
          details: `Total payments of $${total.toLocaleString()} in FY${fy} across multiple transactions.`,
          severity: total > threshold * 2 ? 'high' : 'medium',
        });
      }
    }
  }
  
  return flags;
}

/**
 * Detect duplicate transactions
 */
export async function detectDuplicates(): Promise<DuplicateFlag[]> {
  await initializeDb();
  const flags: DuplicateFlag[] = [];
  
  const duplicatePayments = await query(`
    SELECT 
      p.provider_master_id, pr.name_display as vendor, p.amount, p.payment_date as date,
      COUNT(*) as count
    FROM payments p
    JOIN provider_master pr ON p.provider_master_id = pr.id
    GROUP BY p.provider_master_id, p.amount, p.payment_date
    HAVING COUNT(*) > 1
  `);
  
  for (const row of duplicatePayments) {
    const ids = await query(`SELECT id FROM payments WHERE provider_master_id = ? AND amount = ? AND payment_date = ?`, [row.provider_master_id, row.amount, row.date]);
    flags.push({
      vendor: row.vendor,
      amount: row.amount,
      date: row.date,
      count: row.count,
      transaction_ids: ids.map(r => r.id),
    });
  }
  
  return flags;
}

/**
 * Save flags as fraud indicators
 */
export async function saveFraudIndicators(flags: any[]): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  for (const flag of flags) {
    try {
      await execute(`
        INSERT INTO fraud_indicators (
          provider_master_id, indicator_type, severity, description, status
        ) VALUES (?, ?, ?, ?, 'open')
      `, [
        flag.vendor_id || null,
        flag.type || 'duplicate',
        flag.severity || 'medium',
        flag.details || `Duplicate: ${flag.vendor}`,
      ]);
      savedCount++;
    } catch (error) {
      console.error('Error saving indicator:', error);
    }
  }
  return savedCount;
}

/**
 * Run full analysis
 */
export async function runFullFraudAnalysis() {
  console.log('Running full fraud analysis...');
  const structuring = await detectStructuring();
  const duplicates = await detectDuplicates();
  
  const savedCount = await saveFraudIndicators([...structuring, ...duplicates]);
  
  return {
    structuringCount: structuring.length,
    duplicateCount: duplicates.length,
    savedIndicators: savedCount,
  };
}

export default {
  runFullFraudAnalysis,
};
