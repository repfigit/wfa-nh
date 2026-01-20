/**
 * Fraud Detection Analyzer
 * Detects structuring, duplicates, and other fraud patterns
 * Works with both SQLite (local) and Turso (production)
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Fraud detection thresholds
const STRUCTURING_THRESHOLD = 10000; // $10k reporting threshold
const STRUCTURING_WINDOW_DAYS = 7;   // Look for split payments within 7 days
const NEAR_THRESHOLD_PERCENT = 0.9;  // Flag payments at 90%+ of threshold
const RAPID_GROWTH_PERCENT = 200;    // Flag 200%+ growth

interface Payment {
  id: number;
  provider_id: number;
  provider_name?: string;
  amount: number;
  fiscal_year: number;
  fiscal_month: number;
  payment_date?: string;
  children_served?: number;
}

interface Expenditure {
  id: number;
  provider_id?: number;
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
  window_start?: string;
  window_end?: string;
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
 * Detect potential structuring - split payments to avoid reporting thresholds
 */
export async function detectStructuring(
  threshold = STRUCTURING_THRESHOLD,
  windowDays = STRUCTURING_WINDOW_DAYS
): Promise<StructuringFlag[]> {
  await initializeDb();
  const flags: StructuringFlag[] = [];
  
  // Get all payments grouped by provider, ordered by date
  const payments = await query<Payment>(`
    SELECT 
      p.id, p.provider_id, p.amount, p.fiscal_year, p.fiscal_month,
      pr.name as provider_name
    FROM payments p
    JOIN providers pr ON p.provider_id = pr.id
    ORDER BY p.provider_id, p.fiscal_year, p.fiscal_month
  `);
  
  // Group by provider
  const byProvider = new Map<number, Payment[]>();
  
  for (const payment of payments) {
    const providerId = payment.provider_id;
    if (!byProvider.has(providerId)) {
      byProvider.set(providerId, []);
    }
    byProvider.get(providerId)!.push(payment);
  }
  
  // Analyze each provider for structuring patterns
  for (const [providerId, providerPayments] of byProvider) {
    if (providerPayments.length < 2) continue;
    
    const providerName = providerPayments[0].provider_name || `Provider ${providerId}`;
    
    // Check for split payments within the window
    for (let i = 0; i < providerPayments.length; i++) {
      let windowTotal = 0;
      let windowCount = 0;
      const windowPayments: Payment[] = [];
      
      for (let j = i; j < providerPayments.length && j < i + 3; j++) { // 3 months window
        const current = providerPayments[j];
        
        // Check if within window (simplified: consecutive months)
        if (j === i || isConsecutiveMonth(providerPayments[j-1], current)) {
          windowTotal += parseFloat(String(current.amount)) || 0;
          windowCount++;
          windowPayments.push(current);
        } else {
          break;
        }
      }
      
      // Flag if multiple small payments sum to above threshold
      if (windowCount > 1 && windowTotal >= threshold) {
        const maxSingle = Math.max(...windowPayments.map(p => parseFloat(String(p.amount)) || 0));
        
        // Only flag if individual payments are below threshold (actual structuring)
        if (maxSingle < threshold) {
          const severity = windowTotal > threshold * 2 ? 'high' : 'medium';
          
          flags.push({
            type: 'split_payments',
            vendor: providerName,
            vendor_id: providerId,
            total_amount: windowTotal,
            transaction_count: windowCount,
            window_start: `${windowPayments[0].fiscal_year}-${windowPayments[0].fiscal_month}`,
            window_end: `${windowPayments[windowCount-1].fiscal_year}-${windowPayments[windowCount-1].fiscal_month}`,
            details: `${windowCount} payments totaling $${windowTotal.toLocaleString()} within ${windowCount} months. Max single payment: $${maxSingle.toLocaleString()}`,
            severity,
          });
        }
      }
    }
    
    // Check for near-threshold payments
    for (const payment of providerPayments) {
      const amount = parseFloat(String(payment.amount)) || 0;
      if (amount >= threshold * NEAR_THRESHOLD_PERCENT && amount < threshold) {
        flags.push({
          type: 'near_threshold',
          vendor: providerName,
          vendor_id: providerId,
          total_amount: amount,
          transaction_count: 1,
          details: `Single payment of $${amount.toLocaleString()} is ${((amount / threshold) * 100).toFixed(1)}% of $${threshold.toLocaleString()} threshold`,
          severity: 'low',
        });
      }
    }
  }
  
  // Also check expenditures table
  const expenditures = await query<Expenditure>(`
    SELECT id, provider_id, contractor_id, vendor_name, amount, fiscal_year, payment_date
    FROM expenditures
    WHERE amount > 0
    ORDER BY vendor_name, fiscal_year, payment_date
  `);
  
  // Group by vendor
  const byVendor = new Map<string, Expenditure[]>();
  
  for (const exp of expenditures) {
    const key = (exp.vendor_name || '').toLowerCase();
    if (!byVendor.has(key)) {
      byVendor.set(key, []);
    }
    byVendor.get(key)!.push(exp);
  }
  
  // Check each vendor for structuring
  for (const [vendorKey, vendorExps] of byVendor) {
    if (vendorExps.length < 2) continue;
    
    // Rolling window check
    for (let i = 0; i < vendorExps.length - 1; i++) {
      let windowTotal = parseFloat(String(vendorExps[i].amount)) || 0;
      let windowCount = 1;
      
      for (let j = i + 1; j < vendorExps.length && j <= i + 3; j++) {
        windowTotal += parseFloat(String(vendorExps[j].amount)) || 0;
        windowCount++;
        
        const maxInWindow = Math.max(...vendorExps.slice(i, j+1).map(e => parseFloat(String(e.amount)) || 0));
        if (windowTotal >= threshold && maxInWindow < threshold) {
          flags.push({
            type: 'split_payments',
            vendor: vendorExps[0].vendor_name,
            vendor_id: vendorExps[0].provider_id || vendorExps[0].contractor_id,
            total_amount: windowTotal,
            transaction_count: windowCount,
            details: `Expenditures: ${windowCount} payments totaling $${windowTotal.toLocaleString()}`,
            severity: windowTotal > threshold * 2 ? 'high' : 'medium',
          });
          break;
        }
      }
    }
  }
  
  return flags;
}

/**
 * Check if two payments are in consecutive months
 */
function isConsecutiveMonth(prev: Payment, current: Payment): boolean {
  const prevYear = parseInt(String(prev.fiscal_year)) || 0;
  const prevMonth = parseInt(String(prev.fiscal_month)) || 0;
  const currYear = parseInt(String(current.fiscal_year)) || 0;
  const currMonth = parseInt(String(current.fiscal_month)) || 0;
  
  if (prevYear === currYear) {
    return currMonth === prevMonth + 1;
  }
  if (currYear === prevYear + 1) {
    return prevMonth === 12 && currMonth === 1;
  }
  return false;
}

/**
 * Detect duplicate transactions
 */
export async function detectDuplicates(): Promise<DuplicateFlag[]> {
  await initializeDb();
  const flags: DuplicateFlag[] = [];
  
  // Find duplicate payments (same provider, amount, and month)
  // Note: GROUP_CONCAT is SQLite-specific
  // The adapter handles this conversion
  const duplicatePayments = await query(`
    SELECT 
      p.provider_id, pr.name as vendor, p.amount, 
      CAST(p.fiscal_year AS TEXT) || '-' || CAST(p.fiscal_month AS TEXT) as date,
      COUNT(*) as count
    FROM payments p
    JOIN providers pr ON p.provider_id = pr.id
    GROUP BY p.provider_id, p.amount, p.fiscal_year, p.fiscal_month
    HAVING COUNT(*) > 1
  `);
  
  for (const row of duplicatePayments) {
    // Get the individual IDs for this duplicate set
    const ids = await query(`
      SELECT id FROM payments 
      WHERE provider_id = ? AND amount = ? 
      AND CAST(fiscal_year AS TEXT) || '-' || CAST(fiscal_month AS TEXT) = ?
    `, [row.provider_id, row.amount, row.date]);
    
    flags.push({
      vendor: row.vendor as string,
      amount: parseFloat(String(row.amount)) || 0,
      date: row.date as string,
      count: parseInt(String(row.count)) || 0,
      transaction_ids: ids.map(r => r.id as number),
    });
  }
  
  // Find duplicate expenditures
  const duplicateExpenditures = await query(`
    SELECT 
      vendor_name as vendor, amount, payment_date as date,
      COUNT(*) as count
    FROM expenditures
    WHERE payment_date IS NOT NULL
    GROUP BY vendor_name, amount, payment_date
    HAVING COUNT(*) > 1
  `);
  
  for (const row of duplicateExpenditures) {
    const ids = await query(`
      SELECT id FROM expenditures 
      WHERE vendor_name = ? AND amount = ? AND payment_date = ?
    `, [row.vendor, row.amount, row.date]);
    
    flags.push({
      vendor: row.vendor as string,
      amount: parseFloat(String(row.amount)) || 0,
      date: row.date as string,
      count: parseInt(String(row.count)) || 0,
      transaction_ids: ids.map(r => r.id as number),
    });
  }
  
  return flags;
}

/**
 * Analyze vendor concentration - top vendors by payment volume
 */
export async function analyzeVendorConcentration(topN = 10): Promise<{
  vendor: string;
  vendor_id?: number;
  total_amount: number;
  transaction_count: number;
  average_amount: number;
  is_immigrant_owned?: boolean;
}[]> {
  await initializeDb();
  
  const results = await query(`
    SELECT 
      pr.id as vendor_id,
      pr.name as vendor,
      pr.is_immigrant_owned,
      COALESCE(SUM(p.amount), 0) as total_amount,
      COUNT(p.id) as transaction_count,
      COALESCE(AVG(p.amount), 0) as average_amount
    FROM providers pr
    LEFT JOIN payments p ON pr.id = p.provider_id
    GROUP BY pr.id, pr.name, pr.is_immigrant_owned
    ORDER BY total_amount DESC
    LIMIT ?
  `, [topN]);
  
  return results.map(row => ({
    vendor_id: row.vendor_id as number,
    vendor: row.vendor as string,
    is_immigrant_owned: row.is_immigrant_owned === 1 || row.is_immigrant_owned === '1',
    total_amount: parseFloat(String(row.total_amount)) || 0,
    transaction_count: parseInt(String(row.transaction_count)) || 0,
    average_amount: parseFloat(String(row.average_amount)) || 0,
  }));
}

/**
 * Save structuring flags as fraud indicators
 */
export async function saveStructuringFlags(flags: StructuringFlag[]): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  for (const flag of flags) {
    try {
      // Check if similar flag already exists
      const existing = await query(`
        SELECT id FROM fraud_indicators 
        WHERE indicator_type = 'structuring' 
        AND (provider_id = ? OR description LIKE ?)
        AND status != 'dismissed'
      `, [flag.vendor_id || 0, `%${flag.vendor}%`]);
      
      if (existing.length > 0) continue;
      
      await execute(`
        INSERT INTO fraud_indicators (
          provider_id, indicator_type, severity, description, evidence, status
        ) VALUES (?, ?, ?, ?, ?, 'open')
      `, [
        flag.vendor_id || null,
        flag.type === 'split_payments' ? 'structuring' : 'near_threshold',
        flag.severity,
        `${flag.type === 'split_payments' ? 'Potential Structuring' : 'Near-Threshold Payment'}: ${flag.vendor}`,
        flag.details,
      ]);
      
      savedCount++;
    } catch (error) {
      console.error('Error saving structuring flag:', error);
    }
  }
  
  await saveDb();
  return savedCount;
}

/**
 * Save duplicate flags as fraud indicators
 */
export async function saveDuplicateFlags(flags: DuplicateFlag[]): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  for (const flag of flags) {
    try {
      await execute(`
        INSERT INTO fraud_indicators (
          indicator_type, severity, description, evidence, status
        ) VALUES ('duplicate_payment', 'medium', ?, ?, 'open')
      `, [
        `Duplicate Transaction: ${flag.vendor} - $${flag.amount.toLocaleString()} on ${flag.date}`,
        `Found ${flag.count} identical transactions. IDs: ${flag.transaction_ids.join(', ')}`,
      ]);
      
      savedCount++;
    } catch (error) {
      console.error('Error saving duplicate flag:', error);
    }
  }
  
  await saveDb();
  return savedCount;
}

/**
 * Run all fraud detection analyses
 */
export async function runFullFraudAnalysis(): Promise<{
  structuring: StructuringFlag[];
  duplicates: DuplicateFlag[];
  vendorConcentration: Awaited<ReturnType<typeof analyzeVendorConcentration>>;
  savedIndicators: number;
}> {
  console.log('Running full fraud analysis...');
  
  const structuring = await detectStructuring();
  console.log(`Found ${structuring.length} structuring flags`);
  
  const duplicates = await detectDuplicates();
  console.log(`Found ${duplicates.length} duplicate flags`);
  
  const vendorConcentration = await analyzeVendorConcentration();
  
  // Save new fraud indicators
  const savedStructuring = await saveStructuringFlags(structuring);
  const savedDuplicates = await saveDuplicateFlags(duplicates);
  
  return {
    structuring,
    duplicates,
    vendorConcentration,
    savedIndicators: savedStructuring + savedDuplicates,
  };
}

export default {
  detectStructuring,
  detectDuplicates,
  analyzeVendorConcentration,
  saveStructuringFlags,
  saveDuplicateFlags,
  runFullFraudAnalysis,
};
