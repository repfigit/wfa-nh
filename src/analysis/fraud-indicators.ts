/**
 * Fraud Analysis Module
 * 
 * Analyzes contracts and expenditures for potential fraud indicators.
 * This is for research and transparency purposes - flags are starting points
 * for investigation, not accusations.
 */

import { initializeDb, getDb, dbHelpers, saveDb } from '../db/database.js';
import { FRAUD_INDICATOR_TYPES } from '../types/index.js';

interface AnalysisResult {
  contract_id?: number;
  contractor_id?: number;
  expenditure_id?: number;
  indicator_type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  description: string;
  evidence: string;
}

/**
 * Helper to run a query and get results
 */
async function runQuery(sql: string, params: any[] = []): Promise<any[]> {
  const db = await getDb();
  const stmt = db.prepare(sql);
  if (params.length > 0) {
    stmt.bind(params);
  }
  const results: any[] = [];
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

/**
 * Analyze all contracts for sole source procurement
 */
async function analyzeSoleSource(): Promise<AnalysisResult[]> {
  const results: AnalysisResult[] = [];
  
  const contracts = await runQuery(`
    SELECT c.*, ct.name as contractor_name
    FROM contracts c
    JOIN contractors ct ON c.contractor_id = ct.id
    WHERE c.procurement_type LIKE '%sole source%'
  `);
  
  for (const contract of contracts) {
    // High value sole source is more concerning
    const severity = contract.current_amount > 500000 ? 'high' 
      : contract.current_amount > 100000 ? 'medium' 
      : 'low';
    
    results.push({
      contract_id: contract.id,
      contractor_id: contract.contractor_id,
      indicator_type: FRAUD_INDICATOR_TYPES.SOLE_SOURCE,
      severity,
      description: `Sole source contract awarded to ${contract.contractor_name} for "${contract.title}"`,
      evidence: `Procurement type: ${contract.procurement_type}. Value: $${contract.current_amount?.toLocaleString() || 'Unknown'}`,
    });
  }
  
  return results;
}

/**
 * Analyze contracts for rapid/multiple amendments
 */
async function analyzeAmendments(): Promise<AnalysisResult[]> {
  const results: AnalysisResult[] = [];
  
  const contracts = await runQuery(`
    SELECT c.*, ct.name as contractor_name
    FROM contracts c
    JOIN contractors ct ON c.contractor_id = ct.id
    WHERE c.amendment_count >= 2
  `);
  
  for (const contract of contracts) {
    const severity = contract.amendment_count >= 4 ? 'high' 
      : contract.amendment_count >= 3 ? 'medium' 
      : 'low';
    
    results.push({
      contract_id: contract.id,
      contractor_id: contract.contractor_id,
      indicator_type: FRAUD_INDICATOR_TYPES.RAPID_AMENDMENTS,
      severity,
      description: `Contract has ${contract.amendment_count} amendments: "${contract.title}"`,
      evidence: `Amendment count: ${contract.amendment_count}. May indicate scope creep or poor initial planning.`,
    });
  }
  
  return results;
}

/**
 * Analyze contracts for large value increases
 */
async function analyzeLargeIncreases(): Promise<AnalysisResult[]> {
  const results: AnalysisResult[] = [];
  
  const contracts = await runQuery(`
    SELECT c.*, ct.name as contractor_name
    FROM contracts c
    JOIN contractors ct ON c.contractor_id = ct.id
    WHERE c.original_amount IS NOT NULL 
      AND c.current_amount IS NOT NULL
      AND c.current_amount > c.original_amount * 1.25
  `);
  
  for (const contract of contracts) {
    const increasePercent = ((contract.current_amount - contract.original_amount) / contract.original_amount) * 100;
    
    const severity = increasePercent > 200 ? 'critical'
      : increasePercent > 100 ? 'high'
      : increasePercent > 50 ? 'medium'
      : 'low';
    
    results.push({
      contract_id: contract.id,
      contractor_id: contract.contractor_id,
      indicator_type: FRAUD_INDICATOR_TYPES.LARGE_INCREASE,
      severity,
      description: `Contract value increased ${increasePercent.toFixed(1)}% from original`,
      evidence: `Original: $${contract.original_amount.toLocaleString()}, Current: $${contract.current_amount.toLocaleString()}. Increase: $${(contract.current_amount - contract.original_amount).toLocaleString()}`,
    });
  }
  
  return results;
}

/**
 * Analyze for contractors with unusual patterns
 */
async function analyzeContractorPatterns(): Promise<AnalysisResult[]> {
  const results: AnalysisResult[] = [];
  
  // Check for contractors with many sole source contracts
  const soloSourceContractors = await runQuery(`
    SELECT 
      ct.id, ct.name,
      COUNT(*) as sole_source_count,
      SUM(c.current_amount) as total_value
    FROM contractors ct
    JOIN contracts c ON ct.id = c.contractor_id
    WHERE c.procurement_type LIKE '%sole source%'
    GROUP BY ct.id
    HAVING sole_source_count >= 3
  `);
  
  for (const contractor of soloSourceContractors) {
    results.push({
      contractor_id: contractor.id,
      indicator_type: FRAUD_INDICATOR_TYPES.NO_COMPETITION,
      severity: contractor.sole_source_count >= 5 ? 'high' : 'medium',
      description: `${contractor.name} has ${contractor.sole_source_count} sole source contracts`,
      evidence: `Total sole source value: $${contractor.total_value?.toLocaleString() || 'Unknown'}. Pattern suggests limited competition.`,
    });
  }
  
  return results;
}

/**
 * Check for potential duplicate payments
 */
async function analyzeDuplicatePayments(): Promise<AnalysisResult[]> {
  const results: AnalysisResult[] = [];
  
  // Look for same vendor, same amount, same time period
  const duplicates = await runQuery(`
    SELECT 
      e1.id as exp1_id,
      e2.id as exp2_id,
      e1.vendor_name,
      e1.amount,
      e1.payment_date as date1,
      e2.payment_date as date2,
      e1.contractor_id
    FROM expenditures e1
    JOIN expenditures e2 ON e1.vendor_name = e2.vendor_name 
      AND e1.amount = e2.amount 
      AND e1.id < e2.id
      AND ABS(julianday(e1.payment_date) - julianday(e2.payment_date)) < 30
    WHERE e1.amount > 10000
  `);
  
  for (const dup of duplicates) {
    results.push({
      expenditure_id: dup.exp1_id,
      contractor_id: dup.contractor_id,
      indicator_type: FRAUD_INDICATOR_TYPES.DUPLICATE_PAYMENTS,
      severity: dup.amount > 100000 ? 'high' : 'medium',
      description: `Potential duplicate payment to ${dup.vendor_name}`,
      evidence: `Amount: $${dup.amount.toLocaleString()} paid on ${dup.date1} and ${dup.date2}`,
    });
  }
  
  return results;
}

/**
 * Run all fraud analyses and save results
 */
export async function runFullAnalysis(): Promise<void> {
  console.log('=== Running Fraud Indicator Analysis ===\n');
  
  const allResults: AnalysisResult[] = [];
  
  // Run each analysis
  console.log('Analyzing sole source contracts...');
  allResults.push(...await analyzeSoleSource());
  
  console.log('Analyzing contract amendments...');
  allResults.push(...await analyzeAmendments());
  
  console.log('Analyzing large value increases...');
  allResults.push(...await analyzeLargeIncreases());
  
  console.log('Analyzing contractor patterns...');
  allResults.push(...await analyzeContractorPatterns());
  
  console.log('Checking for duplicate payments...');
  allResults.push(...await analyzeDuplicatePayments());
  
  console.log(`\nFound ${allResults.length} potential indicators\n`);
  
  // Save to database (avoiding duplicates)
  const db = await getDb();
  let saved = 0;
  let skipped = 0;
  
  for (const result of allResults) {
    // Check if similar indicator already exists
    const existing = await runQuery(`
      SELECT id FROM fraud_indicators 
      WHERE indicator_type = ? 
        AND (contract_id = ? OR (contract_id IS NULL AND ? IS NULL))
        AND (contractor_id = ? OR (contractor_id IS NULL AND ? IS NULL))
        AND (expenditure_id = ? OR (expenditure_id IS NULL AND ? IS NULL))
    `, [
      result.indicator_type,
      result.contract_id ?? null, result.contract_id ?? null,
      result.contractor_id ?? null, result.contractor_id ?? null,
      result.expenditure_id ?? null, result.expenditure_id ?? null
    ]);
    
    if (existing.length > 0) {
      skipped++;
      continue;
    }
    
    try {
      await dbHelpers.insertFraudIndicator({
        contract_id: result.contract_id || null,
        contractor_id: result.contractor_id || null,
        expenditure_id: result.expenditure_id || null,
        indicator_type: result.indicator_type,
        severity: result.severity,
        description: result.description,
        evidence: result.evidence,
        status: 'open',
      });
      saved++;
    } catch (error) {
      console.error('Error saving indicator:', error);
    }
  }
  
  console.log(`Saved ${saved} new indicators, skipped ${skipped} existing\n`);
  
  // Print summary
  const summary = await runQuery(`
    SELECT severity, COUNT(*) as count
    FROM fraud_indicators
    WHERE status = 'open'
    GROUP BY severity
    ORDER BY 
      CASE severity 
        WHEN 'critical' THEN 1 
        WHEN 'high' THEN 2 
        WHEN 'medium' THEN 3 
        WHEN 'low' THEN 4 
      END
  `);
  
  console.log('=== Fraud Indicator Summary ===');
  for (const row of summary) {
    const icon = row.severity === 'critical' ? '!!' 
      : row.severity === 'high' ? '!' 
      : row.severity === 'medium' ? '*' 
      : '-';
    console.log(`  ${icon} ${row.severity.toUpperCase()}: ${row.count}`);
  }
  console.log();
}

// Main execution
async function main() {
  await initializeDb();
  await runFullAnalysis();
}

// Check if running directly
const isMain = process.argv[1]?.includes('fraud-indicators');
if (isMain) {
  main().catch(console.error);
}

export default { runFullAnalysis };
