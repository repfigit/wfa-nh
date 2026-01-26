import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

/**
 * Bridge TransparentNH expenditure data from source_transparent_nh into master tables
 * 
 * This bridges:
 * - Unique vendors → contractors table
 * - Individual payments → expenditures table
 * - Links vendors to existing provider_master records where possible
 */
export async function bridgeTransparentNH(options?: { fiscalYear?: number; month?: string; calendarYear?: number }) {
  console.log('Bridging TransparentNH expenditure data...');
  
  // Build WHERE clause for optional filtering
  let whereClause = '';
  const whereArgs: any[] = [];
  
  if (options?.fiscalYear) {
    whereClause = 'WHERE fiscal_year = ?';
    whereArgs.push(options.fiscalYear);
    
    if (options?.month && options?.calendarYear) {
      whereClause += ' AND month = ? AND calendar_year = ?';
      whereArgs.push(options.month, options.calendarYear);
    }
  }
  
  // 1. Get unique vendors from source data
  console.log('  Extracting unique vendors...');
  const vendors = await query<{ vendor_name: string; total_amount: number; payment_count: number }>(
    `SELECT 
      vendor_name, 
      SUM(amount) as total_amount,
      COUNT(*) as payment_count
    FROM source_transparent_nh 
    ${whereClause}
    GROUP BY vendor_name
    ORDER BY total_amount DESC`,
    whereArgs
  );
  
  console.log(`  Found ${vendors.length} unique vendors`);
  
  let contractorsImported = 0;
  let contractorsUpdated = 0;
  
  // 2. Upsert vendors into contractors table
  for (const vendor of vendors) {
    if (!vendor.vendor_name || vendor.vendor_name.trim() === '') continue;
    
    const canonicalName = normalizeName(vendor.vendor_name);
    
    // Check if contractor exists
    const existing = await query<{ id: number }>(
      'SELECT id FROM contractors WHERE name = ? OR name = ?',
      [vendor.vendor_name, canonicalName]
    );
    
    if (existing.length > 0) {
      // Update existing contractor with latest totals (could track historical)
      contractorsUpdated++;
    } else {
      // Insert new contractor
      await execute(
        `INSERT INTO contractors (name, city, state, is_immigrant_related, created_at)
         VALUES (?, NULL, 'NH', 0, datetime('now'))`,
        [vendor.vendor_name]
      );
      contractorsImported++;
    }
  }
  
  console.log(`  Contractors: ${contractorsImported} imported, ${contractorsUpdated} existing`);
  
  // 3. Bridge expenditure records
  console.log('  Bridging expenditure records...');
  
  // Get source records
  const sourceRecords = await query<{
    id: number;
    fiscal_year: number;
    vendor_name: string;
    amount: number;
    check_date: string;
    department: string;
    agency: string;
    activity_name: string;
    expense_class: string;
  }>(
    `SELECT id, fiscal_year, vendor_name, amount, check_date, department, agency, activity_name, expense_class
     FROM source_transparent_nh ${whereClause}`,
    whereArgs
  );
  
  console.log(`  Processing ${sourceRecords.length} expenditure records...`);
  
  // Clear existing expenditures for this data set (to allow re-bridging)
  if (options?.fiscalYear) {
    await execute(
      'DELETE FROM expenditures WHERE fiscal_year = ? AND source_url LIKE ?',
      [options.fiscalYear, '%transparent_nh%']
    );
  }
  
  let expendituresImported = 0;
  const batchSize = 100;
  
  for (let i = 0; i < sourceRecords.length; i += batchSize) {
    const batch = sourceRecords.slice(i, i + batchSize);
    
    for (const record of batch) {
      // Find contractor ID
      const contractor = await query<{ id: number }>(
        'SELECT id FROM contractors WHERE name = ?',
        [record.vendor_name]
      );
      
      const contractorId = contractor[0]?.id || null;
      
      // Try to match to provider_master (for childcare providers)
      let providerMasterId: number | null = null;
      if (record.vendor_name) {
        const canonicalName = normalizeName(record.vendor_name);
        const provider = await query<{ id: number }>(
          `SELECT id FROM provider_master 
           WHERE canonical_name = ? 
           OR name_display = ?
           LIMIT 1`,
          [canonicalName, record.vendor_name]
        );
        providerMasterId = provider[0]?.id || null;
      }
      
      // Build description from available fields
      const description = [
        record.department,
        record.agency,
        record.activity_name,
        record.expense_class
      ].filter(Boolean).join(' | ');
      
      await execute(
        `INSERT INTO expenditures (
          provider_master_id, contractor_id, fiscal_year, vendor_name,
          amount, payment_date, description, source_url, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
        [
          providerMasterId,
          contractorId,
          record.fiscal_year,
          record.vendor_name,
          record.amount,
          record.check_date,
          description,
          'transparent_nh'
        ]
      );
      
      expendituresImported++;
    }
    
    if ((i + batchSize) % 5000 === 0 || i + batchSize >= sourceRecords.length) {
      console.log(`    Bridged ${Math.min(i + batchSize, sourceRecords.length)} / ${sourceRecords.length}...`);
    }
  }
  
  console.log(`\n✅ Bridge complete:`);
  console.log(`   Contractors: ${contractorsImported} new, ${contractorsUpdated} existing`);
  console.log(`   Expenditures: ${expendituresImported} records`);
  
  return {
    contractorsImported,
    contractorsUpdated,
    expendituresImported
  };
}

/**
 * Bridge all TransparentNH data (full refresh)
 */
export async function bridgeAll() {
  return bridgeTransparentNH();
}

/**
 * Bridge a specific fiscal year
 */
export async function bridgeFiscalYear(fiscalYear: number) {
  return bridgeTransparentNH({ fiscalYear });
}

/**
 * Bridge a specific month
 */
export async function bridgeMonth(fiscalYear: number, month: string, calendarYear: number) {
  return bridgeTransparentNH({ fiscalYear, month, calendarYear });
}

export default {
  bridgeTransparentNH,
  bridgeAll,
  bridgeFiscalYear,
  bridgeMonth
};
