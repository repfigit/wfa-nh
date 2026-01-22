import { query, execute } from '../db/db-adapter.js';
import { resolveEntity, normalizeName } from '../matcher/entity-resolver.js';

const CHILDCARE_KEYWORDS = ['daycare', 'child care', 'childcare', 'preschool', 'early learning', 'nursery', 'head start'];

/**
 * Bridge structured Transparent NH data into the master tables
 */
export async function bridgeTransparentNH(year: number) {
  const tableName = `source_transparent_nh_${year}`;
  console.log(`Bridging Transparent NH FY${year} from ${tableName}...`);

  const records = await query(`SELECT * FROM ${tableName}`);
  console.log(`Analyzing ${records.length} records...`);

  let imported = 0;

  for (const raw of records) {
    try {
      const vendorName = raw.vendor_name;
      if (!vendorName) continue;

      const amount = parseFloat(raw.amount.replace(/[$,]/g, '')) || 0;
      if (amount <= 0) continue;

      // Filter for childcare related or DHHS
      const isRelated = CHILDCARE_KEYWORDS.some(k => vendorName.toLowerCase().includes(k)) || 
                        (raw.department || '').toLowerCase().includes('health and human services');
      
      if (!isRelated) continue;

      // 1. Resolve to Master Provider
      const match = await resolveEntity({
        name: vendorName,
        sourceSystem: 'transparent_nh',
        sourceIdentifier: `TNH-${year}-${vendorName}-${amount}`
      });

      const masterId = match.matched ? match.providerId : null;

      // 2. Insert into Expenditures
      await execute(`
        INSERT INTO expenditures (provider_master_id, fiscal_year, vendor_name, amount, payment_date, description, source_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `, [
        masterId,
        year,
        vendorName,
        amount,
        raw.transaction_date,
        `${raw.department} - ${raw.activity}`,
        `Transparent NH FY${year}`
      ]);

      imported++;
    } catch (err) {
      console.error(`Error bridging record:`, err);
    }
  }

  console.log(`Bridge complete: ${imported} expenditures imported/updated.`);
  return { imported };
}
