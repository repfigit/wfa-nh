import { execute, executeRaw, executeBatch } from '../db/db-adapter.js';

const API_BASE = 'https://api.usaspending.gov/api/v2';
const NH_STATE_CODE = 'NH';
const CCDF_CFDA_CODES = ['93.575', '93.596'];
const GRANT_AWARD_TYPES = ['02', '03', '04', '05'];

/**
 * USAspending Scraper (EXTRACT & LOAD)
 */
export async function scrapeUSASpending(fiscalYear?: number) {
  console.log('Fetching NH CCDF awards from USAspending.gov...');
  
  const filters: any = {
    recipient_locations: [{ country: 'USA', state: NH_STATE_CODE }],
    program_numbers: CCDF_CFDA_CODES,
    award_type_codes: GRANT_AWARD_TYPES,
  };
  
  if (fiscalYear) {
    filters.time_period = [{ start_date: `${fiscalYear - 1}-10-01`, end_date: `${fiscalYear}-09-30` }];
  }

  const response = await fetch(`${API_BASE}/search/spending_by_award/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filters,
      fields: ['Award ID', 'Recipient Name', 'Award Amount', 'Start Date', 'Awarding Agency', 'CFDA Number', 'Description'],
      limit: 100
    }),
  });

  if (!response.ok) throw new Error(`API error: ${response.status}`);
  const data = await response.json();
  const awards = data.results || [];

  // 1. Create Dedicated Source Table
  console.log(`Loading ${awards.length} awards into source_usaspending...`);
  await executeRaw(`DROP TABLE IF EXISTS source_usaspending`);
  await executeRaw(`
    CREATE TABLE source_usaspending (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      award_id TEXT,
      recipient_name TEXT,
      amount TEXT,
      start_date TEXT,
      agency TEXT,
      cfda TEXT,
      description TEXT,
      loaded_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // 2. Bulk Load
  const batchSize = 50;
  for (let i = 0; i < awards.length; i += batchSize) {
    const batch = awards.slice(i, i + batchSize);
    await executeBatch(batch.map((a: any) => ({
      sql: `INSERT INTO source_usaspending (award_id, recipient_name, amount, start_date, agency, cfda, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
      args: [
        a['Award ID'],
        a['Recipient Name'],
        a['Award Amount'],
        a['Start Date'],
        a['Awarding Agency'],
        a['CFDA Number'],
        a['Description']
      ]
    })));
  }

  // 3. Audit log
  const dbResult = await execute(`
    INSERT INTO scraped_documents (source_key, url, title, raw_content)
    VALUES (?, ?, ?, ?)
  `, ['usaspending', `${API_BASE}/search/spending_by_award/`, 'USAspending CCDF Awards', JSON.stringify(awards)]);

  return { success: true, count: awards.length, documentId: dbResult.lastId };
}

export async function getNHStateOverview() { return { message: "Overview fetching not yet implemented in new arch" }; }

export default { scrapeUSASpending, getNHStateOverview };
