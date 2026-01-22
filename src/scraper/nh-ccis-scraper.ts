import { query, execute, executeBatch } from '../db/db-adapter.js';
import { readFileSync, existsSync } from 'fs';
import { resolve } from 'path';

// Interfaces
interface ScrapeResult {
  success: boolean;
  totalFound: number;
  documentId?: number;
  error?: string;
}

// Path to the manually downloaded CSV file
const CSV_PATH = resolve(process.cwd(), 'data/downloads/nhccis-providers.csv');
const CCIS_URL = 'https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch';

/**
 * Main import function - loads from local CSV file
 * 
 * The NH CCIS website requires manual CSV download (Download Provider Results button).
 * This function imports that CSV into the source_ccis table.
 */
export async function scrapeCCIS(): Promise<ScrapeResult> {
  const result: ScrapeResult = { success: false, totalFound: 0 };

  try {
    console.log('Starting NH CCIS import from local CSV...');
    
    if (!existsSync(CSV_PATH)) {
      throw new Error(`CSV file not found at ${CSV_PATH}. Please download from ${CCIS_URL}`);
    }

    const csvData = readFileSync(CSV_PATH, 'utf-8');
    console.log(`Read CSV file: ${csvData.length} bytes`);

    const rows = parseCSVToObjects(csvData);
    result.totalFound = rows.length;
    console.log(`Parsed ${rows.length} provider records`);

    // 1. REPLACEMENT LOGIC: Wipe the dedicated source table
    console.log('Wiping previous source_ccis data...');
    await execute('DELETE FROM source_ccis');

    // 2. LOAD: Bulk insert into dedicated source table
    console.log(`Loading ${rows.length} records into source_ccis...`);
    const batchSize = 100;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      await executeBatch(batch.map(row => ({
        sql: `INSERT INTO source_ccis (
          program_name, status, phone, email, region, county,
          street, city, state, zip,
          record_type, gsq_step, provider_number, license_date, license_type,
          accepts_scholarship, accredited, capacity, age_groups, enrollment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          row.program_name, row.status, row.phone, row.email, row.region, row.county,
          row.street, row.city, row.state, row.zip,
          row.record_type, row.gsq_step, row.provider_number, row.license_date, row.license_type,
          row.accepts_scholarship, row.accredited, row.capacity, row.age_groups, row.enrollment
        ]
      })));
      if ((i + batchSize) % 500 === 0) {
        console.log(`  Inserted ${Math.min(i + batchSize, rows.length)} / ${rows.length}...`);
      }
    }

    // 3. AUDIT: Record the run in scraped_documents
    const dbResult = await execute(`
      INSERT INTO scraped_documents (source_key, url, document_type, title, raw_content)
      VALUES (?, ?, ?, ?, ?)
    `, ['ccis', CCIS_URL, 'csv', 'NH CCIS Provider Directory', csvData]);

    result.documentId = dbResult.lastId;
    result.success = true;
    console.log(`Successfully loaded source_ccis. Records: ${result.totalFound}, Audit Doc ID: ${result.documentId}`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('CCIS import error:', error);
  }
  
  return result;
}

/**
 * Parse CSV to objects with proper column mapping
 */
function parseCSVToObjects(csv: string): any[] {
  // Map CSV headers to database column names (matching actual CSV from the site)
  const headerMap: Record<string, string> = {
    'Program Name': 'program_name',
    'Provider Enrollment Status': 'status',
    'Program Phone': 'phone',
    'Program Email': 'email',
    'Region': 'region',
    'County': 'county',
    'Shipping Street': 'street',
    'Shipping City': 'city',
    'Shipping State': 'state',
    'Shipping Zip': 'zip',
    'Account Record Type': 'record_type',
    'GSQ Approved Step': 'gsq_step',
    'Provider Number': 'provider_number',
    'License Issue Date': 'license_date',
    'License Type': 'license_type',
    'Accepts NH Child Care Scholarship': 'accepts_scholarship',
    'Accredited': 'accredited',
    'Licensed Capacity': 'capacity',
    'Age Groups Served': 'age_groups',
    'Total Enrollment': 'enrollment',
  };
  
  const lines = csv.split('\n').filter(l => l.trim());
  if (lines.length === 0) return [];
  
  const rawHeaders = parseCSVLine(lines[0]);
  const headers = rawHeaders.map(h => headerMap[h.trim()] || h.trim().toLowerCase().replace(/\s+/g, '_'));
  
  console.log('CSV columns:', rawHeaders.join(', '));
  
  return lines.slice(1).map(line => {
    const values = parseCSVLine(line);
    const obj: any = {};
    headers.forEach((h, i) => {
      obj[h] = (values[i] || '').trim();
    });
    return obj;
  });
}

/**
 * Parse a single CSV line handling quoted fields
 */
function parseCSVLine(line: string): string[] {
  const values: string[] = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      values.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  values.push(current);
  
  return values.map(v => v.replace(/^"|"$/g, ''));
}

export default { scrapeCCIS };
