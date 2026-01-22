import { execute, executeRaw, executeBatch } from '../db/db-adapter.js';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import puppeteer from 'puppeteer';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '../../data/downloads');

const FISCAL_YEAR_BASE_URL = 'https://www.nh.gov/transparentnh/where-the-money-goes/documents';
const FISCAL_YEAR_URLS: Record<number, string> = {
  2026: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2026.zip`,
  2025: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2025.zip`,
  2024: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2024.zip`,
  2023: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2023.zip`,
  2022: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2022.zip`,
  2021: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2021.zip`,
};

/**
 * Main scrape function for Transparent NH (EXTRACT & LOAD)
 */
export async function scrapeFiscalYear(year: number) {
  const url = FISCAL_YEAR_URLS[year];
  if (!url) throw new Error(`No URL for FY${year}`);

  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
  const zipPath = path.join(DATA_DIR, `fy${year}.zip`);

  console.log(`Downloading FY${year} from ${url}...`);
  await downloadFile(url, zipPath);

  console.log(`Extracting FY${year} CSV...`);
  const csvContent = await extractZip(zipPath);
  if (!csvContent) throw new Error('Failed to extract CSV');

  // 1. Create Dedicated Source Table for this year
  const tableName = `source_transparent_nh_${year}`;
  console.log(`Loading data into ${tableName}...`);
  
  await executeRaw(`DROP TABLE IF EXISTS ${tableName}`);
  await executeRaw(`
    CREATE TABLE ${tableName} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      department TEXT,
      agency TEXT,
      activity TEXT,
      expense_class TEXT,
      vendor_name TEXT,
      amount TEXT,
      transaction_date TEXT,
      check_number TEXT,
      loaded_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // 2. Parse and Bulk Load
  const lines = csvContent.split('\n').filter(l => l.trim());
  const headers = parseCSVLine(lines[0]).map(h => h.toLowerCase().trim());
  
  const records = lines.slice(1).map(line => {
    const values = parseCSVLine(line);
    const row: any = {};
    headers.forEach((h, i) => row[h] = values[i] || '');
    return row;
  });

  const batchSize = 100;
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await executeBatch(batch.map(r => ({
      sql: `INSERT INTO ${tableName} (department, agency, activity, expense_class, vendor_name, amount, transaction_date, check_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        r.department || r.dept,
        r.agency,
        r.activity,
        r.expense_class || r.exp_class,
        r.vendor_name || r.vendor,
        r.amount || r.dollar_amount,
        r.transaction_date || r.date,
        r.check_number
      ]
    })));
  }

  // 3. Audit log
  const dbResult = await execute(`
    INSERT INTO scraped_documents (source_key, url, title, raw_content)
    VALUES (?, ?, ?, ?)
  `, [`transparent_nh_${year}`, url, `Transparent NH FY${year}`, `ZIP file at ${zipPath}`]);

  return { success: true, tableName, documentId: dbResult.lastId, count: records.length };
}

async function downloadFile(url: string, destPath: string) {
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  const response = await page.goto(url);
  const buffer = await response?.buffer();
  if (buffer) writeFileSync(destPath, buffer);
  await browser.close();
}

async function extractZip(zipPath: string): Promise<string | null> {
  const JSZip = (await import('jszip')).default;
  const zipData = readFileSync(zipPath);
  const zip = await JSZip.loadAsync(zipData);
  const csvFile = Object.keys(zip.files).find(name => name.toLowerCase().endsWith('.csv'));
  return csvFile ? await zip.files[csvFile].async('string') : null;
}

function parseCSVLine(line: string): string[] {
  const values: string[] = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      values.push(current.trim());
      current = '';
    } else current += char;
  }
  values.push(current.trim());
  return values;
}

export function getAvailableFiscalYears() { return [2026, 2025, 2024, 2023, 2022, 2021]; }
export async function scrapeCurrentFiscalYear() { return scrapeFiscalYear(2026); }
export async function scrapeAllHistoricalYears() {
  const results = [];
  for (const year of [2025, 2024]) results.push(await scrapeFiscalYear(year));
  return results;
}

export default { scrapeFiscalYear, getAvailableFiscalYears, scrapeCurrentFiscalYear, scrapeAllHistoricalYears };
