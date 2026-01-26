import { execute, executeRaw, executeBatch } from '../db/db-adapter.js';
import { existsSync, mkdirSync, writeFileSync, readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import * as XLSX from 'xlsx';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '../../data/downloads/monthly');

// Browser-like headers to bypass Akamai
const BROWSER_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9',
  'Accept-Encoding': 'gzip, deflate, br',
  'Connection': 'keep-alive',
  'Referer': 'https://www.nh.gov/transparentnh/where-the-money-goes/monthly-expenditure-reports.htm',
  'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
  'Sec-Ch-Ua-Mobile': '?0',
  'Sec-Ch-Ua-Platform': '"Windows"',
  'Sec-Fetch-Dest': 'document',
  'Sec-Fetch-Mode': 'navigate',
  'Sec-Fetch-Site': 'same-origin',
  'Sec-Fetch-User': '?1',
  'Upgrade-Insecure-Requests': '1',
};

// NH Fiscal Year runs July 1 - June 30
// FY 2026 = July 2025 - June 2026
const MONTHS = ['jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'jan', 'feb', 'mar', 'apr', 'may', 'jun'];

interface FiscalYearMonth {
  fiscalYear: number;
  month: string;
  calendarYear: number;
  url: string;
}

/**
 * Generate all month URLs for a given fiscal year
 */
function getFiscalYearMonths(fiscalYear: number): FiscalYearMonth[] {
  const months: FiscalYearMonth[] = [];
  const startYear = fiscalYear - 1; // FY 2026 starts in calendar 2025
  
  for (let i = 0; i < 12; i++) {
    const month = MONTHS[i];
    // July-Dec are in the prior calendar year, Jan-Jun are in the FY year
    const calendarYear = i < 6 ? startYear : fiscalYear;
    
    const url = `https://www.nh.gov/transparentnh/where-the-money-goes/${fiscalYear}/documents/expend-detail-${month}${calendarYear}-no_exclusions.xlsx`;
    
    months.push({
      fiscalYear,
      month,
      calendarYear,
      url
    });
  }
  
  return months;
}

/**
 * Download a file with browser-like headers
 */
async function downloadFile(url: string, destPath: string): Promise<boolean> {
  try {
    const response = await fetch(url, { headers: BROWSER_HEADERS });
    
    if (!response.ok) {
      console.log(`  ⚠️ HTTP ${response.status} for ${url}`);
      return false;
    }
    
    const buffer = await response.arrayBuffer();
    writeFileSync(destPath, Buffer.from(buffer));
    return true;
  } catch (error) {
    console.log(`  ❌ Error downloading ${url}: ${error}`);
    return false;
  }
}

/**
 * Parse XLSX file and return rows as objects
 */
function parseXlsx(filePath: string): Record<string, any>[] {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  
  // Convert to JSON with headers
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  return rows as Record<string, any>[];
}

/**
 * Normalize column names from XLSX headers
 */
function normalizeColumnName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

/**
 * Scrape a single month's expenditure data
 */
export async function scrapeMonth(fiscalYear: number, month: string, calendarYear: number): Promise<{
  success: boolean;
  count: number;
  filePath?: string;
  error?: string;
}> {
  const url = `https://www.nh.gov/transparentnh/where-the-money-goes/${fiscalYear}/documents/expend-detail-${month}${calendarYear}-no_exclusions.xlsx`;
  
  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
  
  const fileName = `fy${fiscalYear}-${month}${calendarYear}.xlsx`;
  const filePath = path.join(DATA_DIR, fileName);
  
  console.log(`📥 Downloading ${month} ${calendarYear} (FY${fiscalYear})...`);
  
  const downloaded = await downloadFile(url, filePath);
  if (!downloaded) {
    return { success: false, count: 0, error: `Failed to download ${url}` };
  }
  
  console.log(`📊 Parsing ${fileName}...`);
  const rows = parseXlsx(filePath);
  
  if (rows.length === 0) {
    return { success: false, count: 0, error: 'No data in file' };
  }
  
  // Create source table for this month
  const tableName = `source_transparent_nh_fy${fiscalYear}_${month}${calendarYear}`;
  
  // Get column names from first row
  const columns = Object.keys(rows[0]).map(normalizeColumnName);
  
  console.log(`💾 Loading ${rows.length} rows into ${tableName}...`);
  
  // Drop and recreate table
  await executeRaw(`DROP TABLE IF EXISTS ${tableName}`);
  
  const columnDefs = columns.map(c => `${c} TEXT`).join(', ');
  await executeRaw(`
    CREATE TABLE ${tableName} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ${columnDefs},
      fiscal_year INTEGER DEFAULT ${fiscalYear},
      month TEXT DEFAULT '${month}',
      calendar_year INTEGER DEFAULT ${calendarYear},
      loaded_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);
  
  // Batch insert
  const batchSize = 100;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const placeholders = columns.map(() => '?').join(', ');
    
    await executeBatch(batch.map(row => {
      const values = Object.keys(row).map(k => String(row[k] ?? ''));
      return {
        sql: `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`,
        args: values
      };
    }));
    
    if ((i + batchSize) % 1000 === 0) {
      console.log(`  ... inserted ${Math.min(i + batchSize, rows.length)}/${rows.length} rows`);
    }
  }
  
  // Log to scraped_documents
  await execute(`
    INSERT INTO scraped_documents (source_key, url, title, raw_content)
    VALUES (?, ?, ?, ?)
  `, [`transparent_nh_fy${fiscalYear}_${month}${calendarYear}`, url, `Transparent NH FY${fiscalYear} ${month} ${calendarYear}`, `Downloaded to ${filePath}`]);
  
  console.log(`✅ Loaded ${rows.length} rows for ${month} ${calendarYear}`);
  
  return { success: true, count: rows.length, filePath };
}

/**
 * Scrape all available months for a fiscal year
 */
export async function scrapeFiscalYear(fiscalYear: number): Promise<{
  success: boolean;
  monthsScraped: number;
  totalRows: number;
  errors: string[];
}> {
  console.log(`\n📅 Scraping Fiscal Year ${fiscalYear}...\n`);
  
  const months = getFiscalYearMonths(fiscalYear);
  let monthsScraped = 0;
  let totalRows = 0;
  const errors: string[] = [];
  
  for (const { month, calendarYear } of months) {
    const result = await scrapeMonth(fiscalYear, month, calendarYear);
    
    if (result.success) {
      monthsScraped++;
      totalRows += result.count;
    } else if (result.error) {
      // Don't treat missing future months as errors
      if (!result.error.includes('404') && !result.error.includes('Failed to download')) {
        errors.push(`${month}${calendarYear}: ${result.error}`);
      }
    }
    
    // Small delay between requests to be nice
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  console.log(`\n📊 FY${fiscalYear} Summary: ${monthsScraped} months, ${totalRows} total rows\n`);
  
  return { success: monthsScraped > 0, monthsScraped, totalRows, errors };
}

/**
 * Initial historical load - scrape all fiscal years from 2010-2025
 */
export async function scrapeHistorical(): Promise<{
  success: boolean;
  yearsScraped: number;
  totalRows: number;
}> {
  console.log('🏛️ Starting historical data load (FY 2010-2025)...\n');
  
  const fiscalYears = Array.from({ length: 16 }, (_, i) => 2010 + i); // 2010-2025
  let yearsScraped = 0;
  let totalRows = 0;
  
  for (const fy of fiscalYears) {
    const result = await scrapeFiscalYear(fy);
    if (result.success) {
      yearsScraped++;
      totalRows += result.totalRows;
    }
  }
  
  console.log(`\n✅ Historical load complete: ${yearsScraped} fiscal years, ${totalRows} total rows`);
  
  return { success: yearsScraped > 0, yearsScraped, totalRows };
}

/**
 * Scrape current fiscal year (FY 2026) - for recurring updates
 */
export async function scrapeCurrentFiscalYear(): Promise<{
  success: boolean;
  monthsScraped: number;
  totalRows: number;
}> {
  const currentFY = 2026;
  console.log(`🔄 Scraping current fiscal year (FY${currentFY})...\n`);
  
  const result = await scrapeFiscalYear(currentFY);
  
  return {
    success: result.success,
    monthsScraped: result.monthsScraped,
    totalRows: result.totalRows
  };
}

/**
 * Check for new months in current FY and scrape only new data
 */
export async function scrapeNewMonths(): Promise<{
  success: boolean;
  newMonths: string[];
  totalRows: number;
}> {
  const currentFY = 2026;
  console.log(`🔍 Checking for new months in FY${currentFY}...\n`);
  
  const months = getFiscalYearMonths(currentFY);
  const newMonths: string[] = [];
  let totalRows = 0;
  
  for (const { month, calendarYear } of months) {
    const tableName = `source_transparent_nh_fy${currentFY}_${month}${calendarYear}`;
    
    // Check if we already have this month
    try {
      const result = await execute(`SELECT COUNT(*) as count FROM ${tableName}`, []);
      if (result.rows && result.rows.length > 0 && (result.rows[0] as any).count > 0) {
        console.log(`  ⏭️ Skipping ${month} ${calendarYear} (already loaded)`);
        continue;
      }
    } catch {
      // Table doesn't exist, try to scrape
    }
    
    const result = await scrapeMonth(currentFY, month, calendarYear);
    if (result.success) {
      newMonths.push(`${month}${calendarYear}`);
      totalRows += result.count;
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  if (newMonths.length > 0) {
    console.log(`\n✅ Scraped ${newMonths.length} new months: ${newMonths.join(', ')}`);
  } else {
    console.log(`\n✓ No new months available`);
  }
  
  return { success: true, newMonths, totalRows };
}

/**
 * Get list of available fiscal years
 */
export function getAvailableFiscalYears(): number[] {
  return Array.from({ length: 17 }, (_, i) => 2010 + i); // 2010-2026
}

export default {
  scrapeMonth,
  scrapeFiscalYear,
  scrapeHistorical,
  scrapeCurrentFiscalYear,
  scrapeNewMonths,
  getAvailableFiscalYears
};
