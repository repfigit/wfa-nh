import { query, execute, executeBatch } from '../db/db-adapter.js';
import { existsSync, mkdirSync, writeFileSync, readFileSync, readdirSync, unlinkSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import XLSX from 'xlsx';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DOWNLOAD_DIR = process.env.TRIGGER_ENV ? '/tmp/downloads/transparent-nh' : path.resolve(process.cwd(), 'data/downloads/transparent-nh');

// Browser-like headers to bypass Akamai
const BROWSER_HEADERS: Record<string, string> = {
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

interface ScrapeResult {
  success: boolean;
  totalFound: number;
  documentId?: number;
  error?: string;
}

interface TransparentNHRecord {
  fiscal_year: number;
  month: string;
  calendar_year: number;
  department: string;
  agency: string;
  activity_number: string;
  activity_name: string;
  expense_class: string;
  vendor_name: string;
  amount: number;
  check_number: string;
  check_date: string;
}

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
    
    months.push({ fiscalYear, month, calendarYear, url });
  }
  
  return months;
}

/**
 * Download a file with browser-like headers using curl
 */
async function downloadFile(url: string, destPath: string): Promise<boolean> {
  const { spawn } = await import('child_process');
  
  return new Promise((resolve) => {
    const args = ['-sL', '-o', destPath];
    
    // Add all headers
    for (const [key, value] of Object.entries(BROWSER_HEADERS)) {
      args.push('-H', `${key}: ${value}`);
    }
    args.push(url);
    
    const curl = spawn('curl', args);
    
    curl.on('close', (code) => {
      if (code !== 0) {
        console.log(`  ⚠️ curl exited with code ${code}`);
        resolve(false);
        return;
      }
      
      if (!existsSync(destPath)) {
        console.log(`  ⚠️ File not created for ${url}`);
        resolve(false);
        return;
      }
      
      const content = readFileSync(destPath);
      if (content.length < 1000) {
        console.log(`  ⚠️ File too small (${content.length} bytes) - likely error page`);
        resolve(false);
        return;
      }
      
      const head = content.slice(0, 200).toString();
      if (head.includes('Access Denied') || head.includes('<HTML>')) {
        console.log(`  ⚠️ Access denied or HTML error page`);
        resolve(false);
        return;
      }
      
      resolve(true);
    });
    
    curl.on('error', (err) => {
      console.log(`  ❌ curl error: ${err}`);
      resolve(false);
    });
  });
}

/**
 * Parse XLSX file and return standardized records
 */
function parseXlsx(filePath: string, fiscalYear: number, month: string, calendarYear: number): TransparentNHRecord[] {
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' }) as Record<string, any>[];
  
  return rows.map(row => {
    // Normalize column names (they vary slightly between files)
    const getValue = (keys: string[]): string => {
      for (const key of keys) {
        if (row[key] !== undefined && row[key] !== '') return String(row[key]);
        const lowerKey = Object.keys(row).find(k => k.toLowerCase() === key.toLowerCase());
        if (lowerKey && row[lowerKey] !== undefined) return String(row[lowerKey]);
      }
      return '';
    };
    
    const amountStr = getValue(['Amount', 'Dollar Amount', 'amount', 'dollar_amount']);
    const amount = parseFloat(amountStr.replace(/[,$]/g, '')) || 0;
    
    return {
      fiscal_year: fiscalYear,
      month,
      calendar_year: calendarYear,
      department: getValue(['Department', 'Dept', 'department', 'dept']),
      agency: getValue(['Agency', 'agency']),
      activity_number: getValue(['Activity No', 'Activity Number', 'activity_no', 'activity_number']),
      activity_name: getValue(['Activity Name', 'activity_name']),
      expense_class: getValue(['Expense Class', 'Exp Class', 'expense_class', 'exp_class']),
      vendor_name: getValue(['Vendor Name', 'Vendor', 'vendor_name', 'vendor']),
      amount,
      check_number: getValue(['Check Number', 'Check No', 'check_number', 'check_no']),
      check_date: getValue(['Check Date', 'Date', 'check_date', 'transaction_date']),
    };
  });
}

/**
 * Convert empty string to null for database insertion
 */
function emptyToNull(value: string | number | null): string | number | null {
  if (value === '' || value === null || value === undefined) return null;
  return value;
}

/**
 * Load records into the source_transparent_nh table
 */
async function loadIntoDatabase(records: TransparentNHRecord[], fiscalYear: number, month: string, calendarYear: number): Promise<void> {
  // Delete existing records for this specific month (allows re-scraping)
  console.log(`  Deleting existing records for FY${fiscalYear} ${month} ${calendarYear}...`);
  await execute(
    'DELETE FROM source_transparent_nh WHERE fiscal_year = ? AND month = ? AND calendar_year = ?',
    [fiscalYear, month, calendarYear]
  );
  
  // Bulk insert
  console.log(`  Loading ${records.length} records...`);
  const batchSize = 100;
  let insertedCount = 0;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    
    await executeBatch(batch.map(row => ({
      sql: `INSERT INTO source_transparent_nh (
        fiscal_year, month, calendar_year, department, agency, 
        activity_number, activity_name, expense_class, vendor_name, 
        amount, check_number, check_date
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        row.fiscal_year,
        row.month,
        row.calendar_year,
        emptyToNull(row.department),
        emptyToNull(row.agency),
        emptyToNull(row.activity_number),
        emptyToNull(row.activity_name),
        emptyToNull(row.expense_class),
        emptyToNull(row.vendor_name),
        row.amount,
        emptyToNull(row.check_number),
        emptyToNull(row.check_date),
      ]
    })));
    
    insertedCount += batch.length;
    if ((i + batchSize) % 1000 === 0 || i + batchSize >= records.length) {
      console.log(`    Inserted ${insertedCount} / ${records.length}...`);
    }
  }
}

/**
 * Scrape a single month's expenditure data
 */
export async function scrapeMonth(fiscalYear: number, month: string, calendarYear: number): Promise<ScrapeResult> {
  const result: ScrapeResult = { success: false, totalFound: 0 };
  
  const url = `https://www.nh.gov/transparentnh/where-the-money-goes/${fiscalYear}/documents/expend-detail-${month}${calendarYear}-no_exclusions.xlsx`;
  
  if (!existsSync(DOWNLOAD_DIR)) mkdirSync(DOWNLOAD_DIR, { recursive: true });
  
  const fileName = `fy${fiscalYear}-${month}${calendarYear}.xlsx`;
  const filePath = path.join(DOWNLOAD_DIR, fileName);
  
  console.log(`📥 Downloading ${month} ${calendarYear} (FY${fiscalYear})...`);
  
  const downloaded = await downloadFile(url, filePath);
  if (!downloaded) {
    result.error = `Failed to download ${url}`;
    return result;
  }
  
  console.log(`📊 Parsing ${fileName}...`);
  const records = parseXlsx(filePath, fiscalYear, month, calendarYear);
  
  if (records.length === 0) {
    result.error = 'No data in file';
    return result;
  }
  
  result.totalFound = records.length;
  
  console.log(`💾 Loading ${records.length} records into source_transparent_nh...`);
  await loadIntoDatabase(records, fiscalYear, month, calendarYear);
  
  // Audit log
  const dbResult = await execute(`
    INSERT INTO scraped_documents (source_key, url, document_type, title, raw_content)
    VALUES (?, ?, ?, ?, ?)
  `, [
    `transparent_nh_fy${fiscalYear}_${month}${calendarYear}`,
    url,
    'xlsx',
    `Transparent NH FY${fiscalYear} ${month} ${calendarYear}`,
    `Downloaded: ${filePath}, Records: ${records.length}`
  ]);
  
  result.documentId = dbResult.lastId;
  result.success = true;
  
  console.log(`✅ Loaded ${records.length} records for ${month} ${calendarYear}`);
  
  return result;
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
      totalRows += result.totalFound;
    } else if (result.error && !result.error.includes('Failed to download')) {
      errors.push(`${month}${calendarYear}: ${result.error}`);
    }
    
    // Small delay between requests
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
export async function scrapeCurrentFiscalYear(): Promise<ScrapeResult & { monthsScraped: number }> {
  const currentFY = 2026;
  console.log(`🔄 Scraping current fiscal year (FY${currentFY})...\n`);
  
  const result = await scrapeFiscalYear(currentFY);
  
  return {
    success: result.success,
    totalFound: result.totalRows,
    monthsScraped: result.monthsScraped,
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
    // Check if we already have this month
    const existing = await query<{ count: number }>(
      'SELECT COUNT(*) as count FROM source_transparent_nh WHERE fiscal_year = ? AND month = ? AND calendar_year = ?',
      [currentFY, month, calendarYear]
    );
    
    if (existing[0]?.count > 0) {
      console.log(`  ⏭️ Skipping ${month} ${calendarYear} (already loaded)`);
      continue;
    }
    
    const result = await scrapeMonth(currentFY, month, calendarYear);
    if (result.success) {
      newMonths.push(`${month}${calendarYear}`);
      totalRows += result.totalFound;
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
