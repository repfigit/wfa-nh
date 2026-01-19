/**
 * TransparentNH Scraper
 * Downloads and processes fiscal year expenditure data from TransparentNH
 * 
 * Data source: https://www.nh.gov/transparentnh/where-the-money-goes/fiscal-yr-downloads.htm
 * Files are ZIP archives containing CSV data of all state expenditures
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';
import { existsSync, mkdirSync, writeFileSync, readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Use /tmp on Vercel (read-only filesystem), local data dir otherwise
const DATA_DIR = process.env.VERCEL ? '/tmp/downloads' : path.join(__dirname, '../../data/downloads');

// TransparentNH fiscal year download URLs
const FISCAL_YEAR_URLS: Record<number, string> = {
  2026: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2026.zip',
  2025: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2025.zip',
  2024: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2024.zip',
  2023: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2023.zip',
  2022: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2022.zip',
  2021: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2021.zip',
  2020: 'https://www.nh.gov/transparentnh/where-the-money-goes/documents/fy2020.zip',
};

// Keywords to identify childcare-related expenditures
const CHILDCARE_KEYWORDS = [
  'daycare', 'day care', 'child care', 'childcare',
  'early learning', 'early childhood',
  'preschool', 'pre-school', 'pre school',
  'nursery', 'head start', 'headstart',
  'after school', 'afterschool',
  'ccdf', 'child development',
  'child care scholarship',
  'child care subsidy',
];

// Keywords that may indicate immigrant-related services
const IMMIGRANT_KEYWORDS = [
  'immigrant', 'refugee', 'asylum', 'resettlement',
  'translation', 'interpreter', 'multicultural',
  'esl', 'newcomer', 'foreign',
];

// DHHS-related departments (where childcare payments would come from)
const DHHS_DEPARTMENTS = [
  'health and human services',
  'dhhs',
  'hhs',
  'human services',
];

interface TransparentNHRecord {
  department?: string;
  agency?: string;
  activity?: string;
  activityProjectNumber?: string;
  activityProjectDescription?: string;
  accountingUnit?: string;
  expenseClass?: string;
  detailAccount?: string;
  vendorName?: string;
  amount?: number;
  fiscalYear?: number;
  transactionDate?: string;
  checkNumber?: string;
}

interface ScrapeResult {
  success: boolean;
  fiscalYear: number;
  totalRecords: number;
  childcareRecords: number;
  importedRecords: number;
  totalAmount: number;
  error?: string;
}

/**
 * Generate realistic browser headers to avoid bot detection
 */
function getBrowserHeaders(referer?: string): Record<string, string> {
  const headers: Record<string, string> = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
  };
  
  if (referer) {
    headers['Referer'] = referer;
  }
  
  return headers;
}

/**
 * Sleep for a random duration to mimic human behavior
 */
function randomSleep(minMs: number, maxMs: number): Promise<void> {
  const delay = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
  return new Promise(resolve => setTimeout(resolve, delay));
}

/**
 * Download a file from URL with retry logic and browser emulation
 */
async function downloadFile(url: string, destPath: string, maxRetries = 3): Promise<boolean> {
  const baseUrl = 'https://www.nh.gov/transparentnh/where-the-money-goes/';
  const downloadPageUrl = 'https://www.nh.gov/transparentnh/where-the-money-goes/fiscal-yr-downloads.htm';
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Download attempt ${attempt}/${maxRetries} for ${url}...`);
      
      // Add random delay between attempts (2-5 seconds)
      if (attempt > 1) {
        await randomSleep(2000, 5000);
      }
      
      // First, try to "visit" the download page to establish a session
      if (attempt === 1) {
        console.log('Establishing session by visiting download page...');
        try {
          const sessionResponse = await fetch(downloadPageUrl, {
            headers: getBrowserHeaders(),
            redirect: 'follow',
          });
          console.log(`Download page status: ${sessionResponse.status}`);
          // Small delay after visiting the page
          await randomSleep(500, 1500);
        } catch (e) {
          console.log('Could not access download page, continuing anyway...');
        }
      }
      
      // Now try to download the file with referer header
      const response = await fetch(url, {
        headers: getBrowserHeaders(downloadPageUrl),
        redirect: 'follow',
      });
      
      console.log(`Response status: ${response.status}`);
      
      if (response.status === 403) {
        console.log('Access forbidden (403) - site may be blocking automated requests');
        if (attempt < maxRetries) {
          console.log('Will retry with longer delay...');
          await randomSleep(3000, 7000);
          continue;
        }
        throw new Error(`HTTP 403: Access Forbidden - site is blocking automated downloads`);
      }
      
      if (response.status === 404) {
        // Check if this is a real 404 or a bot-block masquerading as 404
        const contentType = response.headers.get('content-type') || '';
        const contentLength = response.headers.get('content-length');
        
        if (contentType.includes('text/html') && contentLength && parseInt(contentLength) < 1000) {
          console.log('Received small HTML response for ZIP file - likely bot protection');
          if (attempt < maxRetries) {
            await randomSleep(3000, 7000);
            continue;
          }
        }
        throw new Error(`HTTP 404: File not found - ${url}`);
      }
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      // Verify we got a ZIP file (check content-type or first bytes)
      const contentType = response.headers.get('content-type') || '';
      if (contentType.includes('text/html')) {
        console.log('Received HTML instead of ZIP - possible redirect or block page');
        if (attempt < maxRetries) {
          await randomSleep(2000, 5000);
          continue;
        }
        throw new Error('Received HTML response instead of ZIP file');
      }
      
      const buffer = await response.arrayBuffer();
      
      // Verify it's a valid ZIP (starts with PK magic bytes)
      const bytes = new Uint8Array(buffer);
      if (bytes.length < 4 || bytes[0] !== 0x50 || bytes[1] !== 0x4B) {
        console.log('Downloaded file is not a valid ZIP');
        if (attempt < maxRetries) {
          await randomSleep(2000, 5000);
          continue;
        }
        throw new Error('Downloaded file is not a valid ZIP archive');
      }
      
      const fs = await import('fs');
      fs.writeFileSync(destPath, Buffer.from(buffer));
      
      console.log(`Successfully downloaded to ${destPath} (${buffer.byteLength} bytes)`);
      return true;
      
    } catch (error) {
      console.error(`Attempt ${attempt} failed:`, error);
      if (attempt === maxRetries) {
        console.error(`All ${maxRetries} download attempts failed`);
        throw error;
      }
    }
  }
  
  return false;
}

/**
 * Extract ZIP file and return CSV content
 */
async function extractZip(zipPath: string): Promise<string | null> {
  try {
    // Use dynamic import for JSZip
    const JSZip = (await import('jszip')).default;
    const fs = await import('fs');
    
    const zipData = fs.readFileSync(zipPath);
    const zip = await JSZip.loadAsync(zipData);
    
    // Find the CSV file in the ZIP
    const csvFileName = Object.keys(zip.files).find(name => 
      name.toLowerCase().endsWith('.csv')
    );
    
    if (!csvFileName) {
      console.error('No CSV file found in ZIP');
      return null;
    }
    
    console.log(`Extracting ${csvFileName}...`);
    const csvContent = await zip.files[csvFileName].async('string');
    
    return csvContent;
  } catch (error) {
    console.error('Error extracting ZIP:', error);
    return null;
  }
}

/**
 * Parse CSV content into records
 */
function parseCSV(csvContent: string): TransparentNHRecord[] {
  const lines = csvContent.split(/\r?\n/).filter(line => line.trim());
  if (lines.length < 2) return [];
  
  // Parse header
  const headers = parseCSVLine(lines[0]).map(h => 
    h.toLowerCase().trim().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '')
  );
  
  const records: TransparentNHRecord[] = [];
  
  for (let i = 1; i < lines.length; i++) {
    try {
      const values = parseCSVLine(lines[i]);
      const record: TransparentNHRecord = {};
      
      // Map columns to record fields
      for (let j = 0; j < headers.length && j < values.length; j++) {
        const header = headers[j];
        const value = values[j];
        
        switch (header) {
          case 'department':
          case 'dept':
          case 'department_name':
            record.department = value;
            break;
          case 'agency':
          case 'agency_name':
            record.agency = value;
            break;
          case 'activity':
          case 'activity_name':
            record.activity = value;
            break;
          case 'activity_project_number':
          case 'activity_proj_number':
            record.activityProjectNumber = value;
            break;
          case 'activity_project_description':
          case 'activity_proj_desc':
          case 'activity_project_desc':
            record.activityProjectDescription = value;
            break;
          case 'accounting_unit':
          case 'acct_unit':
            record.accountingUnit = value;
            break;
          case 'expense_class':
          case 'class':
          case 'exp_class':
            record.expenseClass = value;
            break;
          case 'detail_account':
          case 'account':
          case 'detail_acct':
            record.detailAccount = value;
            break;
          case 'vendor':
          case 'vendor_name':
          case 'payee':
          case 'payee_name':
            record.vendorName = value;
            break;
          case 'amount':
          case 'expenditure_amount':
          case 'payment_amount':
          case 'transaction_amount':
            record.amount = parseAmount(value);
            break;
          case 'fiscal_year':
          case 'fy':
            record.fiscalYear = parseInt(value) || undefined;
            break;
          case 'transaction_date':
          case 'date':
          case 'check_date':
          case 'payment_date':
            record.transactionDate = value;
            break;
          case 'check_number':
          case 'check_num':
            record.checkNumber = value;
            break;
        }
      }
      
      if (record.vendorName && record.amount !== undefined) {
        records.push(record);
      }
    } catch (error) {
      // Skip malformed rows
    }
  }
  
  return records;
}

/**
 * Parse a single CSV line, handling quoted values
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
      values.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  values.push(current.trim());
  return values;
}

/**
 * Parse amount string to number
 */
function parseAmount(amountStr: string): number {
  if (!amountStr) return 0;
  const cleaned = amountStr.replace(/[$,()]/g, '').trim();
  const amount = parseFloat(cleaned);
  if (amountStr.includes('(') && amountStr.includes(')')) {
    return -Math.abs(amount);
  }
  return isNaN(amount) ? 0 : amount;
}

/**
 * Check if a record is childcare-related
 */
function isChildcareRelated(record: TransparentNHRecord): boolean {
  const searchText = [
    record.vendorName,
    record.activity,
    record.activityProjectDescription,
    record.detailAccount,
    record.agency,
  ].filter(Boolean).join(' ').toLowerCase();
  
  return CHILDCARE_KEYWORDS.some(keyword => searchText.includes(keyword));
}

/**
 * Check if a record is immigrant-related
 */
function isImmigrantRelated(record: TransparentNHRecord): boolean {
  const searchText = [
    record.vendorName,
    record.activity,
    record.activityProjectDescription,
  ].filter(Boolean).join(' ').toLowerCase();
  
  return IMMIGRANT_KEYWORDS.some(keyword => searchText.includes(keyword));
}

/**
 * Check if a record is from DHHS
 */
function isDHHS(record: TransparentNHRecord): boolean {
  const dept = (record.department || '').toLowerCase();
  return DHHS_DEPARTMENTS.some(d => dept.includes(d));
}

/**
 * Save records to the database
 */
async function saveRecords(records: TransparentNHRecord[], fiscalYear: number): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  // Get existing providers for matching
  const providers = await query('SELECT id, name FROM providers');
  const providerMap = new Map<string, number>();
  for (const row of providers) {
    const name = (row.name as string).toLowerCase();
    providerMap.set(name, row.id as number);
  }
  
  for (const record of records) {
    try {
      // Try to match to existing provider
      let providerId: number | null = null;
      const vendorLower = (record.vendorName || '').toLowerCase();
      
      for (const [providerName, id] of providerMap) {
        if (vendorLower.includes(providerName) || providerName.includes(vendorLower)) {
          providerId = id;
          break;
        }
      }
      
      // Check for duplicate
      const existing = await query(`
        SELECT id FROM expenditures 
        WHERE vendor_name = ? AND amount = ? AND fiscal_year = ?
        AND (payment_date = ? OR (payment_date IS NULL AND ? IS NULL))
        LIMIT 1
      `, [
        record.vendorName || '',
        record.amount || 0,
        fiscalYear,
        record.transactionDate || null,
        record.transactionDate || null,
      ]);
      
      if (existing.length > 0) continue;
      
      // Insert expenditure
      await execute(`
        INSERT INTO expenditures (
          provider_id, fiscal_year, department, agency, activity,
          expense_class, vendor_name, amount, payment_date, description,
          source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        providerId,
        fiscalYear,
        record.department || null,
        record.agency || null,
        record.activity || null,
        record.expenseClass || null,
        record.vendorName || '',
        record.amount || 0,
        record.transactionDate || null,
        record.activityProjectDescription || record.detailAccount || null,
        `TransparentNH FY${fiscalYear}`,
      ]);
      
      savedCount++;
      
      // Create provider if childcare-related and not matched
      if (!providerId && isChildcareRelated(record)) {
        const isImmigrant = isImmigrantRelated(record);
        
        // Use INSERT ... ON CONFLICT for PostgreSQL compatibility
        await execute(`
          INSERT INTO providers (name, accepts_ccdf, is_immigrant_owned, notes)
          VALUES (?, 1, ?, 'Auto-created from TransparentNH scrape')
        `, [
          record.vendorName || '',
          isImmigrant ? 1 : 0,
        ]);
      }
      
    } catch (error) {
      // Skip errors
    }
  }
  
  await saveDb();
  return savedCount;
}

/**
 * Scrape a specific fiscal year
 */
export async function scrapeFiscalYear(fiscalYear: number): Promise<ScrapeResult> {
  const result: ScrapeResult = {
    success: false,
    fiscalYear,
    totalRecords: 0,
    childcareRecords: 0,
    importedRecords: 0,
    totalAmount: 0,
  };
  
  try {
    const url = FISCAL_YEAR_URLS[fiscalYear];
    if (!url) {
      result.error = `No download URL available for FY${fiscalYear}`;
      return result;
    }
    
    // Ensure download directory exists
    if (!existsSync(DATA_DIR)) {
      mkdirSync(DATA_DIR, { recursive: true });
    }
    
    const zipPath = path.join(DATA_DIR, `fy${fiscalYear}.zip`);
    
    // Download if not already cached
    if (!existsSync(zipPath)) {
      try {
        await downloadFile(url, zipPath);
      } catch (error) {
        result.error = error instanceof Error ? error.message : 'Failed to download file';
        console.error(`Download failed for FY${fiscalYear}:`, error);
        return result;
      }
    } else {
      console.log(`Using cached file: ${zipPath}`);
    }
    
    // Extract CSV from ZIP
    const csvContent = await extractZip(zipPath);
    if (!csvContent) {
      result.error = 'Failed to extract CSV from ZIP';
      return result;
    }
    
    // Parse CSV
    console.log('Parsing CSV...');
    const allRecords = parseCSV(csvContent);
    result.totalRecords = allRecords.length;
    console.log(`Parsed ${allRecords.length} total records`);
    
    // Filter for childcare-related records
    const childcareRecords = allRecords.filter(r => 
      isChildcareRelated(r) || (isDHHS(r) && r.amount && r.amount > 1000)
    );
    result.childcareRecords = childcareRecords.length;
    result.totalAmount = childcareRecords.reduce((sum, r) => sum + (r.amount || 0), 0);
    console.log(`Found ${childcareRecords.length} childcare-related records ($${result.totalAmount.toLocaleString()})`);
    
    // Save to database
    console.log('Saving to database...');
    result.importedRecords = await saveRecords(childcareRecords, fiscalYear);
    
    result.success = true;
    console.log(`Imported ${result.importedRecords} new records`);
    
  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('Scrape error:', error);
  }
  
  return result;
}

/**
 * Scrape multiple fiscal years
 */
export async function scrapeMultipleYears(years: number[]): Promise<ScrapeResult[]> {
  const results: ScrapeResult[] = [];
  
  for (const year of years) {
    console.log(`\n=== Scraping FY${year} ===`);
    const result = await scrapeFiscalYear(year);
    results.push(result);
    
    // Small delay between years
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  return results;
}

/**
 * Scrape recent fiscal years (current and previous 2)
 */
export async function scrapeRecentYears(): Promise<ScrapeResult[]> {
  const currentYear = new Date().getMonth() >= 6 
    ? new Date().getFullYear() + 1 
    : new Date().getFullYear();
  
  const years = [currentYear, currentYear - 1, currentYear - 2].filter(y => y in FISCAL_YEAR_URLS);
  
  return scrapeMultipleYears(years);
}

/**
 * Get available fiscal years
 */
export function getAvailableFiscalYears(): number[] {
  return Object.keys(FISCAL_YEAR_URLS).map(Number).sort((a, b) => b - a);
}

// Export for use by upload endpoint
export { extractZip, parseCSV, saveRecords, isChildcareRelated, isDHHS, FISCAL_YEAR_URLS };

export default {
  scrapeFiscalYear,
  scrapeMultipleYears,
  scrapeRecentYears,
  getAvailableFiscalYears,
  extractZip,
  parseCSV,
  saveRecords,
  isChildcareRelated,
  isDHHS,
  CHILDCARE_KEYWORDS,
  IMMIGRANT_KEYWORDS,
  FISCAL_YEAR_URLS,
};
