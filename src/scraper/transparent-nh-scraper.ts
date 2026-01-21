/**
 * TransparentNH Scraper
 * Downloads and processes fiscal year expenditure data from TransparentNH
 * 
 * Data source: https://www.nh.gov/transparentnh/where-the-money-goes/fiscal-yr-downloads.htm
 * Files are ZIP archives containing CSV data of all state expenditures
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';
import {
  resolveEntity,
  normalizeName,
  addProviderAlias,
  DEFAULT_MATCH_CONFIG,
} from '../matcher/entity-resolver.js';
import { existsSync, mkdirSync, writeFileSync, readFileSync } from 'fs';
import { createHash } from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Use /tmp on Vercel (read-only filesystem), local data dir otherwise
const DATA_DIR = process.env.VERCEL ? '/tmp/downloads' : path.join(__dirname, '../../data/downloads');

// TransparentNH fiscal year download URLs
// Source: https://www.nh.gov/transparentnh/where-the-money-goes/fiscal-yr-downloads.htm
const FISCAL_YEAR_BASE_URL = 'https://www.nh.gov/transparentnh/where-the-money-goes/documents';

const FISCAL_YEAR_URLS: Record<number, string> = {
  2026: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2026.zip`,
  2025: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2025.zip`,
  2024: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2024.zip`,
  2023: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2023.zip`,
  2022: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2022.zip`,
  2021: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2021.zip`,
  2020: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2020.zip`,
  2019: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2019.zip`,
  2018: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2018.zip`,
  2017: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2017.zip`,
  2016: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2016.zip`,
  2015: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2015.zip`,
  2014: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2014.zip`,
  2013: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2013.zip`,
  2012: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2012.zip`,
  2011: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2011.zip`,
  2010: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2010.zip`,
  2009: `${FISCAL_YEAR_BASE_URL}/expenditure-register-2009.zip`,
};

// Current fiscal year (updated throughout the year)
// NH fiscal year runs July 1 - June 30, so FY2026 = July 2025 - June 2026
function getCurrentFiscalYear(): number {
  const now = new Date();
  // If we're in July or later, we're in the next fiscal year
  return now.getMonth() >= 6 ? now.getFullYear() + 1 : now.getFullYear();
}

const TRANSPARENT_NH_ROOTS = [
  'https://www.nh.gov/transparentnh/where-the-money-goes/fiscal-yr-downloads.htm',
  'https://www.nh.gov/transparentnh/',
  'https://business.nh.gov/ExpenditureTransparency/',
];

const CRAWL_MAX_PAGES = 300;
const CRAWL_MAX_DEPTH = 3;
const CRAWL_DELAY_MS = 500;

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

interface CrawlResult {
  success: boolean;
  pagesVisited: number;
  pagesDiscovered: number;
  downloadLinks: Array<{
    url: string;
    text?: string;
    sourcePage: string;
  }>;
  errors: string[];
}

interface CrawlDownloadResult {
  url: string;
  savedPath?: string;
  contentType?: string;
  status?: number;
  error?: string;
}

interface IngestionSummary {
  totalRecords: number;
  childcareRecords: number;
  importedRecords: number;
  totalAmount: number;
  filesProcessed: number;
  filesWithErrors: number;
  errors: string[];
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

function normalizeUrl(input: string, base?: string): string | null {
  try {
    const url = base ? new URL(input, base) : new URL(input);
    url.hash = '';
    return url.toString();
  } catch {
    return null;
  }
}

function sameSite(url: string, root: string): boolean {
  try {
    return new URL(url).hostname === new URL(root).hostname;
  } catch {
    return false;
  }
}

const NON_DOWNLOAD_EXTENSIONS = new Set([
  'html', 'htm', 'php', 'aspx', 'asp', 'jsp', 'cfm'
]);

function getUrlExtension(url: string): string | undefined {
  try {
    const { pathname } = new URL(url);
    const match = pathname.toLowerCase().match(/\.([a-z0-9]{1,8})$/);
    return match ? match[1] : undefined;
  } catch {
    return undefined;
  }
}

function looksLikeDownload(url: string): boolean {
  const ext = getUrlExtension(url);
  if (!ext) return false;
  return !NON_DOWNLOAD_EXTENSIONS.has(ext);
}

function isZipUrl(url: string): boolean {
  const ext = getUrlExtension(url);
  return ext === 'zip';
}

function isAllowedDomain(url: string): boolean {
  try {
    const host = new URL(url).hostname;
    return host === 'nh.gov' || host.endsWith('.nh.gov') || host.endsWith('.state.nh.us');
  } catch {
    return false;
  }
}

function isCrawlableLink(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function inferFiscalYear(input: string): number | undefined {
  const fyMatch = input.match(/(?:fy|fiscal[-_\s]?year)[^0-9]*(20\d{2})/i);
  if (fyMatch) {
    return parseInt(fyMatch[1], 10);
  }
  const yearMatch = input.match(/\b(20\d{2})\b/);
  if (yearMatch) {
    return parseInt(yearMatch[1], 10);
  }
  return undefined;
}

function getMostCommonFiscalYear(records: TransparentNHRecord[]): number | undefined {
  const counts = new Map<number, number>();
  for (const record of records) {
    if (!record.fiscalYear) continue;
    const year = record.fiscalYear;
    counts.set(year, (counts.get(year) || 0) + 1);
  }

  let bestYear: number | undefined;
  let bestCount = 0;
  for (const [year, count] of counts.entries()) {
    if (count > bestCount || (count === bestCount && (bestYear || 0) < year)) {
      bestYear = year;
      bestCount = count;
    }
  }

  return bestYear;
}

function resolveFiscalYear(records: TransparentNHRecord[], sourceUrl?: string, filePath?: string): number | undefined {
  return getMostCommonFiscalYear(records) ||
    (sourceUrl ? inferFiscalYear(sourceUrl) : undefined) ||
    (filePath ? inferFiscalYear(path.basename(filePath)) : undefined);
}

function isHtmlContentType(contentType?: string | null): boolean {
  return !!contentType && contentType.toLowerCase().includes('text/html');
}

function makeFileName(url: string): string {
  const parsed = new URL(url);
  const baseName = path.basename(parsed.pathname) || 'download';
  const hash = createHash('sha1').update(url).digest('hex').slice(0, 8);
  return `${hash}-${baseName}`;
}

async function fetchPage(url: string): Promise<{ ok: boolean; status: number; html?: string; contentType?: string; error?: string }> {
  try {
    const response = await fetch(url, {
      headers: getBrowserHeaders(),
      redirect: 'follow',
    });

    const contentType = response.headers.get('content-type') || undefined;
    if (!response.ok) {
      return { ok: false, status: response.status, contentType, error: `HTTP ${response.status}` };
    }

    if (!isHtmlContentType(contentType)) {
      return { ok: true, status: response.status, contentType };
    }

    const html = await response.text();
    return { ok: true, status: response.status, contentType, html };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error';
    return { ok: false, status: 0, error: message };
  }
}

function extractLinks(html: string, baseUrl: string): string[] {
  const $ = cheerio.load(html);
  const links: string[] = [];

  $('a[href], link[href], script[src]').each((_, el) => {
    const attr = $(el).attr('href') || $(el).attr('src');
    if (!attr) return;
    const normalized = normalizeUrl(attr, baseUrl);
    if (normalized) {
      links.push(normalized);
    }
  });

  return links;
}

async function crawlTransparentNH(
  roots: string[],
  maxPages: number,
  maxDepth: number
): Promise<CrawlResult> {
  const queue: Array<{ url: string; depth: number; source?: string }> = [];
  const visited = new Set<string>();
  const discovered = new Set<string>();
  const downloadLinks: CrawlResult['downloadLinks'] = [];
  const errors: string[] = [];

  for (const root of roots) {
    const normalized = normalizeUrl(root);
    if (normalized) {
      queue.push({ url: normalized, depth: 0 });
      discovered.add(normalized);
    }
  }

  while (queue.length > 0 && visited.size < maxPages) {
    const current = queue.shift();
    if (!current) break;

    const { url, depth } = current;
    if (visited.has(url)) continue;
    visited.add(url);

    const page = await fetchPage(url);
    if (!page.ok) {
      errors.push(`${url}: ${page.error || page.status}`);
      continue;
    }

    if (page.contentType && !isHtmlContentType(page.contentType)) {
      if (looksLikeDownload(url)) {
        downloadLinks.push({ url, sourcePage: current.source || url });
      }
      continue;
    }

    if (!page.html) continue;

    const links = extractLinks(page.html, url);
    for (const link of links) {
      if (!isCrawlableLink(link)) continue;
      if (!isAllowedDomain(link)) continue;

      if (!discovered.has(link)) {
        discovered.add(link);
      }

      if (looksLikeDownload(link)) {
        downloadLinks.push({ url: link, sourcePage: url });
        continue;
      }

      if (depth + 1 <= maxDepth) {
        if (!visited.has(link)) {
          queue.push({ url: link, depth: depth + 1, source: url });
        }
      }
    }

    await new Promise(resolve => setTimeout(resolve, CRAWL_DELAY_MS));
  }

  return {
    success: errors.length === 0,
    pagesVisited: visited.size,
    pagesDiscovered: discovered.size,
    downloadLinks: downloadLinks.filter((value, index, self) => self.findIndex(v => v.url === value.url) === index),
    errors,
  };
}

async function downloadDiscoveredFiles(
  links: CrawlResult['downloadLinks'],
  dataDir: string
): Promise<CrawlDownloadResult[]> {
  const results: CrawlDownloadResult[] = [];

  for (const link of links) {
    const url = link.url;
    const fileName = makeFileName(url);
    const destPath = path.join(dataDir, fileName);

    try {
      if (!existsSync(dataDir)) {
        mkdirSync(dataDir, { recursive: true });
      }

      if (existsSync(destPath)) {
        results.push({ url, savedPath: destPath, status: 200 });
        continue;
      }

      // Use the robust downloadFile function logic here
      const success = await downloadFile(url, destPath);
      
      if (success) {
        if (isZipUrl(url)) {
          const zipName = path.basename(new URL(url).pathname) || fileName;
          const zipTarget = path.join(dataDir, zipName);
          if (!existsSync(zipTarget)) {
            const buffer = readFileSync(destPath);
            writeFileSync(zipTarget, buffer);
          }
        }
        results.push({ url, savedPath: destPath, status: 200 });
      } else {
        results.push({ url, error: 'Download failed' });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      results.push({ url, error: message });
    }

    await new Promise(resolve => setTimeout(resolve, CRAWL_DELAY_MS));
  }

  return results;
}

async function logIngestionRun(summary: {
  source: string;
  status: 'started' | 'completed' | 'failed';
  startedAt: string;
  completedAt?: string;
  recordsProcessed: number;
  recordsImported: number;
  details?: Record<string, unknown>;
  errorMessage?: string;
}): Promise<void> {
  await initializeDb();
  const detailsJson = summary.details ? JSON.stringify(summary.details) : null;
  await execute(
    `INSERT INTO ingestion_runs (
      source, status, started_at, completed_at,
      records_processed, records_imported, details, error_message
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    , [
      summary.source,
      summary.status,
      summary.startedAt,
      summary.completedAt || null,
      summary.recordsProcessed,
      summary.recordsImported,
      detailsJson,
      summary.errorMessage || null,
    ]
  );
}

async function ingestZipFile(zipPath: string, sourceUrl: string | undefined, dryRun: boolean): Promise<{
  fiscalYear?: number;
  totalRecords: number;
  childcareRecords: number;
  importedRecords: number;
  totalAmount: number;
  error?: string;
}> {
  try {
    const csvContent = await extractZip(zipPath);
    if (!csvContent) {
      return { totalRecords: 0, childcareRecords: 0, importedRecords: 0, totalAmount: 0, error: 'Failed to extract CSV' };
    }

    const allRecords = parseCSV(csvContent);
    const fiscalYear = resolveFiscalYear(allRecords, sourceUrl, zipPath);

    const childcareRecords = allRecords.filter(r =>
      isChildcareRelated(r) || (isDHHS(r) && r.amount && r.amount > 1000)
    );

    const totalAmount = childcareRecords.reduce((sum, r) => sum + (r.amount || 0), 0);

    const importedRecords = fiscalYear && !dryRun
      ? await saveRecords(childcareRecords, fiscalYear)
      : 0;

    return {
      fiscalYear,
      totalRecords: allRecords.length,
      childcareRecords: childcareRecords.length,
      importedRecords,
      totalAmount,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error';
    return { totalRecords: 0, childcareRecords: 0, importedRecords: 0, totalAmount: 0, error: message };
  }
}

function groupLinksByFiscalYear(links: CrawlResult['downloadLinks']): Record<number, string> {
  const map: Record<number, string> = {};
  for (const link of links) {
    const match = link.url.match(/fy(\d{4})/i);
    const inferred = match ? parseInt(match[1], 10) : inferFiscalYear(link.url);
    if (inferred) {
      map[inferred] = link.url;
    }
  }
  return map;
}

/**
 * Download a file from URL with retry logic and browser emulation
 */
async function downloadFile(url: string, destPath: string, maxRetries = 3): Promise<boolean> {
  const downloadPageUrl = 'https://www.nh.gov/transparentnh/where-the-money-goes/fiscal-yr-downloads.htm';

  // Ensure download directory exists
  const downloadDir = path.dirname(destPath);
  if (!existsSync(downloadDir)) {
    mkdirSync(downloadDir, { recursive: true });
  }

  let browser = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Download attempt ${attempt}/${maxRetries} using Puppeteer for ${url}...`);

      // Add random delay between attempts
      if (attempt > 1) {
        await randomSleep(2000, 5000);
      }

      browser = await puppeteer.launch({
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas',
          '--disable-gpu',
        ],
      });

      const page = await browser.newPage();
      await page.setViewport({ width: 1920, height: 1080 });
      await page.setUserAgent(
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      );

      // Set up download behavior
      const client = await page.createCDPSession();
      await client.send('Page.setDownloadBehavior', {
        behavior: 'allow',
        downloadPath: downloadDir,
      });

      // First visit the download page to establish session/cookies
      console.log('Visiting download page to establish session...');
      await page.goto(downloadPageUrl, { waitUntil: 'networkidle2', timeout: 30000 });
      await randomSleep(1000, 2000);

      // Now navigate to the file URL to trigger download
      console.log('Triggering download...');

      // Use page.evaluate to click the link or fetch the file
      const downloadPromise = new Promise<Buffer>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Download timeout')), 60000);

        page.on('response', async (response) => {
          if (response.url() === url || response.url().includes('expenditure-register')) {
            try {
              const buffer = await response.buffer();
              clearTimeout(timeout);
              resolve(buffer);
            } catch (e) {
              // Response might not have a body
            }
          }
        });
      });

      // Navigate to the download URL
      await page.goto(url, { waitUntil: 'networkidle0', timeout: 60000 }).catch(() => {
        // Navigation might "fail" because it's a download, not a page
      });

      // Wait a bit for download to complete
      await randomSleep(3000, 5000);

      // Check if file was downloaded directly to the download directory
      const fileName = path.basename(url);
      const downloadedPath = path.join(downloadDir, fileName);

      let fileBuffer: Buffer | null = null;

      // Try to get the buffer from the response
      try {
        fileBuffer = await Promise.race([
          downloadPromise,
          new Promise<Buffer>((_, reject) => setTimeout(() => reject(new Error('timeout')), 10000))
        ]);
      } catch {
        // Check if file exists in download directory
        if (existsSync(downloadedPath)) {
          fileBuffer = readFileSync(downloadedPath) as unknown as Buffer;
        } else if (existsSync(destPath)) {
          fileBuffer = readFileSync(destPath) as unknown as Buffer;
        }
      }

      await browser.close();
      browser = null;

      if (!fileBuffer) {
        // Last resort: try direct fetch with cookies from the browser session
        console.log('Puppeteer download did not capture file, trying direct download...');
        throw new Error('Could not capture download');
      }

      // Verify it's a valid ZIP (starts with PK magic bytes)
      const bytes = new Uint8Array(fileBuffer);
      if (bytes.length < 4 || bytes[0] !== 0x50 || bytes[1] !== 0x4B) {
        console.log('Downloaded file is not a valid ZIP');
        throw new Error('Downloaded file is not a valid ZIP archive');
      }

      writeFileSync(destPath, fileBuffer);
      console.log(`Successfully downloaded to ${destPath} (${fileBuffer.length} bytes)`);
      return true;

    } catch (error) {
      console.error(`Attempt ${attempt} failed:`, error);
      if (browser) {
        await browser.close();
        browser = null;
      }
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
 * Handles ZIPs with multiple monthly CSV files by concatenating them
 */
async function extractZip(zipPath: string): Promise<string | null> {
  try {
    // Use dynamic import for JSZip
    const JSZip = (await import('jszip')).default;
    const fs = await import('fs');

    const zipData = fs.readFileSync(zipPath);
    const zip = await JSZip.loadAsync(zipData);

    // Find ALL CSV files in the ZIP
    const csvFileNames = Object.keys(zip.files).filter(name =>
      name.toLowerCase().endsWith('.csv') && !zip.files[name].dir
    );

    if (csvFileNames.length === 0) {
      console.error('No CSV file found in ZIP');
      return null;
    }

    console.log(`Found ${csvFileNames.length} CSV files in ZIP`);

    // Extract and concatenate all CSV files
    let combinedContent = '';
    let headerLine = '';
    let isFirstFile = true;

    for (const csvFileName of csvFileNames) {
      console.log(`Extracting ${csvFileName}...`);
      const csvContent = await zip.files[csvFileName].async('string');

      const lines = csvContent.split(/\r?\n/);

      if (isFirstFile) {
        // Keep the header from the first file
        headerLine = lines[0];
        combinedContent = csvContent;
        isFirstFile = false;
      } else {
        // Skip the header line for subsequent files
        const dataLines = lines.slice(1).filter(line => line.trim());
        if (dataLines.length > 0) {
          combinedContent += '\n' + dataLines.join('\n');
        }
      }
    }

    console.log(`Combined ${csvFileNames.length} CSV files`);
    return combinedContent;
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
          case 'expenditure_class':
          case 'expenditure_class_name':
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
          case 'dollar_amount':
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
 * Save records to the database using entity resolver for provider matching
 */
async function saveRecords(records: TransparentNHRecord[], fiscalYear: number): Promise<number> {
  await initializeDb();
  let savedCount = 0;

  for (const record of records) {
    try {
      // Use entity resolver instead of weak substring matching
      let providerMasterId: number | null = null;
      let legacyProviderId: number | null = null;

      if (record.vendorName) {
        // Create a unique identifier for this expenditure record
        const sourceIdentifier = `TNH-${fiscalYear}-${record.vendorName}-${record.amount || 0}-${record.transactionDate || 'nodate'}`;

        // Use the entity resolver to find matching provider_master record
        const matchResult = await resolveEntity({
          name: record.vendorName,
          address: null,  // TransparentNH doesn't provide address
          city: null,
          zip: null,
          phone: null,
          sourceSystem: 'transparent_nh',
          sourceIdentifier: sourceIdentifier,
        }, {
          ...DEFAULT_MATCH_CONFIG,
          // Be more lenient since we only have vendor name
          autoMatchThreshold: 0.80,
          reviewThreshold: 0.55,
        });

        if (matchResult.matched && matchResult.providerId) {
          providerMasterId = matchResult.providerId;

          // Add vendor name as alias if it's different from the matched name
          const normalizedVendor = normalizeName(record.vendorName);
          await addProviderAlias(providerMasterId, record.vendorName, 'vendor_name', 'transparent_nh', matchResult.score);
        }

        // Also try to match to legacy providers table for backward compatibility
        const legacyMatch = await query(
          `SELECT id FROM providers WHERE name = ? LIMIT 1`,
          [record.vendorName]
        );
        if (legacyMatch.length > 0) {
          legacyProviderId = legacyMatch[0].id as number;
        }
      }

      // Check for duplicate expenditure
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

      // Insert expenditure with provider_master_id (new) and provider_id (legacy)
      await execute(`
        INSERT INTO expenditures (
          provider_id, fiscal_year, department, agency, activity,
          expense_class, vendor_name, amount, payment_date, description,
          source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        legacyProviderId,  // Legacy provider_id for backward compatibility
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

      // Create legacy provider if childcare-related and not matched
      // (provider_master should already exist from CCIS - this is fallback)
      if (!legacyProviderId && isChildcareRelated(record)) {
        const isImmigrant = isImmigrantRelated(record);

        await execute(`
          INSERT OR IGNORE INTO providers (name, accepts_ccdf, is_immigrant_owned, notes)
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
    // Prefer crawler discovery over hardcoded fallbacks
    let url: string | undefined;
    
    console.log(`Searching for FY${fiscalYear} download link via crawler...`);
    const crawl = await crawlTransparentNH(TRANSPARENT_NH_ROOTS, CRAWL_MAX_PAGES, CRAWL_MAX_DEPTH);
    const byYear = groupLinksByFiscalYear(crawl.downloadLinks);
    url = byYear[fiscalYear];

    if (!url) {
      console.log(`Crawler did not find FY${fiscalYear}. Using fallback URL if available...`);
      url = FISCAL_YEAR_URLS[fiscalYear];
    }

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

export async function crawlAndDownloadTransparentNH(): Promise<{ crawl: CrawlResult; downloads: CrawlDownloadResult[] }> {
  const crawl = await crawlTransparentNH(TRANSPARENT_NH_ROOTS, CRAWL_MAX_PAGES, CRAWL_MAX_DEPTH);
  const downloads = await downloadDiscoveredFiles(crawl.downloadLinks, DATA_DIR);
  return { crawl, downloads };
}

export async function crawlDownloadAndIngestTransparentNH(): Promise<{
  crawl: CrawlResult;
  downloads: CrawlDownloadResult[];
  ingest: IngestionSummary;
}> {
  return crawlDownloadAndIngestTransparentNHWithOptions({ dryRun: false });
}

export async function crawlDownloadAndIngestTransparentNHWithOptions(options: {
  dryRun: boolean;
}): Promise<{
  crawl: CrawlResult;
  downloads: CrawlDownloadResult[];
  ingest: IngestionSummary;
}> {
  const startedAt = new Date().toISOString();
  const crawl = await crawlTransparentNH(TRANSPARENT_NH_ROOTS, CRAWL_MAX_PAGES, CRAWL_MAX_DEPTH);
  const downloads = await downloadDiscoveredFiles(crawl.downloadLinks, DATA_DIR);

  const zipDownloads = downloads.filter(d => d.savedPath && isZipUrl(d.url));
  let totalRecords = 0;
  let childcareRecords = 0;
  let importedRecords = 0;
  let totalAmount = 0;
  const errors: string[] = [];

  for (const download of zipDownloads) {
    const zipPath = download.savedPath as string;
    const result = await ingestZipFile(zipPath, download.url, options.dryRun);

    totalRecords += result.totalRecords;
    childcareRecords += result.childcareRecords;
    importedRecords += result.importedRecords;
    totalAmount += result.totalAmount;

    if (result.error) {
      errors.push(`${download.url}: ${result.error}`);
    }
  }

  const ingest: IngestionSummary = {
    totalRecords,
    childcareRecords,
    importedRecords,
    totalAmount,
    filesProcessed: zipDownloads.length,
    filesWithErrors: errors.length,
    errors,
  };

  const crawlErrors = crawl.errors.concat(downloads.filter(d => d.error).map(d => `${d.url}: ${d.error}`));
  const allErrors = crawlErrors.concat(ingest.errors);
  const completedAt = new Date().toISOString();

  await logIngestionRun({
    source: 'TransparentNH Crawl Ingest',
    status: allErrors.length > 0 ? 'failed' : 'completed',
    startedAt,
    completedAt,
    recordsProcessed: ingest.totalRecords,
    recordsImported: ingest.importedRecords,
    details: {
      dryRun: options.dryRun,
      crawl: {
        pagesVisited: crawl.pagesVisited,
        pagesDiscovered: crawl.pagesDiscovered,
        downloadsAttempted: downloads.length,
      },
      ingest,
      downloadErrors: crawlErrors,
    },
    errorMessage: allErrors.length > 0 ? allErrors.slice(0, 10).join(' | ') : undefined,
  });

  return {
    crawl,
    downloads,
    ingest,
  };
}

/**
 * Scrape recent fiscal years (current and previous 2)
 */
export async function scrapeRecentYears(): Promise<ScrapeResult[]> {
  const currentYear = getCurrentFiscalYear();

  const years = [currentYear, currentYear - 1, currentYear - 2];
  const available = years.filter(y => y in FISCAL_YEAR_URLS);

  if (available.length > 0) {
    return scrapeMultipleYears(available);
  }

  const crawl = await crawlTransparentNH(TRANSPARENT_NH_ROOTS, CRAWL_MAX_PAGES, CRAWL_MAX_DEPTH);
  const byYear = groupLinksByFiscalYear(crawl.downloadLinks);
  const discoveredYears = years.filter(y => !!byYear[y]);

  return scrapeMultipleYears(discoveredYears);
}

/**
 * Scrape the current fiscal year only
 * Use this for recurring/scheduled updates since FY data is updated throughout the year
 */
export async function scrapeCurrentFiscalYear(): Promise<ScrapeResult> {
  const currentYear = getCurrentFiscalYear();
  console.log(`Scraping current fiscal year: FY ${currentYear}`);
  return scrapeFiscalYear(currentYear);
}

/**
 * Scrape ALL historical fiscal years (initial full ingestion)
 * This downloads and processes all available data from FY 2009 to present
 * WARNING: This is a large operation - use for initial setup only
 */
export async function scrapeAllHistoricalYears(): Promise<ScrapeResult[]> {
  const allYears = Object.keys(FISCAL_YEAR_URLS)
    .map(Number)
    .sort((a, b) => a - b); // Process oldest to newest

  console.log(`Starting full historical ingestion: ${allYears.length} fiscal years (FY ${allYears[0]} - FY ${allYears[allYears.length - 1]})`);
  console.log('This may take a while...\n');

  const results: ScrapeResult[] = [];

  for (const year of allYears) {
    console.log(`\n=== Processing FY ${year} (${results.length + 1}/${allYears.length}) ===`);
    try {
      const result = await scrapeFiscalYear(year);
      results.push(result);

      if (result.success) {
        console.log(`FY ${year}: ${result.childcareRecords} childcare records, $${result.totalAmount.toLocaleString()}`);
      } else {
        console.log(`FY ${year}: Failed - ${result.error}`);
      }

      // Small delay between years to be respectful to the server
      await randomSleep(1000, 2000);
    } catch (error) {
      console.error(`FY ${year}: Exception - ${error}`);
      results.push({
        success: false,
        fiscalYear: year,
        totalRecords: 0,
        childcareRecords: 0,
        importedRecords: 0,
        totalAmount: 0,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  }

  // Summary
  const successful = results.filter(r => r.success);
  const totalChildcare = successful.reduce((sum, r) => sum + r.childcareRecords, 0);
  const totalAmount = successful.reduce((sum, r) => sum + r.totalAmount, 0);

  console.log('\n=== Full Historical Ingestion Complete ===');
  console.log(`Years processed: ${results.length}`);
  console.log(`Successful: ${successful.length}`);
  console.log(`Total childcare records: ${totalChildcare.toLocaleString()}`);
  console.log(`Total amount: $${totalAmount.toLocaleString()}`);

  return results;
}

/**
 * Get available fiscal years
 */
export function getAvailableFiscalYears(): number[] {
  return Object.keys(FISCAL_YEAR_URLS).map(Number).sort((a, b) => b - a);
}

// Export for use by upload endpoint and scheduled tasks
export { extractZip, parseCSV, saveRecords, isChildcareRelated, isDHHS, FISCAL_YEAR_URLS, getCurrentFiscalYear };

export default {
  scrapeFiscalYear,
  scrapeMultipleYears,
  scrapeRecentYears,
  scrapeCurrentFiscalYear,
  scrapeAllHistoricalYears,
  getAvailableFiscalYears,
  getCurrentFiscalYear,
  crawlAndDownloadTransparentNH,
  crawlDownloadAndIngestTransparentNH,
  crawlDownloadAndIngestTransparentNHWithOptions,
  extractZip,
  parseCSV,
  saveRecords,
  isChildcareRelated,
  isDHHS,
  CHILDCARE_KEYWORDS,
  IMMIGRANT_KEYWORDS,
  FISCAL_YEAR_URLS,
};
