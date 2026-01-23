import { query, execute, executeBatch } from '../db/db-adapter.js';
import puppeteer, { Browser, Page } from 'puppeteer';
import { existsSync, writeFileSync, mkdirSync, readFileSync } from 'fs';
import { resolve, dirname } from 'path';

// Interfaces
interface ScrapeResult {
  success: boolean;
  totalFound: number;
  documentId?: number;
  error?: string;
}

interface CCISProvider {
  program_name: string;
  status: string;
  phone: string;
  email: string;
  region: string;
  county: string;
  street: string;
  city: string;
  state: string;
  zip: string;
  record_type: string;
  gsq_step: string;
  provider_number: string;
  license_date: string;
  license_type: string;
  accepts_scholarship: string;
  accredited: string;
  capacity: string;
  age_groups: string;
  enrollment: string;
}

const CCIS_URL = 'https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch';
const CSV_PATH = resolve(process.cwd(), 'data/downloads/nhccis-providers.csv');

/**
 * Launch Puppeteer browser
 */
async function launchBrowser(): Promise<Browser> {
  const chromePath = '/usr/bin/google-chrome';
  const isTriggerRuntime = process.env.TRIGGER_RUN_ID !== undefined || 
    (process.env.TRIGGER_SECRET_KEY !== undefined && existsSync(chromePath));
  
  const options: any = {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  };
  
  if (isTriggerRuntime && existsSync(chromePath)) {
    options.executablePath = chromePath;
  }
  
  return puppeteer.launch(options);
}

/**
 * Download CSV file from the "Download Provider Results" link
 */
async function downloadCSVFile(page: Page, downloadDir: string): Promise<string | null> {
  console.log('Setting up download behavior...');
  
  // Set up download path using CDP
  const client = await page.target().createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: downloadDir,
  });
  
  console.log('Looking for "Download Provider Results" link...');
  
  // Wait for page to be fully loaded
  await page.waitForSelector('a, button', { timeout: 30000 });
  
  // Try multiple selectors to find the download link
  const downloadSelectors = [
    'a:has-text("Download Provider Results")',
    'a[href*="Download"]',
    'a:contains("Download Provider Results")',
    'a:contains("Download Your Search Results")',
    'a[title*="Download"]',
  ];
  
  let downloadLink = null;
  for (const selector of downloadSelectors) {
    try {
      downloadLink = await page.$(selector);
      if (downloadLink) {
        console.log(`Found download link with selector: ${selector}`);
        break;
      }
    } catch (e) {
      // Try next selector
    }
  }
  
  // If not found with selectors, try finding by text content
  if (!downloadLink) {
    console.log('Trying to find link by text content...');
    downloadLink = await page.evaluateHandle(() => {
      const links = Array.from(document.querySelectorAll('a'));
      return links.find((link: any) => 
        link.textContent?.includes('Download Provider Results') ||
        link.textContent?.includes('Download Your Search Results') ||
        link.textContent?.includes('Download') && link.textContent.includes('Provider')
      ) || null;
    });
    
    if (downloadLink && (downloadLink as any).asElement) {
      downloadLink = (downloadLink as any).asElement();
    } else {
      downloadLink = null;
    }
  }
  
  if (!downloadLink) {
    throw new Error('Could not find "Download Provider Results" link on the page');
  }
  
  console.log('Clicking download link...');
  
  // Get the list of files before download
  const filesBefore = require('fs').readdirSync(downloadDir).filter((f: string) => f.endsWith('.csv'));
  
  // Click the download link
  await downloadLink.click();
  
  // Wait for download to complete (check for new file)
  console.log('Waiting for CSV download to complete...');
  let downloadedFile: string | null = null;
  const maxWait = 30000; // 30 seconds
  const startTime = Date.now();
  
  while (Date.now() - startTime < maxWait) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    const filesAfter = require('fs').readdirSync(downloadDir).filter((f: string) => f.endsWith('.csv'));
    const newFiles = filesAfter.filter((f: string) => !filesBefore.includes(f));
    
    if (newFiles.length > 0) {
          // Find the most recently modified file
          const fileStats = newFiles.map((f: string) => {
            const fullPath = require('path').resolve(downloadDir, f);
            return {
              name: f,
              path: fullPath,
              mtime: require('fs').statSync(fullPath).mtimeMs
            };
          }).sort((a: { mtime: number }, b: { mtime: number }) => b.mtime - a.mtime);
      
      downloadedFile = fileStats[0].path;
      console.log(`Download complete: ${downloadedFile}`);
      break;
    }
  }
  
  if (!downloadedFile) {
    throw new Error('CSV download did not complete within timeout period');
  }
  
  return downloadedFile;
}

/**
 * Parse CSV file and extract CCIS provider data
 */
function parseCSVFile(csvPath: string): CCISProvider[] {
  if (!existsSync(csvPath)) {
    console.warn(`CSV file not found: ${csvPath}`);
    return [];
  }
  
  const csvContent = readFileSync(csvPath, 'utf-8');
  const lines = csvContent.split(/\r?\n/).filter(line => line.trim());
  if (lines.length < 2) {
    console.warn('CSV file has no data rows');
    return [];
  }
  
  // Parse header row
  const headerLine = lines[0];
  const headers: string[] = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < headerLine.length; i++) {
    const char = headerLine[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      headers.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  headers.push(current.trim());
  
  // Create column map (case-insensitive)
  const columnMap = new Map<string, number>();
  headers.forEach((h, idx) => {
    const normalized = h.toLowerCase().trim();
    columnMap.set(normalized, idx);
  });
  
  // Helper to get column index
  const getCol = (name: string): number => {
    const normalized = name.toLowerCase().trim();
    return columnMap.get(normalized) ?? -1;
  };
  
  // Helper to parse CSV line
  const parseLine = (line: string): string[] => {
    const values: string[] = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      if (char === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++; // Skip next quote
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
  };
  
  // Parse data rows
  const providers: CCISProvider[] = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseLine(lines[i]);
    if (values.length === 0) continue;
    
    const getValue = (colName: string): string => {
      const idx = getCol(colName);
      return idx >= 0 && idx < values.length ? values[idx] || '' : '';
    };
    
    providers.push({
      program_name: getValue('Program Name') || getValue('program name'),
      status: getValue('Provider Enrollment Status') || getValue('provider enrollment status') || getValue('status'),
      phone: getValue('Program Phone') || getValue('program phone') || getValue('phone'),
      email: getValue('Program Email') || getValue('program email') || getValue('email'),
      region: getValue('Region') || getValue('region'),
      county: getValue('County') || getValue('county'),
      street: getValue('Shipping Street') || getValue('shipping street') || getValue('street'),
      city: getValue('Shipping City') || getValue('shipping city') || getValue('city'),
      state: getValue('Shipping State') || getValue('shipping state') || getValue('state'),
      zip: getValue('Shipping Zip') || getValue('shipping zip') || getValue('zip'),
      record_type: getValue('Account Record Type') || getValue('account record type') || getValue('record type'),
      gsq_step: getValue('GSQ Approved Step') || getValue('gsq approved step') || getValue('gsq step'),
      provider_number: getValue('Provider Number') || getValue('provider number'),
      license_date: getValue('License Issue Date') || getValue('license issue date') || getValue('license date'),
      license_type: getValue('License Type') || getValue('license type'),
      accepts_scholarship: getValue('Accepts NH Child Care Scholarship') || getValue('accepts nh child care scholarship') || getValue('accepts scholarship'),
      accredited: getValue('Accredited') || getValue('accredited'),
      capacity: getValue('Licensed Capacity') || getValue('licensed capacity') || getValue('capacity'),
      age_groups: getValue('Age Groups Served') || getValue('age groups served') || getValue('age groups'),
      enrollment: getValue('Total Enrollment') || getValue('total enrollment') || getValue('enrollment'),
    });
  }
  
  return providers;
}


/**
 * Main scrape function - downloads CSV from "Download Provider Results" link
 */
export async function scrapeCCIS(): Promise<ScrapeResult> {
  const result: ScrapeResult = { success: false, totalFound: 0 };
  let browser: Browser | null = null;

  try {
    console.log('Starting NH CCIS scraper...');
    console.log(`Target URL: ${CCIS_URL}`);
    
    const downloadDir = dirname(CSV_PATH);
    if (!existsSync(downloadDir)) {
      mkdirSync(downloadDir, { recursive: true });
    }
    
    // Try to use existing CSV file first if it exists and is recent (within 24 hours)
    if (existsSync(CSV_PATH)) {
      const stats = require('fs').statSync(CSV_PATH);
      const ageHours = (Date.now() - stats.mtimeMs) / (1000 * 60 * 60);
      if (ageHours < 24) {
        console.log(`Found recent CSV file (${ageHours.toFixed(1)} hours old), parsing...`);
        const csvProviders = parseCSVFile(CSV_PATH);
        if (csvProviders.length > 0) {
          console.log(`Parsed ${csvProviders.length} providers from existing CSV file`);
          result.totalFound = csvProviders.length;
          
          // Load into database
          await loadProvidersIntoDatabase(csvProviders);
          result.success = true;
          return result;
        }
      }
    }
    
    // Download fresh CSV from website
    console.log('Downloading CSV from NH CCIS website...');
    browser = await launchBrowser();
    const page = await browser.newPage();
    
    // Set a reasonable viewport
    await page.setViewport({ width: 1280, height: 800 });
    
    console.log('Navigating to NH CCIS...');
    await page.goto(CCIS_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    
    // Wait a bit for page to fully render
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Download the CSV file
    const downloadedFile = await downloadCSVFile(page, downloadDir);
    
    if (!downloadedFile) {
      throw new Error('Failed to download CSV file');
    }
    
    // Copy to our standard location
    writeFileSync(CSV_PATH, readFileSync(downloadedFile));
    console.log(`Saved CSV to ${CSV_PATH}`);
    
    // Parse the CSV file
    const providers = parseCSVFile(CSV_PATH);
    if (providers.length === 0) {
      throw new Error('CSV file parsed but contains no provider data');
    }
    
    console.log(`Parsed ${providers.length} providers from CSV`);
    result.totalFound = providers.length;
    
    // Load into database
    await loadProvidersIntoDatabase(providers);
    
    // 3. AUDIT: Record the run in scraped_documents
    const dbResult = await execute(`
      INSERT INTO scraped_documents (source_key, url, document_type, title, raw_content)
      VALUES (?, ?, ?, ?, ?)
    `, ['ccis', CCIS_URL, 'csv', 'NH CCIS Provider Directory', JSON.stringify(providers)]);

    result.documentId = dbResult.lastId;
    result.success = true;
    console.log(`Successfully loaded source_ccis. Records: ${result.totalFound}, Audit Doc ID: ${result.documentId}`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('CCIS scrape error:', error);
  } finally {
    if (browser) await browser.close();
  }
  
  return result;
}

/**
 * Load providers into the source_ccis table
 */
async function loadProvidersIntoDatabase(providers: CCISProvider[]): Promise<void> {
  // 1. REPLACEMENT LOGIC: Wipe the dedicated source table
  console.log('Wiping previous source_ccis data...');
  await execute('DELETE FROM source_ccis');

  // 2. LOAD: Bulk insert into dedicated source table
  console.log(`Loading ${providers.length} records into source_ccis...`);
  const batchSize = 100;
  for (let i = 0; i < providers.length; i += batchSize) {
    const batch = providers.slice(i, i + batchSize);
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
      console.log(`  Inserted ${Math.min(i + batchSize, providers.length)} / ${providers.length}...`);
    }
  }
  console.log(`Successfully inserted ${providers.length} records into source_ccis`);
}

export default { scrapeCCIS };
