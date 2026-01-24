import { query, execute, executeBatch } from '../db/db-adapter.js';
import puppeteer from 'puppeteer';
import type { Browser, Page, CDPSession } from 'puppeteer';
import { existsSync, writeFileSync, mkdirSync, readFileSync, unlinkSync, statSync, readdirSync } from 'fs';
import { resolve, dirname, join } from 'path';

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
// Use /tmp for downloads in containerized environments, fallback to cwd for local dev
const DOWNLOAD_DIR = process.env.TRIGGER_ENV ? '/tmp/downloads' : resolve(process.cwd(), 'data/downloads');
const CSV_PATH = resolve(DOWNLOAD_DIR, 'nhccis-providers.csv');

/**
 * Launch Puppeteer browser with download support
 */
async function launchBrowser(): Promise<Browser> {
  // Use PUPPETEER_EXECUTABLE_PATH if set (Trigger.dev container)
  const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;

  const browser = await puppeteer.launch({
    headless: true,
    ...(executablePath && { executablePath }),
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-software-rasterizer',
    ]
  });

  return browser;
}

/**
 * Wait for a file to appear in directory with timeout
 */
async function waitForDownload(downloadDir: string, timeoutMs: number = 60000): Promise<string | null> {
  const startTime = Date.now();
  const checkInterval = 500;

  while (Date.now() - startTime < timeoutMs) {
    const files = readdirSync(downloadDir);
    // Look for CSV files, ignore .crdownload partial downloads
    const csvFiles = files.filter(f => f.endsWith('.csv') && !f.endsWith('.crdownload'));

    if (csvFiles.length > 0) {
      const filePath = join(downloadDir, csvFiles[0]);
      // Verify file has content
      const stats = statSync(filePath);
      if (stats.size > 100) {
        return filePath;
      }
    }

    await new Promise(resolve => setTimeout(resolve, checkInterval));
  }

  return null;
}

/**
 * Download CSV file from the "Download Provider Results" link using CDP
 */
async function downloadCSVFile(page: Page, downloadDir: string): Promise<string> {
  console.log('Setting up CDP download handling...');

  // Create CDP session for download handling
  const client = await page.createCDPSession();

  // Enable CDP download behavior - this is the key for headless downloads
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: downloadDir,
  });

  console.log(`CDP download path set to: ${downloadDir}`);

  await new Promise(resolve => setTimeout(resolve, 2000));

  // Find the download link
  console.log('Finding download link...');
  const linkInfo = await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a, button'));
    for (const el of links) {
      const text = (el as HTMLElement).textContent?.trim() || '';
      if (text.includes('Download Provider Results') ||
          text.includes('Download Your Search Results') ||
          (text.includes('Download') && text.includes('Provider'))) {
        const anchor = el as HTMLAnchorElement;
        return {
          tag: el.tagName,
          text: text,
          href: anchor.href || '',
          id: el.id || '',
        };
      }
    }
    return null;
  });

  if (!linkInfo) {
    throw new Error('Could not find "Download Provider Results" link');
  }

  console.log(`Found link: ${linkInfo.tag} - "${linkInfo.text}"`);

  const filePath = resolve(downloadDir, 'nhccis-providers.csv');

  // Clean existing files in download dir to detect new download
  const existingFiles = readdirSync(downloadDir);
  for (const file of existingFiles) {
    if (file.endsWith('.csv') || file.endsWith('.crdownload')) {
      unlinkSync(join(downloadDir, file));
    }
  }

  // First click: "Warm-up" - triggers server-side CSV generation
  console.log('Warm-up click: Triggering CSV generation...');
  await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a, button'));
    for (const el of links) {
      const text = (el as HTMLElement).textContent?.trim() || '';
      if (text.includes('Download Provider Results') || text.includes('Download Your Search Results')) {
        (el as HTMLElement).click();
        return;
      }
    }
  });

  console.log('Warm-up click completed, waiting 1 second for server to prepare CSV...');
  await new Promise(resolve => setTimeout(resolve, 1000));

  // Second click: Actual download with CDP handling
  console.log('Download click: Starting download...');

  // Track download progress via CDP events
  let downloadStarted = false;
  client.on('Page.downloadWillBegin', (event: any) => {
    console.log(`Download starting: ${event.suggestedFilename || 'unknown'}`);
    downloadStarted = true;
  });

  client.on('Page.downloadProgress', (event: any) => {
    if (event.state === 'completed') {
      console.log('CDP reports download completed');
    }
  });

  // Click the download link
  await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a, button'));
    for (const el of links) {
      const text = (el as HTMLElement).textContent?.trim() || '';
      if (text.includes('Download Provider Results') || text.includes('Download Your Search Results')) {
        (el as HTMLElement).click();
        return;
      }
    }
  });

  // Wait for download to complete - check for file appearing
  console.log('Waiting for download to complete...');
  const downloadedFile = await waitForDownload(downloadDir, 60000);

  if (downloadedFile) {
    // Rename to expected filename if different
    if (downloadedFile !== filePath) {
      const content = readFileSync(downloadedFile);
      writeFileSync(filePath, content);
      if (downloadedFile !== filePath) {
        unlinkSync(downloadedFile);
      }
    }
    const stats = statSync(filePath);
    console.log(`✓ Downloaded via CDP: ${filePath} (${stats.size} bytes)`);
    await client.detach();
    return filePath;
  }

  // Fallback: try response interception
  console.log('CDP download not detected, trying response interception...');

  let csvBuffer: Buffer | null = null;
  const responseHandler = async (response: any) => {
    const headers = response.headers();
    const contentType = headers['content-type'] || '';
    const contentDisposition = headers['content-disposition'] || '';

    const isCSV = contentType.includes('csv') ||
                  contentType.includes('text/csv') ||
                  contentDisposition.includes('.csv') ||
                  contentDisposition.includes('attachment');

    if (isCSV && response.status() === 200) {
      try {
        const buffer = await response.buffer();
        if (buffer.length > 100) {
          console.log(`✓ Intercepted CSV response (${buffer.length} bytes)`);
          csvBuffer = buffer;
        }
      } catch (e) {
        // Response body may already be consumed
      }
    }
  };

  page.on('response', responseHandler);

  // Click again for interception attempt
  await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a, button'));
    for (const el of links) {
      const text = (el as HTMLElement).textContent?.trim() || '';
      if (text.includes('Download Provider Results') || text.includes('Download Your Search Results')) {
        (el as HTMLElement).click();
        return;
      }
    }
  });

  await new Promise(resolve => setTimeout(resolve, 15000));
  page.off('response', responseHandler);

  if (csvBuffer !== null) {
    writeFileSync(filePath, csvBuffer);
    const stats = statSync(filePath);
    console.log(`✓ Saved from intercepted response: ${filePath} (${stats.size} bytes)`);
    await client.detach();
    return filePath;
  }

  // Final check for file
  if (existsSync(filePath)) {
    const stats = statSync(filePath);
    if (stats.size > 100) {
      console.log(`✓ Found downloaded file: ${filePath} (${stats.size} bytes)`);
      await client.detach();
      return filePath;
    }
  }

  await client.detach();
  throw new Error(`Could not download CSV. CDP download started: ${downloadStarted}, files in dir: ${readdirSync(downloadDir).join(', ') || 'none'}`);
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
  let skippedRows = 0;
  for (let i = 1; i < lines.length; i++) {
    const values = parseLine(lines[i]);
    if (values.length === 0) {
      skippedRows++;
      continue;
    }
    
    const getValue = (colName: string): string => {
      const idx = getCol(colName);
      return idx >= 0 && idx < values.length ? values[idx] || '' : '';
    };
    
    // Construct age_groups from age columns if available
    const ageWeeksLow = getValue('Age Weeks Low');
    const ageMonthsLow = getValue('Age Months Low');
    const ageYearsLow = getValue('Age Years Low');
    const ageYearsHigh = getValue('Age Years High');
    let ageGroups = getValue('Age Groups Served') || getValue('age groups served') || getValue('age groups');
    
    if (!ageGroups && (ageYearsLow || ageYearsHigh)) {
      const parts: string[] = [];
      if (ageYearsLow) parts.push(`${ageYearsLow} years`);
      if (ageYearsHigh) parts.push(`to ${ageYearsHigh} years`);
      if (parts.length > 0) ageGroups = parts.join(' ');
    }
    
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
      license_type: getValue('License Type') || getValue('license type') || getValue('License Expiration Date') || '',
      accepts_scholarship: getValue('Accepts NH Child Care Scholarship') || getValue('accepts nh child care scholarship') || getValue('accepts scholarship') || '',
      accredited: getValue('Accredited') || getValue('accredited') || '',
      capacity: getValue('Capacity') || getValue('capacity') || getValue('Licensed Capacity') || getValue('licensed capacity') || '',
      age_groups: ageGroups || '',
      enrollment: getValue('Total Enrollment') || getValue('total enrollment') || getValue('enrollment') || '',
    });
  }
  
  console.log(`Parsed ${providers.length} providers from ${lines.length - 1} CSV rows (skipped ${skippedRows} empty rows)`);
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
    
    // Use DOWNLOAD_DIR which handles container vs local environments
    if (!existsSync(DOWNLOAD_DIR)) {
      mkdirSync(DOWNLOAD_DIR, { recursive: true });
    }
    console.log(`Download directory: ${DOWNLOAD_DIR}`);
    
    // Delete any existing CSV files to ensure fresh download
    if (existsSync(CSV_PATH)) {
      unlinkSync(CSV_PATH);
      console.log('Deleted existing CSV file to ensure fresh download');
    }
    
    // Download fresh CSV from website using Puppeteer
    console.log('Downloading CSV from NH CCIS website...');
    browser = await launchBrowser();

    const page = await browser.newPage();

    // Set a reasonable viewport
    await page.setViewport({ width: 1280, height: 800 });

    console.log('Navigating to NH CCIS...');
    await page.goto(CCIS_URL, { waitUntil: 'networkidle2', timeout: 60000 });

    // Wait a bit for page to fully render
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Download the CSV file (saves directly to DOWNLOAD_DIR)
    await downloadCSVFile(page, DOWNLOAD_DIR);
    
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
 * Convert empty string or whitespace-only to null for database insertion
 */
function emptyToNull(value: string): string | null {
  if (!value || value.trim() === '') {
    return null;
  }
  return value;
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
  let insertedCount = 0;
  for (let i = 0; i < providers.length; i += batchSize) {
    const batch = providers.slice(i, i + batchSize);
    const results = await executeBatch(batch.map(row => ({
      sql: `INSERT INTO source_ccis (
        program_name, status, phone, email, region, county,
        street, city, state, zip,
        record_type, gsq_step, provider_number, license_date, license_type,
        accepts_scholarship, accredited, capacity, age_groups, enrollment
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        emptyToNull(row.program_name), emptyToNull(row.status), emptyToNull(row.phone), emptyToNull(row.email), emptyToNull(row.region), emptyToNull(row.county),
        emptyToNull(row.street), emptyToNull(row.city), emptyToNull(row.state), emptyToNull(row.zip),
        emptyToNull(row.record_type), emptyToNull(row.gsq_step), emptyToNull(row.provider_number), emptyToNull(row.license_date), emptyToNull(row.license_type),
        emptyToNull(row.accepts_scholarship), emptyToNull(row.accredited), emptyToNull(row.capacity), emptyToNull(row.age_groups), emptyToNull(row.enrollment)
      ]
    })));
    insertedCount += batch.length;
    if ((i + batchSize) % 500 === 0 || i + batchSize >= providers.length) {
      console.log(`  Inserted ${insertedCount} / ${providers.length}...`);
    }
  }
  
  // Verify all records were inserted
  const countResult = await query<{ count: number }>('SELECT COUNT(*) as count FROM source_ccis');
  const dbCount = countResult[0]?.count || 0;
  console.log(`Successfully inserted ${providers.length} records into source_ccis`);
  console.log(`Database verification: ${dbCount} records in source_ccis table`);
  
  if (dbCount !== providers.length) {
    console.warn(`⚠️  WARNING: Database count (${dbCount}) does not match parsed count (${providers.length})`);
  } else {
    console.log(`✓ Verified: All ${providers.length} records loaded successfully`);
  }
}

export default { scrapeCCIS };
