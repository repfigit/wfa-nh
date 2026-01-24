import { query, execute, executeBatch } from '../db/db-adapter.js';
import { chromium, Browser, Page, BrowserContext } from 'playwright';
import { existsSync, writeFileSync, mkdirSync, readFileSync, unlinkSync, statSync } from 'fs';
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
 * Launch Playwright browser
 */
async function launchBrowser(): Promise<{ browser: Browser; context: BrowserContext }> {
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });
  
  const context = await browser.newContext();
  return { browser, context };
}

/**
 * Download CSV file from the "Download Provider Results" link using Playwright
 */
async function downloadCSVFile(page: Page, downloadDir: string): Promise<string> {
  console.log('Setting up download handling...');
  
  await page.waitForLoadState('networkidle', { timeout: 30000 });
  await page.waitForTimeout(2000);
  
  // Intercept ALL responses for CSV data
  const csvData: Array<{ url: string; buffer: Buffer }> = [];
  page.on('response', async (response) => {
    const url = response.url();
    const contentType = response.headers()['content-type'] || '';
    const contentDisposition = response.headers()['content-disposition'] || '';
    
    const isCSV = contentType.includes('csv') || 
                  contentType.includes('text/csv') ||
                  contentDisposition.includes('.csv') ||
                  contentDisposition.includes('attachment') ||
                  url.includes('.csv') || 
                  url.includes('download') || 
                  url.includes('export');
    
    if (isCSV && response.status() === 200) {
      try {
        const buffer = await response.body();
        if (buffer.length > 100) {
          console.log(`✓ Intercepted CSV: ${url} (${buffer.length} bytes)`);
          csvData.push({ url, buffer });
        }
      } catch (e) {
        // Ignore
      }
    }
  });
  
  // Set up download event handler (will be recreated per attempt)
  
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
  const downloadLink = linkInfo.id
    ? page.locator(`#${linkInfo.id}`).first()
    : page.locator(`text="${linkInfo.text}"`).first();
  
  await downloadLink.waitFor({ state: 'visible', timeout: 10000 });
  
  // First click: "Warm-up" - triggers server-side CSV generation
  console.log('Warm-up click: Triggering CSV generation...');
  await downloadLink.click({ timeout: 10000 });
  console.log('Warm-up click completed, waiting 30 seconds for server to prepare CSV...');
  await page.waitForTimeout(30000);
  
  // Second click: Actual download (should work immediately after warm-up)
  console.log('Download click: Attempting to download prepared CSV...');
  const mainDownloadPromise = page.waitForEvent('download', { timeout: 35000 });
  await downloadLink.click({ timeout: 10000 });
  
  // Wait for download event
  try {
    const download = await mainDownloadPromise;
    const filename = download.suggestedFilename();
    if (filename) {
      await download.saveAs(filePath);
      console.log(`✓ Downloaded via download event: ${filePath}`);
      return filePath;
    }
  } catch (e: any) {
    console.log(`No download event (${e.message}), checking intercepted responses...`);
  }
  
  // Wait a bit more for responses
  await page.waitForTimeout(5000);
  
  // Check intercepted CSV data
  if (csvData.length > 0) {
    console.log(`Found ${csvData.length} CSV response(s)`);
    const largest = csvData.reduce((prev, curr) => 
      curr.buffer.length > prev.buffer.length ? curr : prev
    );
    writeFileSync(filePath, largest.buffer);
    console.log(`✓ Saved CSV from intercepted response: ${filePath} (${largest.buffer.length} bytes)`);
    return filePath;
  }
  
  // Check if file was downloaded to default location
  if (existsSync(filePath)) {
    const stats = statSync(filePath);
    if (stats.size > 100) {
      console.log(`✓ Found downloaded file: ${filePath} (${stats.size} bytes)`);
      return filePath;
    }
  }
  
  // Fallback: Try one more time if first attempt didn't work
  console.log('Fallback: Trying one more download attempt...');
  const fallbackDownloadPromise = page.waitForEvent('download', { timeout: 35000 });
  await downloadLink.click({ timeout: 10000 });
  await page.waitForTimeout(10000);
  
  try {
    const download = await fallbackDownloadPromise;
    const filename = download.suggestedFilename();
    if (filename) {
      await download.saveAs(filePath);
      console.log(`✓ Downloaded via fallback attempt: ${filePath}`);
      return filePath;
    }
  } catch (e: any) {
    // Ignore
  }
  
  if (csvData.length > 0) {
    const largest = csvData.reduce((prev, curr) => 
      curr.buffer.length > prev.buffer.length ? curr : prev
    );
    writeFileSync(filePath, largest.buffer);
    console.log(`✓ Saved CSV from fallback intercepted response: ${filePath} (${largest.buffer.length} bytes)`);
    return filePath;
  }
  
  throw new Error(`Could not download CSV after 3 attempts. Intercepted ${csvData.length} responses`);
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
    
    const downloadDir = dirname(CSV_PATH);
    if (!existsSync(downloadDir)) {
      mkdirSync(downloadDir, { recursive: true });
    }
    
    // Delete any existing CSV files to ensure fresh download
    if (existsSync(CSV_PATH)) {
      unlinkSync(CSV_PATH);
      console.log('Deleted existing CSV file to ensure fresh download');
    }
    
    // Download fresh CSV from website using Playwright
    console.log('Downloading CSV from NH CCIS website...');
    const { browser: browserInstance, context } = await launchBrowser();
    browser = browserInstance;
    
    const page = await context.newPage();
    
    // Set a reasonable viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    
    console.log('Navigating to NH CCIS...');
    await page.goto(CCIS_URL, { waitUntil: 'networkidle', timeout: 60000 });
    
    // Wait a bit for page to fully render
    await page.waitForTimeout(2000);
    
    // Download the CSV file (saves directly to CSV_PATH)
    await downloadCSVFile(page, downloadDir);
    
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
        row.program_name, row.status, row.phone, row.email, row.region, row.county,
        row.street, row.city, row.state, row.zip,
        row.record_type, row.gsq_step, row.provider_number, row.license_date, row.license_type,
        row.accepts_scholarship, row.accredited, row.capacity, row.age_groups, row.enrollment
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
