import { query, execute, executeBatch } from '../db/db-adapter.js';
import puppeteer, { Browser, Page } from 'puppeteer';
import { existsSync } from 'fs';

// Interfaces
interface ScrapeResult {
  success: boolean;
  totalFound: number;
  documentId?: number;
  error?: string;
}

const CCIS_URL = 'https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch';

/**
 * Launch Puppeteer browser
 */
async function launchBrowser(): Promise<Browser> {
  const chromePath = '/usr/bin/google-chrome';
  const isTriggerRuntime = process.env.TRIGGER_RUN_ID !== undefined || (process.env.TRIGGER_SECRET_KEY !== undefined && existsSync(chromePath));
  const options: any = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] };
  if (isTriggerRuntime && existsSync(chromePath)) options.executablePath = chromePath;
  return puppeteer.launch(options);
}

/**
 * Fetch data via Visualforce
 */
async function downloadProviderResults(page: Page): Promise<{ csvData: string; count: number } | null> {
  let resolveData: (data: string) => void;
  let rejectData: (err: Error) => void;
  const dataPromise = new Promise<string>((resolve, reject) => {
    resolveData = resolve;
    rejectData = reject;
  });

  await page.exposeFunction('__ccis_sendData', (csvData: string) => resolveData(csvData));
  await page.exposeFunction('__ccis_sendError', (errorMsg: string) => rejectData(new Error(errorMsg)));

  await page.evaluate(() => {
    const w = window as any;
    const vf = w.Visualforce;
    if (!vf?.remoting?.Manager) { w.__ccis_sendError('Visualforce remoting not available'); return; }

    const keys = ['Name', 'Provider_Enrollment_Status_Formula__c', 'Phone', 'Email__c',
      'Shipping_Region__c', 'Shipping_County__c', 'ShippingStreet', 'ShippingCity',
      'ShippingState', 'ShippingPostalCode', 'Account_Record_Type__c', 'GSQ_Approved_Step_1__c',
      'License_Number__c', 'License_Date__c', 'License_Type__c', 'Accepts_NH_Child_Care_Scholarship__c',
      'Accredited__c', 'Facility_Capacity_Formula__c', 'AgesServedFormula__c', 'Total_Enrollment__c'];

    const headers = ['program_name', 'status', 'phone', 'email', 'region', 'county', 'street', 'city', 'state', 'zip', 'record_type', 'gsq_step', 'provider_number', 'license_date', 'license_type', 'accepts_scholarship', 'accredited', 'capacity', 'age_groups', 'enrollment'];

    vf.remoting.Manager.invokeAction('NH_ChildCareSearchClass.fetchProviderList', false, function (result: any, event: any) {
        if (event.status && result && Array.isArray(result)) {
          const csvRows = [headers.join(',')];
          for (const row of result) {
            const values = keys.map(k => `"${String(row[k] || '').replace(/"/g, '""')}"`);
            csvRows.push(values.join(','));
          }
          w.__ccis_sendData(csvRows.join('\n'));
        } else w.__ccis_sendError(event.message || 'Remote action failed');
      }, { escape: false, timeout: 120000 });
  });

  try {
    const csvData = await Promise.race([dataPromise, new Promise<never>((_, reject) => setTimeout(() => reject(new Error('Timeout')), 120000))]);
    const lines = csvData.split('\n').filter(l => l.trim());
    return { csvData, count: lines.length > 0 ? lines.length - 1 : 0 };
  } catch { return null; }
}

/**
 * Main scrape function
 */
export async function scrapeCCIS(): Promise<ScrapeResult> {
  const result: ScrapeResult = { success: false, totalFound: 0 };
  let browser: Browser | null = null;

  try {
    console.log('Starting NH CCIS scraper (Raw extraction to source_ccis)...');
    browser = await launchBrowser();
    const page = await browser.newPage();
    await page.goto(CCIS_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    
    // Use aria selector to pierce Shadow DOM (Salesforce LWC)
    const searchBtn = await page.waitForSelector('aria/Search', { timeout: 60000, visible: true });
    if (!searchBtn) throw new Error('Search button not found');
    await searchBtn.click();
    await new Promise(r => setTimeout(r, 5000));

    const download = await downloadProviderResults(page);
    if (!download) throw new Error('Download failed');

    const rows = parseCSVToObjects(download.csvData);
    result.totalFound = rows.length;

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
          program_name, status, phone, email, region, county, street, city, state, zip,
          record_type, gsq_step, provider_number, license_date, license_type,
          accepts_scholarship, accredited, capacity, age_groups, enrollment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          row.program_name, row.status, row.phone, row.email, row.region, row.county, 
          row.street, row.city, row.state, row.zip, row.record_type, row.gsq_step, 
          row.provider_number, row.license_date, row.license_type, row.accepts_scholarship, 
          row.accredited, row.capacity, row.age_groups, row.enrollment
        ]
      })));
    }

    // 3. AUDIT: Record the run in scraped_documents
    const dbResult = await execute(`
      INSERT INTO scraped_documents (source_key, url, document_type, title, raw_content)
      VALUES (?, ?, ?, ?, ?)
    `, ['ccis', CCIS_URL, 'csv', 'NH CCIS Main Directory', download.csvData]);

    result.documentId = dbResult.lastId;
    result.success = true;
    console.log(`Successfully updated source_ccis. Records: ${result.totalFound}, Audit Doc ID: ${result.documentId}`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('CCIS scrape error:', error);
  } finally {
    if (browser) await browser.close();
  }
  return result;
}

function parseCSVToObjects(csv: string): any[] {
  const lines = csv.split('\n').filter(l => l.trim());
  const headers = lines[0].split(',').map(h => h.trim());
  return lines.slice(1).map(line => {
    // Basic CSV parser that handles quotes
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
    
    const obj: any = {};
    headers.forEach((h, i) => obj[h] = (values[i] || '').replace(/^"|"$/g, ''));
    return obj;
  });
}

export default { scrapeCCIS };
