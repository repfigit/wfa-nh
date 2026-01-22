import { query, execute, executeBatch } from '../db/db-adapter.js';
import puppeteer, { Browser, Page } from 'puppeteer';
import { existsSync, writeFileSync, mkdirSync } from 'fs';
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
 * Fetch provider data from NH CCIS via the Download Provider Results functionality
 * This calls the same API endpoint that the "Download Provider Results" button uses
 */
async function fetchProviderData(page: Page): Promise<CCISProvider[]> {
  console.log('Waiting for Visualforce remoting to be ready...');
  
  // Wait for Visualforce remoting to be available
  await page.waitForFunction(() => {
    return typeof (window as any).Visualforce !== 'undefined' && 
           typeof (window as any).Visualforce.remoting !== 'undefined';
  }, { timeout: 30000 });
  
  console.log('Fetching provider data via Download Provider Results API...');
  
  // Call the same remoting method that the Download button uses
  const rawData = await page.evaluate(() => {
    return new Promise<any[]>((resolve, reject) => {
      (window as any).Visualforce.remoting.Manager.invokeAction(
        'NH_ChildCareSearchClass.fetchProviderList', 
        false,  // Same parameter the Download button uses
        function(result: any, event: any) {
          if (event.status) {
            resolve(result);
          } else {
            reject(event.message || 'Remoting call failed');
          }
        },
        { escape: true, timeout: 120000 }
      );
    });
  });
  
  console.log(`Received ${rawData.length} records from NH CCIS`);
  
  // Map the raw Salesforce data to our schema
  return rawData.map((item: any) => ({
    program_name: item.Name || '',
    status: item.Provider_Enrollment_Status_Formula__c || '',
    phone: item.Phone || '',
    email: item.Email__c || '',
    region: item.Shipping_Region__c || '',
    county: item.Shipping_County__c || '',
    street: item.ShippingAddress?.street || '',
    city: item.ShippingAddress?.city || '',
    state: item.ShippingAddress?.state || '',
    zip: item.ShippingAddress?.postalCode || '',
    record_type: '', // Not in the API response
    gsq_step: '', // Not in the API response
    provider_number: '', // Not in the API response
    license_date: '', // Not in the API response
    license_type: '', // Not in the API response
    accepts_scholarship: item.Enrolled_in_the_NH_Child_Care_Scholar__c || '',
    accredited: '', // Not in the API response
    capacity: '', // Not in the API response
    age_groups: '', // Not in the API response
    enrollment: '', // Not in the API response
  }));
}

/**
 * Convert provider data to CSV format (for saving a local copy)
 */
function convertToCSV(providers: CCISProvider[]): string {
  const headers = [
    'Program Name', 'Provider Enrollment Status', 'Program Phone', 'Program Email',
    'Region', 'County', 'Shipping Street', 'Shipping City', 'Shipping State', 'Shipping Zip',
    'Account Record Type', 'GSQ Approved Step', 'Provider Number', 'License Issue Date', 'License Type',
    'Accepts NH Child Care Scholarship', 'Accredited', 'Licensed Capacity', 'Age Groups Served', 'Total Enrollment'
  ];
  
  const rows = providers.map(p => [
    p.program_name, p.status, p.phone, p.email, p.region, p.county,
    p.street, p.city, p.state, p.zip,
    p.record_type, p.gsq_step, p.provider_number, p.license_date, p.license_type,
    p.accepts_scholarship, p.accredited, p.capacity, p.age_groups, p.enrollment
  ].map(v => `"${(v || '').replace(/"/g, '""')}"`).join(','));
  
  return [headers.join(','), ...rows].join('\n');
}

/**
 * Main scrape function - fetches data from NH CCIS via the Download Provider Results API
 */
export async function scrapeCCIS(): Promise<ScrapeResult> {
  const result: ScrapeResult = { success: false, totalFound: 0 };
  let browser: Browser | null = null;

  try {
    console.log('Starting NH CCIS scraper...');
    console.log(`Target URL: ${CCIS_URL}`);
    
    browser = await launchBrowser();
    const page = await browser.newPage();
    
    // Set a reasonable viewport
    await page.setViewport({ width: 1280, height: 800 });
    
    console.log('Navigating to NH CCIS...');
    await page.goto(CCIS_URL, { waitUntil: 'networkidle2', timeout: 60000 });
    
    // Fetch provider data via the Download button's API
    const providers = await fetchProviderData(page);
    result.totalFound = providers.length;
    
    // Save a local CSV copy
    const csvData = convertToCSV(providers);
    const dir = dirname(CSV_PATH);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    writeFileSync(CSV_PATH, csvData);
    console.log(`Saved local copy to ${CSV_PATH}`);

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

    // 3. AUDIT: Record the run in scraped_documents
    const dbResult = await execute(`
      INSERT INTO scraped_documents (source_key, url, document_type, title, raw_content)
      VALUES (?, ?, ?, ?, ?)
    `, ['ccis', CCIS_URL, 'json', 'NH CCIS Provider Directory', JSON.stringify(providers)]);

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

export default { scrapeCCIS };
