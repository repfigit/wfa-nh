/**
 * NH CCIS (Child Care Information System) Scraper
 * Downloads provider data from NH Child Care Search portal using Puppeteer
 *
 * Data source: https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch
 * This is a Salesforce-based portal that generates CSV downloads client-side
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';
import {
  normalizeName,
  normalizeAddress,
  normalizePhone,
  normalizeZip,
  logMatchAudit,
  addProviderAlias,
} from '../matcher/entity-resolver.js';
import puppeteer, { Browser, Page } from 'puppeteer';
import { existsSync, mkdirSync, writeFileSync, readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.VERCEL ? '/tmp/downloads' : path.join(__dirname, '../../data/downloads');

const CCIS_URL = 'https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch';

// Program type mappings from the portal
const PROGRAM_TYPES: Record<string, string> = {
  'Licensed Child Care Center': 'center',
  'Licensed Family Child Care': 'family',
  'License-Exempt Child Care Program': 'exempt',
  'Licensed Plus': 'licensed_plus',
  'Family Resource Center': 'resource_center',
};

interface CCISProvider {
  providerName: string;
  programType?: string;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  phone?: string;
  email?: string;
  county?: string;
  acceptsSubsidy?: boolean;
  qualityRating?: string;
  capacity?: number;
  ageGroups?: string[];
  hoursOfOperation?: string;
  accredited?: boolean;
  providerId?: string;
}

interface ScrapeResult {
  success: boolean;
  providers: CCISProvider[];
  totalFound: number;
  imported: number;
  updated: number;
  error?: string;
  downloadPath?: string;
}

/**
 * Launch Puppeteer browser with appropriate settings
 */
async function launchBrowser(): Promise<Browser> {
  // Check if we're running in Trigger.dev environment
  const isTriggerDev = process.env.TRIGGER_SECRET_KEY !== undefined;
  
  if (isTriggerDev) {
    // In Trigger.dev (and other containerized environments), we use the installed system Chrome
    return puppeteer.launch({
      executablePath: '/usr/bin/google-chrome',
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--disable-gpu',
        '--window-size=1920,1080',
      ],
    });
  }

  // Local development fallback
  return puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--disable-gpu',
      '--window-size=1920,1080',
    ],
  });
}

/**
 * Wait for page to be fully loaded (Salesforce apps can be slow)
 */
async function waitForPageLoad(page: Page): Promise<void> {
  await page.waitForFunction(
    () => document.readyState === 'complete',
    { timeout: 60000 }
  );
  // Additional wait for Salesforce Lightning to initialize
  await new Promise(resolve => setTimeout(resolve, 3000));
}

/**
 * Navigate to the search page and trigger a search for all providers
 */
async function navigateAndSearch(page: Page): Promise<void> {
  console.log('Navigating to NH Child Care Search...');
  await page.goto(CCIS_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  await waitForPageLoad(page);

  console.log('Page loaded, looking for search controls...');

  // Wait for the search form to be available
  // The portal uses various Salesforce Lightning components
  await page.waitForSelector('input, button, select', { timeout: 30000 });

  // Try to trigger a search that returns all providers
  // First, check if there's already a search button we can click
  const searchButtonSelectors = [
    'button[title*="Search"]',
    'button:contains("Search")',
    'input[type="submit"]',
    '[data-aura-class*="search"]',
    '.slds-button:contains("Search")',
    'lightning-button[label="Search"]',
  ];

  for (const selector of searchButtonSelectors) {
    try {
      const button = await page.$(selector);
      if (button) {
        console.log(`Found search button with selector: ${selector}`);
        await button.click();
        await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for results
        break;
      }
    } catch {
      // Try next selector
    }
  }
}

/**
 * Download provider results by calling Visualforce remote action directly
 */
async function downloadProviderResults(page: Page): Promise<string | null> {
  console.log('Fetching provider data via Visualforce remote action...');

  // Ensure download directory exists
  if (!existsSync(DATA_DIR)) {
    mkdirSync(DATA_DIR, { recursive: true });
  }

  const downloadPath = path.join(DATA_DIR, 'nhccis-providers.csv');

  // Expose functions to receive data from page context
  let resolveData: (data: string) => void;
  let rejectData: (err: Error) => void;
  const dataPromise = new Promise<string>((resolve, reject) => {
    resolveData = resolve;
    rejectData = reject;
  });

  await page.exposeFunction('__ccis_sendData', (csvData: string) => {
    resolveData(csvData);
  });

  await page.exposeFunction('__ccis_sendError', (errorMsg: string) => {
    rejectData(new Error(errorMsg));
  });

  // CSV column mappings from the Salesforce page
  const keys = ['Name', 'Provider_Enrollment_Status_Formula__c', 'Phone', 'Email__c',
    'Shipping_Region__c', 'Shipping_County__c', 'ShippingStreet', 'ShippingCity',
    'ShippingState', 'ShippingPostalCode', 'Account_Record_Type__c', 'GSQ_Approved_Step_1__c',
    'License_Number__c', 'License_Date__c', 'License_Type__c', 'Accepts_NH_Child_Care_Scholarship__c',
    'Accredited__c', 'Facility_Capacity_Formula__c', 'AgesServedFormula__c', 'Total_Enrollment__c'];

  const headerKeys = ['Program Name', 'Provider Enrollment Status', 'Program Phone', 'Program Email',
    'Region', 'County', 'Shipping Street', 'Shipping City', 'Shipping State', 'Shipping Zip',
    'Account Record Type', 'GSQ Approved Step', 'Provider Number', 'License Issue Date',
    'License Type', 'Accepts NH Child Care Scholarship', 'Accredited', 'Licensed Capacity',
    'Age Groups Served', 'Total Enrollment'];

  // Inject and execute the fetch code
  await page.evaluate((keysArg, headerKeysArg) => {
    const w = window as any;
    const vf = w.Visualforce;

    if (!vf || !vf.remoting || !vf.remoting.Manager) {
      w.__ccis_sendError('Visualforce remoting not available');
      return;
    }

    vf.remoting.Manager.invokeAction(
      'NH_ChildCareSearchClass.fetchProviderList',
      false,
      function(result: any, event: any) {
        if (event.status && result && Array.isArray(result)) {
          // Build CSV
          const csvRows = [headerKeysArg.join(',')];
          for (let i = 0; i < result.length; i++) {
            const row = result[i];
            const values = [];
            for (let j = 0; j < keysArg.length; j++) {
              let val = row[keysArg[j]];
              if (val === null || val === undefined) {
                values.push('');
              } else {
                const str = String(val);
                if (str.indexOf(',') >= 0 || str.indexOf('"') >= 0 || str.indexOf('\n') >= 0) {
                  values.push('"' + str.replace(/"/g, '""') + '"');
                } else {
                  values.push(str);
                }
              }
            }
            csvRows.push(values.join(','));
          }
          w.__ccis_sendData(csvRows.join('\n'));
        } else {
          w.__ccis_sendError(event.message || 'Remote action failed or no data');
        }
      },
      { escape: false, timeout: 120000 }
    );
  }, keys, headerKeys);

  // Wait for data with timeout
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => reject(new Error('Timeout waiting for CCIS data')), 120000);
  });

  try {
    const csvData = await Promise.race([dataPromise, timeoutPromise]);
    writeFileSync(downloadPath, csvData);
    console.log(`CSV saved to ${downloadPath}`);
    return downloadPath;
  } catch (err) {
    console.error('Failed to fetch provider data:', err);
    return null;
  }
}

/**
 * Parse CSV content into provider records
 */
function parseCSV(csvContent: string): CCISProvider[] {
  const lines = csvContent.split(/\r?\n/).filter(line => line.trim());
  if (lines.length < 2) return [];

  // Parse header row
  const headers = parseCSVLine(lines[0]).map(h => h.toLowerCase().trim());

  const providers: CCISProvider[] = [];

  for (let i = 1; i < lines.length; i++) {
    try {
      const values = parseCSVLine(lines[i]);
      const provider: CCISProvider = {
        providerName: '',
      };

      for (let j = 0; j < headers.length && j < values.length; j++) {
        const header = headers[j];
        const value = values[j]?.trim();

        if (!value) continue;

        // Map CSV columns to provider fields
        // Handle both "Provider Name" and "Program Name" formats
        if ((header.includes('provider') && header.includes('name')) || header === 'program name') {
          provider.providerName = value;
        } else if (header.includes('program') && header.includes('type')) {
          provider.programType = value;
        } else if (header === 'address' || header.includes('street')) {
          provider.address = value;
        } else if (header === 'city' || header === 'shipping city') {
          provider.city = value;
        } else if (header === 'state' || header === 'shipping state') {
          provider.state = value;
        } else if (header === 'zip' || header.includes('postal') || header === 'shipping zip') {
          provider.zip = value;
        } else if (header.includes('phone') || header === 'program phone') {
          provider.phone = value;
        } else if (header.includes('email') || header === 'program email') {
          provider.email = value;
        } else if (header.includes('county')) {
          provider.county = value;
        } else if (header.includes('subsidy') || header.includes('scholarship') || header.includes('ccdf')) {
          provider.acceptsSubsidy = value.toLowerCase() === 'yes' || value === '1' || value.toLowerCase() === 'true';
        } else if (header.includes('quality') || header.includes('gsq') || header.includes('step')) {
          provider.qualityRating = value;
        } else if (header.includes('capacity') || header === 'licensed capacity') {
          provider.capacity = parseInt(value) || undefined;
        } else if (header.includes('age') || header === 'age groups served') {
          provider.ageGroups = value.split(/[,;]/).map(s => s.trim()).filter(Boolean);
        } else if (header.includes('hours') || header.includes('operation')) {
          provider.hoursOfOperation = value;
        } else if (header.includes('accredit')) {
          provider.accredited = value.toLowerCase() === 'yes' || value === '1' || value.toLowerCase() === 'true';
        } else if (header === 'provider number' || header === 'license number' || (header.includes('provider') && header.includes('id'))) {
          provider.providerId = value;
        } else if (header === 'license type') {
          provider.programType = value;
        }
      }

      if (provider.providerName) {
        providers.push(provider);
      }
    } catch (err) {
      // Skip malformed rows
    }
  }

  return providers;
}

/**
 * Parse a single CSV line handling quoted values
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
 * Save providers to provider_master (authoritative CCIS source)
 */
async function saveProviders(providers: CCISProvider[]): Promise<{ imported: number; updated: number }> {
  await initializeDb();
  let imported = 0;
  let updated = 0;

  for (const provider of providers) {
    try {
      const canonicalName = normalizeName(provider.providerName);
      const addressNormalized = provider.address ? normalizeAddress(provider.address) : null;
      const phoneNormalized = normalizePhone(provider.phone);
      const zip5 = normalizeZip(provider.zip);
      const cityNormalized = provider.city?.toUpperCase().trim() || null;

      const providerType = provider.programType
        ? (PROGRAM_TYPES[provider.programType] || provider.programType.toLowerCase())
        : null;

      // Generate a CCIS ID if not provided
      const ccisId = provider.providerId || `CCIS-${canonicalName.substring(0, 20).replace(/\s+/g, '-')}-${Date.now()}`;

      // Check for existing provider in provider_master by CCIS ID or canonical name + city
      const existing = await query(
        `SELECT id, ccis_provider_id FROM provider_master
         WHERE ccis_provider_id = ?
         OR (canonical_name = ? AND city = ?)`,
        [ccisId, canonicalName, cityNormalized || '']
      );

      if (existing.length > 0) {
        const masterId = existing[0].id as number;

        // Update existing master record
        await execute(`
          UPDATE provider_master SET
            ccis_provider_id = COALESCE(?, ccis_provider_id),
            canonical_name = ?,
            name_display = ?,
            address_normalized = COALESCE(?, address_normalized),
            address_display = COALESCE(?, address_display),
            city = COALESCE(?, city),
            zip = COALESCE(?, zip),
            zip5 = COALESCE(?, zip5),
            phone_normalized = COALESCE(?, phone_normalized),
            email = COALESCE(?, email),
            provider_type = COALESCE(?, provider_type),
            capacity = COALESCE(?, capacity),
            accepts_ccdf = COALESCE(?, accepts_ccdf),
            quality_rating = COALESCE(?, quality_rating),
            is_active = 1,
            last_verified_date = datetime('now'),
            updated_at = datetime('now')
          WHERE id = ?
        `, [
          provider.providerId || null,
          canonicalName,
          provider.providerName,
          addressNormalized,
          provider.address,
          cityNormalized,
          provider.zip,
          zip5,
          phoneNormalized || null,
          provider.email || null,
          providerType,
          provider.capacity || null,
          provider.acceptsSubsidy ? 1 : null,
          provider.qualityRating || null,
          masterId,
        ]);

        // Log the update
        await logMatchAudit(
          masterId,
          'ccis',
          ccisId,
          provider.providerName,
          'updated'
        );

        updated++;
      } else {
        // Insert new master record
        const result = await execute(`
          INSERT INTO provider_master (
            ccis_provider_id, canonical_name, name_display,
            address_normalized, address_display, city, state, zip, zip5,
            phone_normalized, email, provider_type, capacity,
            accepts_ccdf, quality_rating, is_active,
            first_seen_date, last_verified_date
          ) VALUES (?, ?, ?, ?, ?, ?, 'NH', ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
        `, [
          ccisId,
          canonicalName,
          provider.providerName,
          addressNormalized,
          provider.address || null,
          cityNormalized,
          provider.zip || null,
          zip5 || null,
          phoneNormalized || null,
          provider.email || null,
          providerType,
          provider.capacity || null,
          provider.acceptsSubsidy ? 1 : 0,
          provider.qualityRating || null,
        ]);

        const masterId = result.lastId as number;

        // Create CCIS source link
        await execute(`
          INSERT OR IGNORE INTO provider_source_links (
            provider_master_id, source_system, source_identifier, source_name,
            match_method, match_score, status
          ) VALUES (?, 'ccis', ?, ?, 'primary_source', 1.0, 'active')
        `, [masterId, ccisId, provider.providerName]);

        // Add the display name as an alias if different from canonical
        if (provider.providerName !== canonicalName) {
          await addProviderAlias(masterId, provider.providerName, 'variant', 'ccis', 1.0);
        }

        // Log the creation
        await logMatchAudit(
          masterId,
          'ccis',
          ccisId,
          provider.providerName,
          'created'
        );

        imported++;
      }

      // Also maintain the legacy providers table for backward compatibility
      await syncToLegacyProviders(provider, providerType);

    } catch (err) {
      console.error(`Error saving provider ${provider.providerName}:`, err);
    }
  }

  await saveDb();
  return { imported, updated };
}

/**
 * Sync to legacy providers table for backward compatibility
 */
async function syncToLegacyProviders(provider: CCISProvider, providerType: string | null): Promise<void> {
  try {
    const existing = await query(
      `SELECT id FROM providers WHERE name = ? OR (provider_id IS NOT NULL AND provider_id = ?)`,
      [provider.providerName, provider.providerId || '']
    );

    if (existing.length > 0) {
      await execute(`
        UPDATE providers SET
          provider_id = COALESCE(?, provider_id),
          address = COALESCE(?, address),
          city = COALESCE(?, city),
          state = COALESCE(?, state),
          zip = COALESCE(?, zip),
          phone = COALESCE(?, phone),
          email = COALESCE(?, email),
          provider_type = COALESCE(?, provider_type),
          capacity = COALESCE(?, capacity),
          accepts_ccdf = COALESCE(?, accepts_ccdf),
          updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `, [
        provider.providerId || null,
        provider.address || null,
        provider.city || null,
        provider.state || 'NH',
        provider.zip || null,
        provider.phone || null,
        provider.email || null,
        providerType,
        provider.capacity || null,
        provider.acceptsSubsidy ? 1 : null,
        existing[0].id,
      ]);
    } else {
      await execute(`
        INSERT INTO providers (
          provider_id, name, address, city, state, zip,
          phone, email, provider_type, capacity, accepts_ccdf
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        provider.providerId || null,
        provider.providerName,
        provider.address || null,
        provider.city || null,
        provider.state || 'NH',
        provider.zip || null,
        provider.phone || null,
        provider.email || null,
        providerType,
        provider.capacity || null,
        provider.acceptsSubsidy ? 1 : 0,
      ]);
    }
  } catch (err) {
    // Non-critical - legacy table sync failure shouldn't stop main process
    console.warn(`Warning: Failed to sync to legacy providers table: ${err}`);
  }
}

/**
 * Main scrape function
 */
export async function scrapeCCIS(): Promise<ScrapeResult> {
  const result: ScrapeResult = {
    success: false,
    providers: [],
    totalFound: 0,
    imported: 0,
    updated: 0,
  };

  let browser: Browser | null = null;

  try {
    console.log('Starting NH CCIS scraper...');

    browser = await launchBrowser();
    const page = await browser.newPage();

    // Set viewport and user agent
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );

    // Navigate and search
    await navigateAndSearch(page);

    // Download results
    const downloadPath = await downloadProviderResults(page);

    if (!downloadPath) {
      result.error = 'Failed to download provider results';
      return result;
    }

    result.downloadPath = downloadPath;

    // Parse CSV
    console.log('Parsing CSV...');
    const csvContent = readFileSync(downloadPath, 'utf-8');
    const providers = parseCSV(csvContent);

    result.providers = providers;
    result.totalFound = providers.length;
    console.log(`Parsed ${providers.length} providers`);

    // Save to database
    console.log('Saving to database...');
    const { imported, updated } = await saveProviders(providers);
    result.imported = imported;
    result.updated = updated;

    result.success = true;
    console.log(`CCIS scrape complete: ${imported} imported, ${updated} updated`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('CCIS scrape error:', error);
  } finally {
    if (browser) {
      await browser.close();
    }
  }

  return result;
}

/**
 * Scrape from a pre-downloaded CSV file
 */
export async function importFromCSV(csvPath: string): Promise<ScrapeResult> {
  const result: ScrapeResult = {
    success: false,
    providers: [],
    totalFound: 0,
    imported: 0,
    updated: 0,
  };

  try {
    if (!existsSync(csvPath)) {
      result.error = `File not found: ${csvPath}`;
      return result;
    }

    console.log(`Importing from ${csvPath}...`);
    const csvContent = readFileSync(csvPath, 'utf-8');
    const providers = parseCSV(csvContent);

    result.providers = providers;
    result.totalFound = providers.length;
    result.downloadPath = csvPath;

    console.log(`Parsed ${providers.length} providers`);

    const { imported, updated } = await saveProviders(providers);
    result.imported = imported;
    result.updated = updated;

    result.success = true;
    console.log(`Import complete: ${imported} imported, ${updated} updated`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('CSV import error:', error);
  }

  return result;
}

export default {
  scrapeCCIS,
  importFromCSV,
  parseCSV,
  saveProviders,
  CCIS_URL,
};
