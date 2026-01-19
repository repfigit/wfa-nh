/**
 * NH Child Care Licensing Scraper
 * Scrapes licensed childcare provider data from DHHS
 * 
 * Data source: https://www.dhhs.nh.gov/programs-services/childcare-parenting-childbirth/child-care-licensing
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';
import * as cheerio from 'cheerio';

const LICENSING_URL = 'https://www.dhhs.nh.gov/programs-services/childcare-parenting-childbirth/child-care-licensing';
const PROVIDER_SEARCH_URL = 'https://nhlicenses.nh.gov/verification/';

// Common immigrant-associated names/patterns for flagging
const IMMIGRANT_NAME_PATTERNS = [
  /somali/i, /arabic/i, /african/i, /refugee/i,
  /islamic/i, /muslim/i, /multicultural/i,
  /international/i, /global/i, /world/i,
];

interface LicensedProvider {
  name: string;
  licenseNumber?: string;
  licenseType?: string;
  licenseStatus?: string;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  phone?: string;
  capacity?: number;
  ageRange?: string;
  ownerName?: string;
}

interface ScrapeResult {
  success: boolean;
  providers: LicensedProvider[];
  totalFound: number;
  imported: number;
  error?: string;
}

/**
 * Fetch with retry and proper headers
 */
async function fetchWithRetry(url: string, retries = 3): Promise<string | null> {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
        },
      });
      
      if (response.ok) {
        return await response.text();
      }
      
      console.warn(`HTTP ${response.status} for ${url}`);
      
      if (response.status === 403) {
        console.warn('Access forbidden - site may be blocking scrapers');
        return null;
      }
    } catch (error) {
      console.warn(`Fetch attempt ${attempt + 1} failed:`, error);
    }
    
    // Wait before retry
    await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
  }
  
  return null;
}

/**
 * Check if provider name suggests immigrant ownership
 */
function checkImmigrantPatterns(name: string): boolean {
  return IMMIGRANT_NAME_PATTERNS.some(pattern => pattern.test(name));
}

/**
 * Parse provider listing from HTML
 */
function parseProviderListing(html: string): LicensedProvider[] {
  const $ = cheerio.load(html);
  const providers: LicensedProvider[] = [];
  
  // Try various table/list selectors that might contain provider data
  $('table tr, .provider-row, .listing-item').each((_, element) => {
    const $row = $(element);
    const text = $row.text();
    
    // Skip headers
    if (text.toLowerCase().includes('name') && text.toLowerCase().includes('license')) {
      return;
    }
    
    // Try to extract provider info from cells or text
    const cells = $row.find('td');
    if (cells.length >= 3) {
      const provider: LicensedProvider = {
        name: $(cells[0]).text().trim(),
        licenseNumber: $(cells[1]).text().trim() || undefined,
        licenseStatus: $(cells[2]).text().trim() || undefined,
      };
      
      if (cells.length >= 4) {
        provider.address = $(cells[3]).text().trim();
      }
      if (cells.length >= 5) {
        provider.city = $(cells[4]).text().trim();
      }
      
      if (provider.name && provider.name.length > 2) {
        providers.push(provider);
      }
    }
  });
  
  return providers;
}

/**
 * Save providers to database
 */
async function saveProviders(providers: LicensedProvider[]): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  for (const provider of providers) {
    try {
      // Check for existing provider by name
      const existing = await query(
        'SELECT id FROM providers WHERE name = ? OR license_number = ?',
        [provider.name, provider.licenseNumber || '']
      );
      
      if (existing.length > 0) {
        // Update existing
        await execute(`
          UPDATE providers SET
            license_number = COALESCE(?, license_number),
            license_type = COALESCE(?, license_type),
            license_status = COALESCE(?, license_status),
            address = COALESCE(?, address),
            city = COALESCE(?, city),
            capacity = COALESCE(?, capacity),
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `, [
          provider.licenseNumber,
          provider.licenseType,
          provider.licenseStatus,
          provider.address,
          provider.city,
          provider.capacity,
          existing[0].id,
        ]);
      } else {
        // Insert new
        const isImmigrant = checkImmigrantPatterns(provider.name) ? 1 : 0;
        
        await execute(`
          INSERT INTO providers (
            name, license_number, license_type, license_status,
            address, city, state, zip, phone, capacity,
            is_immigrant_owned, accepts_ccdf, notes
          ) VALUES (?, ?, ?, ?, ?, ?, 'NH', ?, ?, ?, ?, 1, 'Imported from NH Licensing')
        `, [
          provider.name,
          provider.licenseNumber || null,
          provider.licenseType || null,
          provider.licenseStatus || null,
          provider.address || null,
          provider.city || null,
          provider.zip || null,
          provider.phone || null,
          provider.capacity || null,
          isImmigrant,
        ]);
        
        savedCount++;
      }
    } catch (error) {
      console.error(`Error saving provider ${provider.name}:`, error);
    }
  }
  
  await saveDb();
  return savedCount;
}

/**
 * Scrape NH Child Care Licensing data
 */
export async function scrapeLicensing(): Promise<ScrapeResult> {
  const result: ScrapeResult = {
    success: false,
    providers: [],
    totalFound: 0,
    imported: 0,
  };
  
  try {
    console.log('Scraping NH Child Care Licensing...');
    
    // Try the main licensing page
    const html = await fetchWithRetry(LICENSING_URL);
    
    if (!html) {
      result.error = 'Failed to fetch licensing page (may be blocked)';
      return result;
    }
    
    // Parse for any provider data or links
    const $ = cheerio.load(html);
    
    // Look for links to provider databases or search pages
    const providerLinks: string[] = [];
    $('a').each((_, el) => {
      const href = $(el).attr('href') || '';
      const text = $(el).text().toLowerCase();
      
      if (text.includes('provider') || text.includes('search') || 
          text.includes('database') || text.includes('licensed')) {
        if (href.startsWith('http')) {
          providerLinks.push(href);
        } else if (href.startsWith('/')) {
          providerLinks.push(`https://www.dhhs.nh.gov${href}`);
        }
      }
    });
    
    console.log(`Found ${providerLinks.length} potential provider links`);
    
    // Try each link
    for (const link of providerLinks.slice(0, 5)) {
      console.log(`Trying: ${link}`);
      const pageHtml = await fetchWithRetry(link);
      
      if (pageHtml) {
        const providers = parseProviderListing(pageHtml);
        result.providers.push(...providers);
      }
      
      // Rate limit
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    result.totalFound = result.providers.length;
    
    if (result.providers.length > 0) {
      result.imported = await saveProviders(result.providers);
    }
    
    result.success = true;
    console.log(`Scraped ${result.totalFound} providers, imported ${result.imported}`);
    
  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('Licensing scrape error:', error);
  }
  
  return result;
}

export default {
  scrapeLicensing,
};
