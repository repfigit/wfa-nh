/**
 * USAspending.gov Scraper
 * Fetches federal CCDF (Child Care) grant data for New Hampshire
 * 
 * Data source: https://api.usaspending.gov
 * CFDA codes:
 *   - 93.575: Child Care and Development Block Grant
 *   - 93.596: Child Care Mandatory and Matching Funds
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

const API_BASE = 'https://api.usaspending.gov/api/v2';

// NH state FIPS code
const NH_FIPS = '33';
const NH_STATE_CODE = 'NH';

// CCDF-related CFDA program numbers
const CCDF_CFDA_CODES = ['93.575', '93.596'];

// Award type codes for grants
const GRANT_AWARD_TYPES = ['02', '03', '04', '05'];

interface USASpendingAward {
  'Award ID'?: string;
  generated_internal_id?: string;
  'Recipient Name'?: string;
  recipient_id?: string;
  'Award Amount'?: number;
  'Total Outlays'?: number;
  Description?: string;
  def_codes?: string[];
  'COVID-19 Obligations'?: number;
  'COVID-19 Outlays'?: number;
  'Infrastructure Obligations'?: number;
  'Infrastructure Outlays'?: number;
  'Award Type'?: string;
  'Start Date'?: string;
  'End Date'?: string;
  'Awarding Agency'?: string;
  'Awarding Sub Agency'?: string;
  'CFDA Number'?: string;
  cfda_program_title?: string;
  recipient_location_state_code?: string;
  pop_state_code?: string;
}

interface USASpendingResponse {
  results: USASpendingAward[];
  page_metadata: {
    page: number;
    hasNext: boolean;
    total: number;
  };
  messages?: string[];
}

interface ScrapeResult {
  success: boolean;
  source: string;
  totalAwards: number;
  totalAmount: number;
  importedRecords: number;
  fiscalYears: number[];
  error?: string;
  awards?: Array<{
    awardId: string;
    recipient: string;
    amount: number;
    cfda: string;
    fiscalYear: number;
    description: string;
  }>;
}

/**
 * Fetch NH CCDF awards from USAspending.gov API
 */
async function fetchNHCCDFAwards(fiscalYear?: number): Promise<USASpendingAward[]> {
  const allAwards: USASpendingAward[] = [];
  let page = 1;
  let hasNext = true;
  
  const filters: Record<string, unknown> = {
    recipient_locations: [
      { country: 'USA', state: NH_STATE_CODE }
    ],
    program_numbers: CCDF_CFDA_CODES,
    award_type_codes: GRANT_AWARD_TYPES,
  };
  
  if (fiscalYear) {
    filters.time_period = [
      { start_date: `${fiscalYear - 1}-10-01`, end_date: `${fiscalYear}-09-30` }
    ];
  }
  
  console.log('Fetching NH CCDF awards from USAspending.gov...');
  console.log('Filters:', JSON.stringify(filters, null, 2));
  
  while (hasNext && page <= 10) { // Max 10 pages to avoid runaway
    try {
      const response = await fetch(`${API_BASE}/search/spending_by_award/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify({
          filters,
          fields: [
            'Award ID',
            'generated_internal_id',
            'Recipient Name',
            'recipient_id',
            'Award Amount',
            'Total Outlays',
            'Description',
            'def_codes',
            'COVID-19 Obligations',
            'COVID-19 Outlays',
            'Infrastructure Obligations',
            'Infrastructure Outlays',
            'Award Type',
            'Start Date',
            'End Date',
            'Awarding Agency',
            'Awarding Sub Agency',
            'CFDA Number',
            'cfda_program_title',
            'recipient_location_state_code',
            'pop_state_code',
          ],
          page,
          limit: 100,
          sort: 'Award Amount',
          order: 'desc',
        }),
      });
      
      if (!response.ok) {
        throw new Error(`API error: ${response.status} ${response.statusText}`);
      }
      
      const data: USASpendingResponse = await response.json();
      
      console.log(`Page ${page}: ${data.results.length} awards, total: ${data.page_metadata.total}`);
      
      allAwards.push(...data.results);
      hasNext = data.page_metadata.hasNext;
      page++;
      
      // Rate limiting - small delay between requests
      await new Promise(resolve => setTimeout(resolve, 200));
      
    } catch (error) {
      console.error(`Error fetching page ${page}:`, error);
      break;
    }
  }
  
  return allAwards;
}

/**
 * Fetch NH state spending overview
 */
async function fetchNHStateOverview(): Promise<Record<string, unknown> | null> {
  try {
    const response = await fetch(`${API_BASE}/recipient/state/${NH_FIPS}/`, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    });
    
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error fetching state overview:', error);
    return null;
  }
}

/**
 * Fetch spending by CFDA for NH
 */
async function fetchNHSpendingByCFDA(fiscalYear?: number): Promise<Array<{cfda: string, name: string, amount: number}>> {
  try {
    const filters: Record<string, unknown> = {
      recipient_locations: [
        { country: 'USA', state: NH_STATE_CODE }
      ],
      program_numbers: CCDF_CFDA_CODES,
    };
    
    if (fiscalYear) {
      filters.time_period = [
        { start_date: `${fiscalYear - 1}-10-01`, end_date: `${fiscalYear}-09-30` }
      ];
    }
    
    const response = await fetch(`${API_BASE}/search/spending_by_category/cfda/`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify({
        filters,
        limit: 50,
      }),
    });
    
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    return (data.results || []).map((r: { code: string; name: string; amount: number }) => ({
      cfda: r.code,
      name: r.name,
      amount: r.amount,
    }));
  } catch (error) {
    console.error('Error fetching CFDA spending:', error);
    return [];
  }
}

/**
 * Parse fiscal year from award ID or date
 */
function parseFiscalYear(award: USASpendingAward): number {
  // Try to extract from award ID (e.g., "2401NHCCDD" -> 2024)
  const awardId = award['Award ID'] || '';
  const yearMatch = awardId.match(/^(\d{2})/);
  if (yearMatch) {
    const year = parseInt(yearMatch[1]);
    return year < 50 ? 2000 + year : 1900 + year;
  }
  
  // Try from start date
  if (award['Start Date']) {
    const date = new Date(award['Start Date']);
    // Federal fiscal year starts Oct 1, so Oct-Dec is next FY
    return date.getMonth() >= 9 ? date.getFullYear() + 1 : date.getFullYear();
  }
  
  return new Date().getFullYear();
}

/**
 * Save awards to database
 */
async function saveAwards(awards: USASpendingAward[]): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  for (const award of awards) {
    try {
      const awardId = award['Award ID'] || award.generated_internal_id || '';
      const fiscalYear = parseFiscalYear(award);
      
      // Check for duplicate
      const existing = await query(`
        SELECT id FROM expenditures 
        WHERE source_url LIKE ? AND fiscal_year = ?
        LIMIT 1
      `, [`%${awardId}%`, fiscalYear]);
      
      if (existing.length > 0) continue;
      
      // Insert as expenditure
      await execute(`
        INSERT INTO expenditures (
          fiscal_year, department, agency, activity,
          vendor_name, amount, payment_date, description,
          source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        fiscalYear,
        'Federal Grant',
        award['Awarding Agency'] || 'HHS',
        award.cfda_program_title || `CFDA ${award['CFDA Number'] || 'Unknown'}`,
        award['Recipient Name'] || 'Unknown',
        award['Award Amount'] || 0,
        award['Start Date'] || null,
        award.Description || `Federal CCDF Award ${awardId}`,
        `USAspending:${awardId}`,
      ]);
      
      savedCount++;
    } catch (error) {
      // Skip errors for individual records
      console.error('Error saving award:', error);
    }
  }
  
  await saveDb();
  return savedCount;
}

/**
 * Main scrape function
 */
export async function scrapeUSASpending(fiscalYear?: number): Promise<ScrapeResult> {
  const result: ScrapeResult = {
    success: false,
    source: 'USAspending.gov',
    totalAwards: 0,
    totalAmount: 0,
    importedRecords: 0,
    fiscalYears: [],
  };
  
  try {
    console.log(`\n=== USAspending.gov Scraper ===`);
    console.log(`Target: NH CCDF Awards ${fiscalYear ? `(FY${fiscalYear})` : '(All Years)'}`);
    
    // Fetch awards
    const awards = await fetchNHCCDFAwards(fiscalYear);
    result.totalAwards = awards.length;
    result.totalAmount = awards.reduce((sum, a) => sum + (a['Award Amount'] || 0), 0);
    
    // Get unique fiscal years
    const fiscalYearsSet = new Set(awards.map(a => parseFiscalYear(a)));
    result.fiscalYears = Array.from(fiscalYearsSet).sort((a, b) => b - a);
    
    console.log(`Found ${awards.length} awards totaling $${result.totalAmount.toLocaleString()}`);
    console.log(`Fiscal years: ${result.fiscalYears.join(', ')}`);
    
    // Save to database
    result.importedRecords = await saveAwards(awards);
    console.log(`Imported ${result.importedRecords} new records`);
    
    // Include award details in result
    result.awards = awards.map(a => ({
      awardId: a['Award ID'] || a.generated_internal_id || 'Unknown',
      recipient: a['Recipient Name'] || 'Unknown',
      amount: a['Award Amount'] || 0,
      cfda: a['CFDA Number'] || 'Unknown',
      fiscalYear: parseFiscalYear(a),
      description: a.Description || '',
    }));
    
    result.success = true;
    
  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('USAspending scrape error:', error);
  }
  
  return result;
}

/**
 * Get CFDA spending summary for NH
 */
export async function getCFDASpendingSummary(fiscalYear?: number): Promise<Array<{cfda: string, name: string, amount: number}>> {
  return fetchNHSpendingByCFDA(fiscalYear);
}

/**
 * Get NH state overview
 */
export async function getNHStateOverview(): Promise<Record<string, unknown> | null> {
  return fetchNHStateOverview();
}

export default {
  scrapeUSASpending,
  getCFDASpendingSummary,
  getNHStateOverview,
  CCDF_CFDA_CODES,
};
