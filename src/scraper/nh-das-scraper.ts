/**
 * NH DAS Contracts Scraper
 * Scrapes contracts from https://apps.das.nh.gov/bidscontracts/contracts.aspx
 * Handles ASP.NET ViewState for form submissions
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

const BASE_URL = 'https://apps.das.nh.gov/bidscontracts/contracts.aspx';
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Content-Type': 'application/x-www-form-urlencoded',
};

// Keywords to search for childcare/daycare contracts
export const CHILDCARE_KEYWORDS = [
  'daycare',
  'child care',
  'childcare', 
  'early learning',
  'preschool',
  'nursery',
  'head start',
  'after school',
  'youth center',
  'kindergarten',
  'CCDF',
  'child development',
];

// Keywords that may indicate immigrant-related services
export const IMMIGRANT_KEYWORDS = [
  'immigrant',
  'refugee',
  'asylum',
  'resettlement',
  'translation',
  'interpreter',
  'multicultural',
  'ESL',
  'newcomer',
];

interface ScrapedContract {
  contract_num: string;
  description: string;
  vendor: string;
  effective_date: string;
  expiration_date: string;
  agency: string;
  pdf_url: string;
  amount?: number;
}

interface ScrapeResult {
  success: boolean;
  contracts: ScrapedContract[];
  error?: string;
}

/**
 * Sleep helper for rate limiting
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Retry HTTP request with exponential backoff
 */
async function retryFetch(
  url: string, 
  options: RequestInit, 
  retries = 3, 
  delay = 1000
): Promise<Response> {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) {
        return response;
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    } catch (error) {
      console.warn(`Request failed (attempt ${attempt + 1}/${retries}):`, error);
      if (attempt < retries - 1) {
        await sleep(delay * Math.pow(2, attempt));
      } else {
        throw error;
      }
    }
  }
  throw new Error('All retries failed');
}

/**
 * Extract ASP.NET ViewState and EventValidation from HTML
 */
function extractViewState(html: string): { viewState: string; eventValidation: string } | null {
  const viewStateMatch = html.match(/id="__VIEWSTATE"\s+value="([^"]+)"/);
  const eventValidationMatch = html.match(/id="__EVENTVALIDATION"\s+value="([^"]+)"/);
  
  if (!viewStateMatch || !eventValidationMatch) {
    return null;
  }
  
  return {
    viewState: viewStateMatch[1],
    eventValidation: eventValidationMatch[1],
  };
}

/**
 * Parse contracts table from HTML response
 */
function parseContractsTable(html: string): ScrapedContract[] {
  const contracts: ScrapedContract[] = [];
  
  // Look for the results table - ASP.NET GridView typically has an id
  // Pattern: <table class="..." id="...GridView...">
  const tableMatch = html.match(/<table[^>]*class="[^"]*"[^>]*>([\s\S]*?)<\/table>/gi);
  
  if (!tableMatch) {
    return contracts;
  }
  
  // Find the data table (usually has rows with contract data)
  for (const table of tableMatch) {
    const rows = table.match(/<tr[^>]*>([\s\S]*?)<\/tr>/gi);
    if (!rows || rows.length < 2) continue;
    
    // Skip header row
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const cells = row.match(/<td[^>]*>([\s\S]*?)<\/td>/gi);
      
      if (cells && cells.length >= 6) {
        // Extract text content from cells
        const getText = (cell: string) => cell.replace(/<[^>]+>/g, '').trim();
        const getLink = (cell: string) => {
          const match = cell.match(/href="([^"]+)"/);
          return match ? match[1] : '';
        };
        
        const contract: ScrapedContract = {
          contract_num: getText(cells[0]),
          description: getText(cells[1]),
          vendor: getText(cells[2]),
          effective_date: getText(cells[3]),
          expiration_date: getText(cells[4]),
          agency: getText(cells[5]),
          pdf_url: getLink(cells[0]),
        };
        
        // Try to extract amount if present
        if (cells.length >= 7) {
          const amountText = getText(cells[6]).replace(/[$,]/g, '');
          const amount = parseFloat(amountText);
          if (!isNaN(amount)) {
            contract.amount = amount;
          }
        }
        
        if (contract.contract_num) {
          contracts.push(contract);
        }
      }
    }
  }
  
  return contracts;
}

/**
 * Search for contracts using a keyword
 */
export async function searchContracts(keyword: string): Promise<ScrapeResult> {
  try {
    console.log(`Searching NH DAS contracts for: "${keyword}"`);
    
    // Step 1: Get the initial page to extract ViewState
    const initialResponse = await retryFetch(BASE_URL, {
      method: 'GET',
      headers: HEADERS,
    });
    
    const initialHtml = await initialResponse.text();
    const viewState = extractViewState(initialHtml);
    
    if (!viewState) {
      return {
        success: false,
        contracts: [],
        error: 'Failed to extract ViewState from page',
      };
    }
    
    // Step 2: Submit search form
    const formData = new URLSearchParams();
    formData.append('__VIEWSTATE', viewState.viewState);
    formData.append('__EVENTVALIDATION', viewState.eventValidation);
    formData.append('ctl00$ContentPlaceHolder1$txtContractDescription', keyword);
    formData.append('ctl00$ContentPlaceHolder1$btnSearch', 'Search');
    
    // Rate limit
    await sleep(500);
    
    const searchResponse = await retryFetch(BASE_URL, {
      method: 'POST',
      headers: HEADERS,
      body: formData.toString(),
    });
    
    const searchHtml = await searchResponse.text();
    const contracts = parseContractsTable(searchHtml);
    
    console.log(`Found ${contracts.length} contracts for "${keyword}"`);
    
    return {
      success: true,
      contracts,
    };
  } catch (error) {
    console.error('Scraping error:', error);
    return {
      success: false,
      contracts: [],
      error: error instanceof Error ? error.message : 'Unknown error',
    };
  }
}

/**
 * Check if a contract matches fraud indicators
 */
export function checkContractFraudIndicators(contract: ScrapedContract): string[] {
  const reasons: string[] = [];
  const desc = (contract.description || '').toLowerCase();
  const vendor = (contract.vendor || '').toLowerCase();
  const searchText = `${desc} ${vendor}`;
  
  // Check for childcare keywords
  for (const keyword of CHILDCARE_KEYWORDS) {
    if (searchText.includes(keyword.toLowerCase())) {
      reasons.push(`Childcare-related: contains "${keyword}"`);
      break;
    }
  }
  
  // Check for immigrant-related keywords
  for (const keyword of IMMIGRANT_KEYWORDS) {
    if (searchText.includes(keyword.toLowerCase())) {
      reasons.push(`Immigrant-related: contains "${keyword}"`);
      break;
    }
  }
  
  // Check for high-value contracts
  if (contract.amount && contract.amount > 100000) {
    reasons.push(`High value contract: $${contract.amount.toLocaleString()}`);
  }
  
  return reasons;
}

/**
 * Save scraped contracts to the database
 */
export async function saveScrapedContracts(contracts: ScrapedContract[]): Promise<number> {
  await initializeDb();
  let savedCount = 0;
  
  for (const contract of contracts) {
    try {
      // Check if contract already exists
      const existing = await query('SELECT id FROM contracts WHERE contract_number = ?', [contract.contract_num]);
      if (existing.length > 0) {
        continue; // Skip duplicates
      }
      
      // Try to find or create a contractor/provider
      let contractorId: number | null = null;
      
      if (contract.vendor) {
        // Check if vendor exists
        const vendors = await query('SELECT id FROM contractors WHERE name = ?', [contract.vendor]);
        if (vendors.length > 0) {
          contractorId = vendors[0].id as number;
        }
        
        // Create if not exists
        if (!contractorId) {
          const result = await execute(`
            INSERT INTO contractors (name, is_immigrant_related, notes)
            VALUES (?, 0, 'Added from NH DAS scraper')
          `, [contract.vendor]);
          contractorId = result.lastId || null;
        }
      }
      
      // Insert the contract
      const contractResult = await execute(`
        INSERT INTO contracts (
          contractor_id, contract_number, title, description, department,
          start_date, end_date, original_amount, current_amount,
          funding_source, source_url, source_document
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'State', ?, 'NH DAS Scraper')
      `, [
        contractorId,
        contract.contract_num,
        contract.description.substring(0, 200),
        contract.description,
        contract.agency,
        contract.effective_date,
        contract.expiration_date,
        contract.amount || null,
        contract.amount || null,
        contract.pdf_url ? `https://apps.das.nh.gov${contract.pdf_url}` : null,
      ]);
      
      savedCount++;
      const newContractId = contractResult.lastId;
      
      // Check for fraud indicators
      const fraudReasons = checkContractFraudIndicators(contract);
      for (const reason of fraudReasons) {
        await execute(`
          INSERT INTO fraud_indicators (
            contract_id, contractor_id, indicator_type, severity, 
            description, evidence, status
          ) VALUES (?, ?, 'keyword_match', 'low', ?, ?, 'open')
        `, [
          newContractId,
          contractorId,
          reason,
          `Contract: ${contract.contract_num}, Vendor: ${contract.vendor}`,
        ]);
      }
    } catch (error) {
      console.error(`Error saving contract ${contract.contract_num}:`, error);
    }
  }
  
  await saveDb();
  return savedCount;
}

/**
 * Run a full scrape for all childcare-related keywords
 */
export async function scrapeAllChildcareContracts(): Promise<{
  total: number;
  saved: number;
  errors: string[];
}> {
  const allContracts: ScrapedContract[] = [];
  const errors: string[] = [];
  const seenContractNums = new Set<string>();
  
  for (const keyword of CHILDCARE_KEYWORDS) {
    const result = await searchContracts(keyword);
    
    if (result.success) {
      for (const contract of result.contracts) {
        if (!seenContractNums.has(contract.contract_num)) {
          seenContractNums.add(contract.contract_num);
          allContracts.push(contract);
        }
      }
    } else if (result.error) {
      errors.push(`${keyword}: ${result.error}`);
    }
    
    // Rate limit between searches
    await sleep(1000);
  }
  
  const saved = await saveScrapedContracts(allContracts);
  
  return {
    total: allContracts.length,
    saved,
    errors,
  };
}

export default {
  searchContracts,
  saveScrapedContracts,
  scrapeAllChildcareContracts,
  checkContractFraudIndicators,
  CHILDCARE_KEYWORDS,
  IMMIGRANT_KEYWORDS,
};
