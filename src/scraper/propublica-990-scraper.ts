/**
 * ProPublica Nonprofit Explorer Scraper
 * Fetches IRS 990 filing data for NH childcare nonprofits
 *
 * Data source: https://projects.propublica.org/nonprofits/api
 * Provides: Revenue, expenses, executive compensation, assets for nonprofits
 *
 * NTEE Codes for childcare:
 *   - P33: Child Day Care
 *   - P30: Children & Youth Services (broader category)
 *   - B20: Elementary/Secondary Education (includes some childcare)
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

const API_BASE = 'https://projects.propublica.org/nonprofits/api/v2';

// NTEE codes related to childcare
const CHILDCARE_NTEE_CODES = ['P33', 'P30', 'P31', 'P32'];

// Keywords to search for childcare organizations
const CHILDCARE_SEARCH_TERMS = [
  'child care',
  'childcare',
  'daycare',
  'day care',
  'early learning',
  'early childhood',
  'head start',
  'preschool',
  'nursery school',
];

interface ProPublicaOrg {
  ein: number;
  name: string;
  city: string;
  state: string;
  ntee_code: string;
  score?: number;
}

interface ProPublicaFiling {
  tax_prd: number;
  tax_prd_yr: number;
  totrevenue: number;
  totfuncexpns: number;
  totassetsend: number;
  totliabend: number;
  pct_compnsatncurrofcr: number;
  pdf_url?: string;
}

interface ProPublicaOrgDetail {
  organization: {
    ein: number;
    name: string;
    city: string;
    state: string;
    zipcode: string;
    ntee_code: string;
    subseccd: number;
    classification_codes: string;
    ruling_date: string;
    asset_cd: number;
    income_cd: number;
    filing_req_cd: number;
  };
  filings_with_data: ProPublicaFiling[];
  filings_without_data: Array<{ tax_prd: number; pdf_url: string }>;
}

interface ScrapeResult {
  success: boolean;
  source: string;
  organizationsFound: number;
  filingsProcessed: number;
  filingsImported: number;
  totalRevenue: number;
  organizations: Array<{
    ein: string;
    name: string;
    city: string;
    nteeCode: string;
    latestRevenue: number;
    latestExpenses: number;
    latestAssets: number;
  }>;
  error?: string;
}

/**
 * Search for NH childcare organizations
 */
async function searchNHChildcareOrgs(searchTerm: string): Promise<ProPublicaOrg[]> {
  try {
    const url = `${API_BASE}/search.json?q=${encodeURIComponent(searchTerm)}&state[id]=NH`;
    console.log(`Searching: ${url}`);

    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'NH-Childcare-Tracker/1.0',
      },
    });

    if (!response.ok) {
      console.error(`Search failed: ${response.status} ${response.statusText}`);
      return [];
    }

    const data = await response.json();
    return data.organizations || [];
  } catch (error) {
    console.error(`Search error for "${searchTerm}":`, error);
    return [];
  }
}

/**
 * Get detailed organization info including filings
 */
async function getOrgDetail(ein: number): Promise<ProPublicaOrgDetail | null> {
  try {
    // Format EIN with leading zeros
    const einStr = ein.toString().padStart(9, '0');
    const url = `${API_BASE}/organizations/${einStr}.json`;

    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'NH-Childcare-Tracker/1.0',
      },
    });

    if (!response.ok) {
      if (response.status === 404) {
        console.log(`Organization not found: ${einStr}`);
        return null;
      }
      console.error(`Detail fetch failed: ${response.status}`);
      return null;
    }

    return await response.json();
  } catch (error) {
    console.error(`Detail fetch error for EIN ${ein}:`, error);
    return null;
  }
}

/**
 * Save nonprofit filing to database
 */
async function saveFiling(
  org: ProPublicaOrgDetail['organization'],
  filing: ProPublicaFiling
): Promise<boolean> {
  try {
    const einStr = org.ein.toString().padStart(9, '0');
    const fiscalYear = filing.tax_prd_yr;

    // Check for duplicate
    const existing = await query(`
      SELECT id FROM nonprofit_filings
      WHERE ein = ? AND fiscal_year = ?
      LIMIT 1
    `, [einStr, fiscalYear]);

    if (existing.length > 0) {
      return false; // Already exists
    }

    // Determine NTEE code - try to categorize as childcare
    const nteeCode = org.ntee_code || 'Unknown';

    await execute(`
      INSERT INTO nonprofit_filings (
        ein, name, city, state, zip, ntee_code,
        fiscal_year, total_revenue, total_expenses, total_assets,
        total_liabilities, executive_compensation,
        source, source_url, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    `, [
      einStr,
      org.name,
      org.city,
      org.state,
      org.zipcode || null,
      nteeCode,
      fiscalYear,
      filing.totrevenue || 0,
      filing.totfuncexpns || 0,
      filing.totassetsend || 0,
      filing.totliabend || 0,
      filing.pct_compnsatncurrofcr || 0,
      'propublica',
      filing.pdf_url || null,
    ]);

    return true;
  } catch (error) {
    console.error('Error saving filing:', error);
    return false;
  }
}

/**
 * Create nonprofit_filings table if not exists
 */
async function ensureTable(): Promise<void> {
  await execute(`
    CREATE TABLE IF NOT EXISTS nonprofit_filings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ein TEXT NOT NULL,
      name TEXT NOT NULL,
      city TEXT,
      state TEXT,
      zip TEXT,
      ntee_code TEXT,
      fiscal_year INTEGER NOT NULL,
      total_revenue REAL,
      total_expenses REAL,
      total_assets REAL,
      total_liabilities REAL,
      executive_compensation REAL,
      program_service_revenue REAL,
      source TEXT DEFAULT 'propublica',
      source_url TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(ein, fiscal_year)
    )
  `, []);

  await execute(`CREATE INDEX IF NOT EXISTS idx_nonprofit_ein ON nonprofit_filings(ein)`, []);
  await execute(`CREATE INDEX IF NOT EXISTS idx_nonprofit_year ON nonprofit_filings(fiscal_year)`, []);
  await execute(`CREATE INDEX IF NOT EXISTS idx_nonprofit_ntee ON nonprofit_filings(ntee_code)`, []);
}

/**
 * Main scrape function - search and collect NH childcare nonprofit data
 */
export async function scrapeProPublica990(
  options: { maxOrgs?: number; recentYearsOnly?: boolean } = {}
): Promise<ScrapeResult> {
  const { maxOrgs = 100, recentYearsOnly = true } = options;

  const result: ScrapeResult = {
    success: false,
    source: 'ProPublica Nonprofit Explorer',
    organizationsFound: 0,
    filingsProcessed: 0,
    filingsImported: 0,
    totalRevenue: 0,
    organizations: [],
  };

  try {
    console.log('\n=== ProPublica 990 Scraper ===');
    console.log(`Max organizations: ${maxOrgs}, Recent years only: ${recentYearsOnly}`);

    await initializeDb();
    await ensureTable();

    // Collect unique organizations from all search terms
    const orgMap = new Map<number, ProPublicaOrg>();

    for (const term of CHILDCARE_SEARCH_TERMS) {
      console.log(`\nSearching for "${term}"...`);
      const orgs = await searchNHChildcareOrgs(term);
      console.log(`  Found ${orgs.length} organizations`);

      for (const org of orgs) {
        if (!orgMap.has(org.ein)) {
          orgMap.set(org.ein, org);
        }
      }

      // Rate limit - wait between searches
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    const allOrgs = Array.from(orgMap.values()).slice(0, maxOrgs);
    result.organizationsFound = allOrgs.length;
    console.log(`\nTotal unique organizations: ${allOrgs.length}`);

    // Fetch details and filings for each org
    for (const org of allOrgs) {
      console.log(`\nFetching details for: ${org.name} (EIN: ${org.ein})`);

      const detail = await getOrgDetail(org.ein);
      if (!detail) {
        continue;
      }

      const filings = detail.filings_with_data || [];
      console.log(`  Found ${filings.length} filings with data`);

      // Filter to recent years if requested
      const currentYear = new Date().getFullYear();
      const filingsToProcess = recentYearsOnly
        ? filings.filter(f => f.tax_prd_yr >= currentYear - 5)
        : filings;

      let latestFiling: ProPublicaFiling | null = null;

      for (const filing of filingsToProcess) {
        result.filingsProcessed++;

        const saved = await saveFiling(detail.organization, filing);
        if (saved) {
          result.filingsImported++;
          result.totalRevenue += filing.totrevenue || 0;
        }

        if (!latestFiling || filing.tax_prd_yr > latestFiling.tax_prd_yr) {
          latestFiling = filing;
        }
      }

      // Add to result organizations
      if (latestFiling) {
        result.organizations.push({
          ein: org.ein.toString().padStart(9, '0'),
          name: org.name,
          city: org.city,
          nteeCode: org.ntee_code || 'Unknown',
          latestRevenue: latestFiling.totrevenue || 0,
          latestExpenses: latestFiling.totfuncexpns || 0,
          latestAssets: latestFiling.totassetsend || 0,
        });
      }

      // Rate limit between organizations
      await new Promise(resolve => setTimeout(resolve, 300));
    }

    await saveDb();
    result.success = true;

    console.log('\n=== Scrape Complete ===');
    console.log(`Organizations: ${result.organizationsFound}`);
    console.log(`Filings processed: ${result.filingsProcessed}`);
    console.log(`Filings imported: ${result.filingsImported}`);
    console.log(`Total revenue: $${result.totalRevenue.toLocaleString()}`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('ProPublica scrape error:', error);
  }

  return result;
}

/**
 * Get summary statistics for NH childcare nonprofits
 */
export async function getNH990Summary(): Promise<{
  totalOrgs: number;
  totalRevenue: number;
  totalAssets: number;
  avgRevenue: number;
  byYear: Array<{ year: number; count: number; totalRevenue: number }>;
}> {
  await initializeDb();
  await ensureTable();

  const stats = await query(`
    SELECT
      COUNT(DISTINCT ein) as total_orgs,
      SUM(total_revenue) as total_revenue,
      SUM(total_assets) as total_assets,
      AVG(total_revenue) as avg_revenue
    FROM nonprofit_filings
    WHERE state = 'NH'
  `);

  const byYear = await query(`
    SELECT
      fiscal_year as year,
      COUNT(*) as count,
      SUM(total_revenue) as total_revenue
    FROM nonprofit_filings
    WHERE state = 'NH'
    GROUP BY fiscal_year
    ORDER BY fiscal_year DESC
  `);

  return {
    totalOrgs: stats[0]?.total_orgs || 0,
    totalRevenue: stats[0]?.total_revenue || 0,
    totalAssets: stats[0]?.total_assets || 0,
    avgRevenue: stats[0]?.avg_revenue || 0,
    byYear: byYear.map((r: Record<string, unknown>) => ({
      year: r.year as number,
      count: r.count as number,
      totalRevenue: r.total_revenue as number,
    })),
  };
}

export default {
  scrapeProPublica990,
  getNH990Summary,
  CHILDCARE_SEARCH_TERMS,
  CHILDCARE_NTEE_CODES,
};
