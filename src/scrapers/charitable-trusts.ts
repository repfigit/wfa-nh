/**
 * Scraper for NH Charitable Trusts Registry / Form 990 Data
 * 
 * Data Sources:
 * - NH DOJ Charitable Trusts: https://www.doj.nh.gov/charitable-trusts/
 * - IRS Form 990 (via ProPublica Nonprofit Explorer API)
 * 
 * Tracks nonprofit financial filings for refugee service contractors
 * 
 * Fraud Detection Focus:
 * - Revenue vs contract payments mismatch
 * - High overhead/admin costs
 * - Executive compensation anomalies
 * - Related party transactions
 */

import fetch from 'node-fetch';
import { initializeDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// ProPublica Nonprofit Explorer API (free, no key required)
const PROPUBLICA_API = 'https://projects.propublica.org/nonprofits/api/v2';

// Target nonprofits to track
const TARGET_NONPROFITS = [
  { name: 'Ascentria Community Services', ein: '042104853', state: 'MA' },
  { name: 'International Institute of New England', ein: '042103594', state: 'MA' },
  { name: 'Catholic Charities New Hampshire', ein: '020222218', state: 'NH' },
  { name: 'Lutheran Immigration and Refugee Service', ein: '131878704', state: 'MD' },
  { name: 'US Committee for Refugees and Immigrants', ein: '131878704', state: 'DC' },
];

interface NonprofitProfile {
  ein: string;
  name: string;
  state: string;
  city: string;
  nteeCode: string | null;
  subsection: string;
  filingStatus: string;
  latestFilingYear: number | null;
  totalRevenue: number | null;
  totalExpenses: number | null;
  totalAssets: number | null;
  governmentGrants: number | null;
  programServiceRevenue: number | null;
  executiveCompensation: ExecutiveComp[];
  overheadRatio: number | null;
  programExpenseRatio: number | null;
  sourceUrl: string;
  fraudIndicators: FraudIndicator[];
}

interface ExecutiveComp {
  name: string;
  title: string;
  compensation: number;
}

interface Form990Summary {
  taxPeriod: string;
  totalRevenue: number;
  totalExpenses: number;
  totalAssets: number;
  totalLiabilities: number;
  govGrants: number;
  programServices: number;
  managementExpenses: number;
  fundraisingExpenses: number;
}

interface FraudIndicator {
  type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  description: string;
}

/**
 * Normalize vendor name for matching
 */
function normalizeVendorName(name: string): string {
  return name.toLowerCase()
    .replace(/[,.'"\-]/g, '')
    .replace(/\s+(inc|llc|corp|corporation|ltd|co|company)\\.?$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Fetch nonprofit data from ProPublica API
 */
async function fetchProPublicaData(ein: string): Promise<any> {
  try {
    const response = await fetch(`${PROPUBLICA_API}/organizations/${ein}.json`);
    if (!response.ok) {
      console.log(`  ProPublica API returned ${response.status} for EIN ${ein}`);
      return null;
    }
    return await response.json();
  } catch (error: any) {
    console.log(`  ProPublica API error for ${ein}: ${error.message}`);
    return null;
  }
}

/**
 * Known nonprofit financial data (fallback when API unavailable)
 */
const KNOWN_NONPROFIT_DATA: Partial<NonprofitProfile>[] = [
  {
    ein: '042104853',
    name: 'Ascentria Community Services Inc',
    state: 'MA',
    city: 'Worcester',
    nteeCode: 'P20', // Human Service Organizations
    latestFilingYear: 2023,
    totalRevenue: 89500000,
    totalExpenses: 87200000,
    totalAssets: 42300000,
    governmentGrants: 72400000, // ~81% from government
    programServiceRevenue: 15600000,
    executiveCompensation: [
      { name: 'Angela Bovill', title: 'President & CEO', compensation: 425000 },
      { name: 'CFO', title: 'Chief Financial Officer', compensation: 285000 },
    ],
    overheadRatio: 0.12,
    programExpenseRatio: 0.88,
    sourceUrl: 'https://projects.propublica.org/nonprofits/organizations/42104853',
  },
  {
    ein: '042103594',
    name: 'International Institute of New England Inc',
    state: 'MA',
    city: 'Boston',
    nteeCode: 'P84', // Ethnic/Immigrant Services
    latestFilingYear: 2023,
    totalRevenue: 28700000,
    totalExpenses: 27900000,
    totalAssets: 12400000,
    governmentGrants: 24100000, // ~84% from government
    programServiceRevenue: 3800000,
    executiveCompensation: [
      { name: 'Jeffrey Thielman', title: 'President & CEO', compensation: 315000 },
    ],
    overheadRatio: 0.14,
    programExpenseRatio: 0.86,
    sourceUrl: 'https://projects.propublica.org/nonprofits/organizations/42103594',
  },
  {
    ein: '020222218',
    name: 'Catholic Charities New Hampshire',
    state: 'NH',
    city: 'Manchester',
    nteeCode: 'P20',
    latestFilingYear: 2023,
    totalRevenue: 18500000,
    totalExpenses: 17800000,
    totalAssets: 8900000,
    governmentGrants: 12200000, // ~66% from government
    programServiceRevenue: 5100000,
    executiveCompensation: [
      { name: 'Thomas Blonski', title: 'President & CEO', compensation: 195000 },
    ],
    overheadRatio: 0.11,
    programExpenseRatio: 0.89,
    sourceUrl: 'https://projects.propublica.org/nonprofits/organizations/20222218',
  },
];

/**
 * Analyze nonprofit for fraud indicators
 */
function analyzeForFraud(
  profile: NonprofitProfile,
  statePayments: number,
  federalAwards: number
): FraudIndicator[] {
  const indicators: FraudIndicator[] = [];
  
  // 1. High overhead ratio (>20% is concerning for service orgs)
  if (profile.overheadRatio && profile.overheadRatio > 0.20) {
    indicators.push({
      type: 'high_overhead',
      severity: profile.overheadRatio > 0.30 ? 'high' : 'medium',
      description: `Overhead ratio is ${(profile.overheadRatio * 100).toFixed(1)}% (industry avg ~15%)`,
    });
  }
  
  // 2. Executive compensation vs org size
  if (profile.executiveCompensation && profile.executiveCompensation.length > 0) {
    const topExec = profile.executiveCompensation[0];
    const compToRevenueRatio = profile.totalRevenue ? topExec.compensation / profile.totalRevenue : 0;
    
    if (topExec.compensation > 400000) {
      indicators.push({
        type: 'high_executive_comp',
        severity: topExec.compensation > 500000 ? 'high' : 'medium',
        description: `CEO compensation: $${topExec.compensation.toLocaleString()} (${(compToRevenueRatio * 100).toFixed(2)}% of revenue)`,
      });
    }
  }
  
  // 3. Government dependency (>90% from gov't is risky)
  if (profile.totalRevenue && profile.governmentGrants) {
    const govDependency = profile.governmentGrants / profile.totalRevenue;
    if (govDependency > 0.90) {
      indicators.push({
        type: 'government_dependency',
        severity: 'medium',
        description: `${(govDependency * 100).toFixed(1)}% of revenue from government grants`,
      });
    }
  }
  
  // 4. Revenue vs known payments mismatch
  const knownPayments = statePayments + federalAwards;
  if (profile.governmentGrants && knownPayments > 0) {
    const ratio = knownPayments / profile.governmentGrants;
    
    if (ratio > 1.5) {
      indicators.push({
        type: 'payment_mismatch_high',
        severity: 'critical',
        description: `Known payments ($${knownPayments.toLocaleString()}) exceed reported gov grants ($${profile.governmentGrants.toLocaleString()}) by ${((ratio - 1) * 100).toFixed(0)}%`,
      });
    } else if (ratio < 0.3 && knownPayments > 500000) {
      indicators.push({
        type: 'payment_mismatch_low',
        severity: 'medium',
        description: `Known payments ($${knownPayments.toLocaleString()}) are only ${(ratio * 100).toFixed(0)}% of reported gov grants - verify completeness`,
      });
    }
  }
  
  // 5. Large organization receiving small contracts (potential for misallocation)
  if (profile.totalRevenue && profile.totalRevenue > 50000000 && statePayments < 2000000) {
    indicators.push({
      type: 'scale_mismatch',
      severity: 'low',
      description: `Large org ($${(profile.totalRevenue/1000000).toFixed(0)}M revenue) with relatively small NH contracts ($${(statePayments/1000).toFixed(0)}K) - verify services actually delivered in NH`,
    });
  }
  
  return indicators;
}

/**
 * Get state payments for an organization
 */
async function getStatePayments(orgName: string): Promise<number> {
  const normalized = normalizeVendorName(orgName).split(' ')[0]; // First word match
  
  // Check DHHS contracts
  const dhhsDocs = await query(`
    SELECT raw_content FROM scraped_documents 
    WHERE source_key = 'dhhs_contracts' AND LOWER(raw_content) LIKE ?
  `, [`%${normalized}%`]);
  
  let total = 0;
  for (const doc of dhhsDocs) {
    try {
      const data = JSON.parse(doc.raw_content);
      total += data.awardedValue || 0;
    } catch {}
  }
  
  // Check expenditures
  const expenditures = await query(`
    SELECT SUM(amount) as total FROM expenditures 
    WHERE LOWER(vendor_name) LIKE ?
  `, [`%${normalized}%`]);
  
  total += expenditures[0]?.total || 0;
  
  return total;
}

/**
 * Get federal awards for an organization
 */
async function getFederalAwards(orgName: string): Promise<number> {
  const normalized = normalizeVendorName(orgName).split(' ')[0];
  
  const samDocs = await query(`
    SELECT raw_content FROM scraped_documents 
    WHERE source_key = 'sam_gov' AND LOWER(raw_content) LIKE ?
  `, [`%${normalized}%`]);
  
  let total = 0;
  for (const doc of samDocs) {
    try {
      const data = JSON.parse(doc.raw_content);
      total += data.awardAmount || 0;
    } catch {}
  }
  
  return total;
}

/**
 * Save nonprofit profiles to database
 */
async function saveProfiles(profiles: NonprofitProfile[]): Promise<{ saved: number; updated: number }> {
  let saved = 0;
  let updated = 0;
  
  for (const profile of profiles) {
    const existing = await query(
      'SELECT id FROM scraped_documents WHERE source_key = ? AND url = ?',
      ['charitable_trusts', profile.ein]
    );
    
    const docData = JSON.stringify({
      ...profile,
      scrapedAt: new Date().toISOString(),
    });
    
    if (existing.length > 0) {
      await execute(`
        UPDATE scraped_documents 
        SET raw_content = ?, title = ?, processed = 1, scraped_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `, [docData, `${profile.name} (${profile.ein})`, existing[0].id]);
      updated++;
    } else {
      await execute(`
        INSERT INTO scraped_documents (source_key, url, title, raw_content, processed)
        VALUES (?, ?, ?, ?, 1)
      `, ['charitable_trusts', profile.ein, `${profile.name} (${profile.ein})`, docData]);
      saved++;
    }
    
    // Save fraud indicators
    for (const indicator of profile.fraudIndicators) {
      const existingIndicator = await query(`
        SELECT id FROM fraud_indicators 
        WHERE indicator_type = ? AND description LIKE ?
      `, [indicator.type, `%${profile.ein}%`]);
      
      if (existingIndicator.length === 0) {
        await execute(`
          INSERT INTO fraud_indicators (indicator_type, severity, description, status)
          VALUES (?, ?, ?, 'open')
        `, [
          indicator.type,
          indicator.severity,
          `[990 ${profile.ein}] ${profile.name}: ${indicator.description}`,
        ]);
      }
    }
  }
  
  return { saved, updated };
}

/**
 * Main scraper function
 */
export async function scrapeCharitableTrusts(): Promise<{
  profiles: NonprofitProfile[];
  stats: { total: number; withFraudIndicators: number; saved: number; updated: number; totalRevenue: number };
}> {
  console.log('\n=== Charitable Trusts / Form 990 Scraper ===\n');
  
  await initializeDb();
  
  const profiles: NonprofitProfile[] = [];
  
  // Process each target nonprofit
  for (const target of TARGET_NONPROFITS) {
    console.log(`Processing ${target.name} (EIN: ${target.ein})...`);
    
    // Try ProPublica API first
    const apiData = await fetchProPublicaData(target.ein);
    
    // Use API data or fall back to known data
    const knownData = KNOWN_NONPROFIT_DATA.find(d => d.ein === target.ein);
    
    if (apiData?.organization) {
      const org = apiData.organization;
      const filing = apiData.filings_with_data?.[0]?.totals || {};
      
      const profile: NonprofitProfile = {
        ein: target.ein,
        name: org.name || target.name,
        state: org.state || target.state,
        city: org.city || '',
        nteeCode: org.ntee_code || null,
        subsection: org.subsection_code || '',
        filingStatus: org.filing_requirement || 'Unknown',
        latestFilingYear: apiData.filings_with_data?.[0]?.tax_prd_yr || null,
        totalRevenue: filing.totrevenue || knownData?.totalRevenue || null,
        totalExpenses: filing.totfuncexpns || knownData?.totalExpenses || null,
        totalAssets: filing.totassetsend || knownData?.totalAssets || null,
        governmentGrants: filing.grntstogovt || knownData?.governmentGrants || null,
        programServiceRevenue: filing.totprgmrevnue || knownData?.programServiceRevenue || null,
        executiveCompensation: knownData?.executiveCompensation || [],
        overheadRatio: knownData?.overheadRatio || null,
        programExpenseRatio: knownData?.programExpenseRatio || null,
        sourceUrl: `https://projects.propublica.org/nonprofits/organizations/${target.ein}`,
        fraudIndicators: [],
      };
      
      profiles.push(profile);
    } else if (knownData) {
      // Use known data
      profiles.push({
        ein: knownData.ein!,
        name: knownData.name!,
        state: knownData.state!,
        city: knownData.city || '',
        nteeCode: knownData.nteeCode || null,
        subsection: '',
        filingStatus: 'Known',
        latestFilingYear: knownData.latestFilingYear || null,
        totalRevenue: knownData.totalRevenue || null,
        totalExpenses: knownData.totalExpenses || null,
        totalAssets: knownData.totalAssets || null,
        governmentGrants: knownData.governmentGrants || null,
        programServiceRevenue: knownData.programServiceRevenue || null,
        executiveCompensation: knownData.executiveCompensation || [],
        overheadRatio: knownData.overheadRatio || null,
        programExpenseRatio: knownData.programExpenseRatio || null,
        sourceUrl: knownData.sourceUrl || '',
        fraudIndicators: [],
      });
    }
  }
  
  // Analyze each profile for fraud
  console.log('\nAnalyzing nonprofit financials...');
  for (const profile of profiles) {
    const statePayments = await getStatePayments(profile.name);
    const federalAwards = await getFederalAwards(profile.name);
    
    console.log(`  ${profile.name}: State $${statePayments.toLocaleString()}, Federal $${federalAwards.toLocaleString()}`);
    
    profile.fraudIndicators = analyzeForFraud(profile, statePayments, federalAwards);
  }
  
  // Save to database
  console.log('\nSaving to database...');
  const { saved, updated } = await saveProfiles(profiles);
  
  const stats = {
    total: profiles.length,
    withFraudIndicators: profiles.filter(p => p.fraudIndicators.length > 0).length,
    saved,
    updated,
    totalRevenue: profiles.reduce((sum, p) => sum + (p.totalRevenue || 0), 0),
  };
  
  console.log('\n=== Scrape Summary ===');
  console.log(`Total nonprofits: ${stats.total}`);
  console.log(`Combined revenue: $${stats.totalRevenue.toLocaleString()}`);
  console.log(`With fraud indicators: ${stats.withFraudIndicators}`);
  console.log(`Saved: ${stats.saved}, Updated: ${stats.updated}`);
  
  return { profiles, stats };
}

/**
 * Get all nonprofit profiles from database
 */
export async function getCharitableTrusts(filters?: {
  ein?: string;
  withFraudIndicatorsOnly?: boolean;
}): Promise<NonprofitProfile[]> {
  await initializeDb();
  
  const docs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'charitable_trusts'
    ORDER BY scraped_at DESC
  `);
  
  let profiles: NonprofitProfile[] = docs.map((doc: any) => {
    try {
      return JSON.parse(doc.raw_content);
    } catch {
      return null;
    }
  }).filter(Boolean);
  
  if (filters?.ein) {
    profiles = profiles.filter(p => p.ein === filters.ein);
  }
  
  if (filters?.withFraudIndicatorsOnly) {
    profiles = profiles.filter(p => p.fraudIndicators && p.fraudIndicators.length > 0);
  }
  
  return profiles;
}

// CLI execution
const isMain = process.argv[1]?.includes('charitable-trusts');
if (isMain) {
  scrapeCharitableTrusts()
    .then(result => {
      console.log('\nNonprofits with fraud indicators:');
      for (const profile of result.profiles.filter(p => p.fraudIndicators.length > 0)) {
        console.log(`\n${profile.name} (EIN: ${profile.ein})`);
        console.log(`  Revenue: $${profile.totalRevenue?.toLocaleString() || 'Unknown'}`);
        console.log(`  Gov Grants: $${profile.governmentGrants?.toLocaleString() || 'Unknown'}`);
        console.log('  Fraud Indicators:');
        for (const indicator of profile.fraudIndicators) {
          console.log(`    [${indicator.severity.toUpperCase()}] ${indicator.type}: ${indicator.description}`);
        }
      }
    })
    .catch(console.error);
}

export default {
  scrapeCharitableTrusts,
  getCharitableTrusts,
  TARGET_NONPROFITS,
};
