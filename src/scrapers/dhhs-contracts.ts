/**
 * Scraper for NH DHHS Contracts & Procurement Data
 * 
 * Data Sources:
 * 1. DHHS RFP/RFA PDFs (available at https://www.dhhs.nh.gov/sites/g/files/ehbemt476/files/inline-documents/sonh/)
 * 2. Contract award notices linked from G&C agendas
 * 3. DHHS news/announcements for RFA postings
 * 
 * This scraper bridges data from:
 * - Governor & Council agendas (contract approvals)
 * - TransparentNH expenditures (actual payments)
 * - DHHS RFP/RFA documents (procurement details)
 * 
 * Fraud Detection Focus:
 * - Sole source contracts (no competitive bidding)
 * - Contract amendments that significantly increase value
 * - Patterns of awards to same vendors
 * - Contracts with immigrant-related service providers
 */

import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { initializeDb, dbHelpers } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Known DHHS RFP/RFA document patterns
const DHHS_PDF_BASE = 'https://www.dhhs.nh.gov/sites/g/files/ehbemt476/files/inline-documents/sonh/';

// Target keywords for immigrant/refugee related contracts
const TARGET_KEYWORDS = [
  'refugee', 'resettlement', 'immigrant', 'immigration', 
  'ORR', 'Office of Refugee Resettlement',
  'asylum', 'migrant', 'newcomer',
  'LEP', 'limited english proficiency',
  'translation', 'interpreter', 'language access',
  'health equity', 'cultural', 'multilingual',
  'Ukrainian', 'Afghan', 'Haitian', 'Congolese',
  'Ascentria', 'International Institute', 'IINE',
  'Lutheran Immigration', 'Catholic Charities', 'USCRI'
];

// Known refugee/immigrant service contractors (for cross-referencing)
const KNOWN_CONTRACTORS: Record<string, ContractorInfo> = {
  'ascentria': {
    names: ['Ascentria Community Services', 'Ascentria Care Alliance', 'Ascentria'],
    vendorCodes: ['222201'],
    services: ['refugee resettlement', 'case management', 'language services', 'ORR support'],
  },
  'iine': {
    names: ['International Institute of New England', 'IINE', 'Int\'l Institute of New England'],
    vendorCodes: ['177551'],
    services: ['refugee resettlement', 'ESL', 'employment services', 'school services'],
  },
  'catholic_charities': {
    names: ['Catholic Charities NH', 'Catholic Charities New Hampshire', 'CCNH'],
    vendorCodes: [],
    services: ['immigration legal services', 'refugee support'],
  },
  'lutheran': {
    names: ['Lutheran Immigration and Refugee Service', 'LIRS'],
    vendorCodes: [],
    services: ['national resettlement coordination'],
  },
};

interface ContractorInfo {
  names: string[];
  vendorCodes: string[];
  services: string[];
}

interface DHHSContract {
  rfpNumber: string;
  title: string;
  department: string;
  division: string;
  description: string;
  estimatedValue: number | null;
  awardedValue: number | null;
  awardedVendor: string | null;
  awardedVendorCode: string | null;
  solicitationType: 'RFP' | 'RFA' | 'RFI' | 'sole_source' | 'amendment';
  postDate: string | null;
  dueDate: string | null;
  awardDate: string | null;
  contractStartDate: string | null;
  contractEndDate: string | null;
  pdfUrl: string | null;
  gcAgendaDate: string | null;
  gcItemNumber: string | null;
  isImmigrantRelated: boolean;
  matchedKeywords: string[];
  fraudIndicators: FraudIndicator[];
  sourceUrl: string;
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
    .replace(/\s+(inc|llc|corp|corporation|ltd|co|company)\.?$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Match vendor name to known contractors
 */
function matchKnownContractor(vendorName: string): { key: string; info: ContractorInfo } | null {
  const normalized = normalizeVendorName(vendorName);
  
  for (const [key, info] of Object.entries(KNOWN_CONTRACTORS)) {
    for (const name of info.names) {
      if (normalized.includes(normalizeVendorName(name)) || 
          normalizeVendorName(name).includes(normalized)) {
        return { key, info };
      }
    }
  }
  return null;
}

/**
 * Check if text contains immigrant-related keywords
 */
function checkImmigrantRelated(text: string): { isRelated: boolean; keywords: string[] } {
  const lowerText = text.toLowerCase();
  const found = TARGET_KEYWORDS.filter(kw => lowerText.includes(kw.toLowerCase()));
  return { isRelated: found.length > 0, keywords: found };
}

/**
 * Parse dollar amount from text
 */
function parseAmount(text: string): number | null {
  const match = text.match(/\$\s*([\d,]+(?:\.\d{2})?)/);
  if (match) {
    return parseFloat(match[1].replace(/,/g, ''));
  }
  // Try without dollar sign for large numbers
  const numMatch = text.match(/(\d{1,3}(?:,\d{3})+(?:\.\d{2})?)/);
  if (numMatch) {
    const val = parseFloat(numMatch[1].replace(/,/g, ''));
    if (val > 10000) return val; // Likely a contract amount
  }
  return null;
}

/**
 * Extract RFP/RFA number from text
 */
function extractRfpNumber(text: string): string | null {
  const patterns = [
    /RFP-(\d{4}-[A-Z]+-\d+-[A-Z]+)/i,
    /RFA-(\d{4}-[A-Z]+-\d+-[A-Z]+)/i,
    /RFP-(\d{4}-[A-Z]+-\d+)/i,
    /RFA-(\d{4}-[A-Z]+-\d+)/i,
    /(RFP|RFA)-(\d{4})-([A-Z]+)-(\d+)/i,
  ];
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return match[0].toUpperCase();
    }
  }
  return null;
}

/**
 * Analyze contract for fraud indicators
 */
function analyzeForFraud(contract: Partial<DHHSContract>, existingContracts: DHHSContract[]): FraudIndicator[] {
  const indicators: FraudIndicator[] = [];
  
  // 1. Sole source without competition
  if (contract.solicitationType === 'sole_source') {
    indicators.push({
      type: 'sole_source',
      severity: 'medium',
      description: `Contract awarded without competitive bidding process`,
    });
  }
  
  // 2. Large contract value
  if (contract.awardedValue && contract.awardedValue > 500000) {
    indicators.push({
      type: 'large_contract',
      severity: contract.awardedValue > 1000000 ? 'high' : 'medium',
      description: `Large contract value: $${contract.awardedValue.toLocaleString()}`,
    });
  }
  
  // 3. Significant value increase from estimate
  if (contract.estimatedValue && contract.awardedValue) {
    const increase = ((contract.awardedValue - contract.estimatedValue) / contract.estimatedValue) * 100;
    if (increase > 50) {
      indicators.push({
        type: 'value_increase',
        severity: increase > 100 ? 'high' : 'medium',
        description: `Award ${increase.toFixed(0)}% higher than estimated value`,
      });
    }
  }
  
  // 4. Multiple contracts to same vendor
  if (contract.awardedVendor) {
    const vendorContracts = existingContracts.filter(c => 
      c.awardedVendor && normalizeVendorName(c.awardedVendor) === normalizeVendorName(contract.awardedVendor!)
    );
    if (vendorContracts.length >= 3) {
      const totalValue = vendorContracts.reduce((sum, c) => sum + (c.awardedValue || 0), 0);
      indicators.push({
        type: 'vendor_concentration',
        severity: vendorContracts.length >= 5 ? 'high' : 'medium',
        description: `Vendor has ${vendorContracts.length} contracts totaling $${totalValue.toLocaleString()}`,
      });
    }
  }
  
  // 5. Short turnaround (award close to posting)
  if (contract.postDate && contract.awardDate) {
    const postDate = new Date(contract.postDate);
    const awardDate = new Date(contract.awardDate);
    const daysBetween = (awardDate.getTime() - postDate.getTime()) / (1000 * 60 * 60 * 24);
    if (daysBetween < 14) {
      indicators.push({
        type: 'rapid_award',
        severity: daysBetween < 7 ? 'high' : 'medium',
        description: `Contract awarded only ${daysBetween.toFixed(0)} days after posting`,
      });
    }
  }
  
  // 6. Amendment pattern
  if (contract.solicitationType === 'amendment') {
    indicators.push({
      type: 'amendment',
      severity: 'low',
      description: `Contract amendment - review original terms`,
    });
  }
  
  return indicators;
}

/**
 * Known DHHS RFPs/RFAs related to refugee/immigrant services
 * Manually compiled from research - will be supplemented by scraping
 */
const KNOWN_DHHS_DOCUMENTS: Partial<DHHSContract>[] = [
  // Refugee Support Services RFAs
  {
    rfpNumber: 'RFA-2025-OCOM-01-REFUG',
    title: 'Refugee Support Services',
    department: 'Health and Human Services',
    division: 'Office of the Commissioner - Health Equity',
    description: 'Refugee Support Services, Youth Mentoring Services, and Refugee School Impact Program',
    solicitationType: 'RFA',
    pdfUrl: `${DHHS_PDF_BASE}rfa-2025-ocom-refug-qa.pdf`,
    isImmigrantRelated: true,
    matchedKeywords: ['refugee', 'resettlement', 'ORR'],
    sourceUrl: 'https://www.dhhs.nh.gov/news-and-media/rfa-2025-ocom-01-refug-refugee-support-services',
  },
  {
    rfpNumber: 'RFA-2024-OCOM-01-REFUG',
    title: 'Refugee Support Services FY2024',
    department: 'Health and Human Services',
    division: 'Office of the Commissioner - Health Equity',
    description: 'Continuation of refugee support services including case management and employment assistance',
    solicitationType: 'RFA',
    awardedVendor: 'Ascentria Community Services, Inc.',
    awardedVendorCode: '222201',
    awardedValue: 1217727,
    isImmigrantRelated: true,
    matchedKeywords: ['refugee', 'Ascentria', 'ORR'],
    gcAgendaDate: '2024-06-19',
    sourceUrl: 'https://media.sos.nh.gov/govcouncil/',
  },
  {
    rfpNumber: 'SS-2023-OCOM-REFUGEE-01',
    title: 'Ukrainian Refugee Resettlement Support',
    department: 'Health and Human Services',
    division: 'Office of Health Equity',
    description: 'Emergency sole-source contract for Ukrainian refugee resettlement support',
    solicitationType: 'sole_source',
    awardedVendor: 'Ascentria Community Services, Inc.',
    awardedVendorCode: '222201',
    awardedValue: 251910,
    awardDate: '2023-03-15',
    isImmigrantRelated: true,
    matchedKeywords: ['refugee', 'Ukrainian', 'Ascentria', 'resettlement'],
    sourceUrl: 'https://media.sos.nh.gov/govcouncil/',
  },
  {
    rfpNumber: 'RFP-2023-OCOM-LANG-01',
    title: 'Language Access Services',
    department: 'Health and Human Services',
    division: 'Office of Health Equity',
    description: 'Translation and interpretation services for LEP populations',
    solicitationType: 'RFP',
    awardedVendor: 'Ascentria Community Services, Inc.',
    awardedVendorCode: '222201',
    estimatedValue: 500000,
    awardedValue: 769637,
    isImmigrantRelated: true,
    matchedKeywords: ['language access', 'LEP', 'translation', 'interpreter'],
    sourceUrl: 'https://media.sos.nh.gov/govcouncil/',
  },
  {
    rfpNumber: 'RFA-2023-OCOM-SCHOOL-01',
    title: 'School Services for Refugees',
    department: 'Health and Human Services',
    division: 'Office of Health Equity',
    description: 'Educational support services for refugee children in NH schools',
    solicitationType: 'RFA',
    awardedVendor: 'International Institute of New England, Inc.',
    awardedVendorCode: '177551',
    estimatedValue: 805000,
    awardedValue: 844000,
    isImmigrantRelated: true,
    matchedKeywords: ['refugee', 'school', 'IINE'],
    gcAgendaDate: '2023-06-14',
    sourceUrl: 'https://media.sos.nh.gov/govcouncil/',
  },
  // Child Care related (potential overlap with immigrant services)
  {
    rfpNumber: 'RFP-2025-DES-02-CCWAP',
    title: 'Child Care Workforce Assistance Project',
    department: 'Health and Human Services',
    division: 'Division of Economic Stability',
    description: 'Child care workforce assistance including multilingual support',
    estimatedValue: 1100000,
    solicitationType: 'RFP',
    pdfUrl: `${DHHS_PDF_BASE}1.-rfp-2025-des-02-ccwap.pdf`,
    isImmigrantRelated: false,
    matchedKeywords: [],
    sourceUrl: 'https://www.dhhs.nh.gov/doing-business-dhhs/contracts-procurement-opportunities',
  },
];

/**
 * Fetch and parse DHHS news page for RFA/RFP announcements
 */
async function fetchDHHSNewsAnnouncements(): Promise<Partial<DHHSContract>[]> {
  const contracts: Partial<DHHSContract>[] = [];
  
  // DHHS news RSS or page - try multiple approaches
  const newsUrls = [
    'https://www.dhhs.nh.gov/news-and-media',
    // SOS media server for direct PDF access
    'https://media.sos.nh.gov/',
  ];
  
  // For now, return empty - the site blocks automated requests
  // In production, this would use a browser automation tool or approved API
  console.log('Note: DHHS website blocks direct fetch. Using known document list.');
  
  return contracts;
}

/**
 * Cross-reference with G&C agenda data to find award details
 */
async function crossReferenceGCAgendas(contracts: Partial<DHHSContract>[]): Promise<DHHSContract[]> {
  const enriched: DHHSContract[] = [];
  
  // Query existing scraped G&C documents
  const gcDocs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'governor_council' 
    ORDER BY scraped_at DESC
  `);
  
  for (const contract of contracts) {
    // Try to match with G&C agenda items by RFP number or vendor
    let gcMatch = null;
    
    if (contract.rfpNumber) {
      gcMatch = gcDocs.find((doc: any) => {
        const parsed = typeof doc.raw_content === 'string' 
          ? JSON.parse(doc.raw_content) 
          : doc.raw_content;
        return parsed?.rfpNumber === contract.rfpNumber ||
               parsed?.title?.includes(contract.rfpNumber);
      });
    }
    
    // Merge data
    const enrichedContract: DHHSContract = {
      rfpNumber: contract.rfpNumber || 'UNKNOWN',
      title: contract.title || 'Unknown Contract',
      department: contract.department || 'Health and Human Services',
      division: contract.division || 'Unknown',
      description: contract.description || '',
      estimatedValue: contract.estimatedValue || null,
      awardedValue: contract.awardedValue || null,
      awardedVendor: contract.awardedVendor || null,
      awardedVendorCode: contract.awardedVendorCode || null,
      solicitationType: contract.solicitationType || 'RFP',
      postDate: contract.postDate || null,
      dueDate: contract.dueDate || null,
      awardDate: contract.awardDate || null,
      contractStartDate: contract.contractStartDate || null,
      contractEndDate: contract.contractEndDate || null,
      pdfUrl: contract.pdfUrl || null,
      gcAgendaDate: contract.gcAgendaDate || gcMatch?.document_date || null,
      gcItemNumber: contract.gcItemNumber || null,
      isImmigrantRelated: contract.isImmigrantRelated || false,
      matchedKeywords: contract.matchedKeywords || [],
      fraudIndicators: [],
      sourceUrl: contract.sourceUrl || '',
    };
    
    enriched.push(enrichedContract);
  }
  
  return enriched;
}

/**
 * Cross-reference with expenditure data to find actual payments
 */
async function crossReferenceExpenditures(contracts: DHHSContract[]): Promise<void> {
  for (const contract of contracts) {
    if (!contract.awardedVendorCode && !contract.awardedVendor) continue;
    
    // Look for matching expenditures
    let expenditures;
    if (contract.awardedVendorCode) {
      expenditures = await query(`
        SELECT * FROM expenditures 
        WHERE vendor_name LIKE ? OR vendor_name LIKE ?
        ORDER BY fiscal_year DESC
      `, [`%${contract.awardedVendorCode}%`, `%${contract.awardedVendor}%`]);
    } else if (contract.awardedVendor) {
      const normalized = normalizeVendorName(contract.awardedVendor);
      expenditures = await query(`
        SELECT * FROM expenditures 
        WHERE LOWER(vendor_name) LIKE ?
        ORDER BY fiscal_year DESC
      `, [`%${normalized}%`]);
    }
    
    if (expenditures && expenditures.length > 0) {
      const totalPaid = expenditures.reduce((sum: number, e: any) => sum + (e.amount || 0), 0);
      
      // Check for payment discrepancies
      if (contract.awardedValue && totalPaid > contract.awardedValue * 1.1) {
        contract.fraudIndicators.push({
          type: 'overpayment',
          severity: 'high',
          description: `Total payments ($${totalPaid.toLocaleString()}) exceed contract value ($${contract.awardedValue.toLocaleString()}) by ${((totalPaid/contract.awardedValue - 1) * 100).toFixed(0)}%`,
        });
      }
    }
  }
}

/**
 * Save contracts to database
 */
async function saveContracts(contracts: DHHSContract[]): Promise<{ saved: number; updated: number }> {
  let saved = 0;
  let updated = 0;
  
  for (const contract of contracts) {
    // Check if contract already exists
    const existing = await query(
      'SELECT id FROM scraped_documents WHERE source_key = ? AND url = ?',
      ['dhhs_contracts', contract.rfpNumber]
    );
    
    const docData = JSON.stringify({
      ...contract,
      scrapedAt: new Date().toISOString(),
    });
    
    if (existing.length > 0) {
      // Update existing
      await execute(`
        UPDATE scraped_documents 
        SET raw_content = ?, title = ?, processed = 1, scraped_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `, [docData, contract.title, existing[0].id]);
      updated++;
    } else {
      // Insert new
      await execute(`
        INSERT INTO scraped_documents (source_key, url, title, raw_content, processed)
        VALUES (?, ?, ?, ?, 1)
      `, ['dhhs_contracts', contract.rfpNumber, contract.title, docData]);
      saved++;
    }
    
    // Also save/update fraud indicators
    for (const indicator of contract.fraudIndicators) {
      // Try to find provider_master_id
      let providerId = null;
      if (contract.awardedVendor) {
        const provider = await query(
          'SELECT id FROM provider_master WHERE canonical_name LIKE ? OR name_display LIKE ?',
          [`%${contract.awardedVendor}%`, `%${contract.awardedVendor}%`]
        );
        if (provider.length > 0) {
          providerId = provider[0].id;
        }
      }
      
      // Check if indicator already exists
      const existingIndicator = await query(`
        SELECT id FROM fraud_indicators 
        WHERE indicator_type = ? AND description LIKE ?
      `, [indicator.type, `%${contract.rfpNumber}%`]);
      
      if (existingIndicator.length === 0) {
        await execute(`
          INSERT INTO fraud_indicators (provider_master_id, indicator_type, severity, description, status)
          VALUES (?, ?, ?, ?, 'open')
        `, [
          providerId,
          indicator.type,
          indicator.severity,
          `[${contract.rfpNumber}] ${indicator.description}`,
        ]);
      }
    }
  }
  
  return { saved, updated };
}

/**
 * Main scraper function
 */
export async function scrapeDHHSContracts(): Promise<{
  contracts: DHHSContract[];
  stats: { total: number; immigrantRelated: number; withFraudIndicators: number; saved: number; updated: number };
}> {
  console.log('\n=== DHHS Contracts Scraper ===\n');
  
  await initializeDb();
  
  // 1. Start with known documents
  console.log('Loading known DHHS contract documents...');
  let contracts = [...KNOWN_DHHS_DOCUMENTS];
  
  // 2. Try to fetch new announcements (will likely fail due to CDN blocking)
  console.log('Attempting to fetch new announcements...');
  const newAnnouncements = await fetchDHHSNewsAnnouncements();
  contracts = contracts.concat(newAnnouncements);
  
  // 3. Cross-reference with G&C agendas
  console.log('Cross-referencing with Governor & Council agendas...');
  const enrichedContracts = await crossReferenceGCAgendas(contracts);
  
  // 4. Analyze for fraud indicators
  console.log('Analyzing for fraud indicators...');
  for (const contract of enrichedContracts) {
    contract.fraudIndicators = analyzeForFraud(contract, enrichedContracts);
  }
  
  // 5. Cross-reference with expenditure data
  console.log('Cross-referencing with expenditure data...');
  await crossReferenceExpenditures(enrichedContracts);
  
  // 6. Save to database
  console.log('Saving to database...');
  const { saved, updated } = await saveContracts(enrichedContracts);
  
  // Calculate stats
  const stats = {
    total: enrichedContracts.length,
    immigrantRelated: enrichedContracts.filter(c => c.isImmigrantRelated).length,
    withFraudIndicators: enrichedContracts.filter(c => c.fraudIndicators.length > 0).length,
    saved,
    updated,
  };
  
  console.log('\n=== Scrape Summary ===');
  console.log(`Total contracts: ${stats.total}`);
  console.log(`Immigrant-related: ${stats.immigrantRelated}`);
  console.log(`With fraud indicators: ${stats.withFraudIndicators}`);
  console.log(`Saved: ${stats.saved}, Updated: ${stats.updated}`);
  
  return { contracts: enrichedContracts, stats };
}

/**
 * Get all DHHS contracts from database
 */
export async function getDHHSContracts(filters?: {
  immigrantRelatedOnly?: boolean;
  withFraudIndicatorsOnly?: boolean;
  vendor?: string;
}): Promise<DHHSContract[]> {
  await initializeDb();
  
  const docs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'dhhs_contracts'
    ORDER BY scraped_at DESC
  `);
  
  let contracts: DHHSContract[] = docs.map((doc: any) => {
    try {
      return JSON.parse(doc.raw_content);
    } catch {
      return null;
    }
  }).filter(Boolean);
  
  if (filters?.immigrantRelatedOnly) {
    contracts = contracts.filter(c => c.isImmigrantRelated);
  }
  
  if (filters?.withFraudIndicatorsOnly) {
    contracts = contracts.filter(c => c.fraudIndicators && c.fraudIndicators.length > 0);
  }
  
  if (filters?.vendor) {
    const normalized = normalizeVendorName(filters.vendor);
    contracts = contracts.filter(c => 
      c.awardedVendor && normalizeVendorName(c.awardedVendor).includes(normalized)
    );
  }
  
  return contracts;
}

/**
 * Link DHHS contract to provider_master
 */
export async function linkContractToProvider(rfpNumber: string, providerId: number): Promise<void> {
  await initializeDb();
  
  // Get the contract
  const docs = await query(
    'SELECT * FROM scraped_documents WHERE source_key = ? AND url = ?',
    ['dhhs_contracts', rfpNumber]
  );
  
  if (docs.length === 0) {
    throw new Error(`Contract ${rfpNumber} not found`);
  }
  
  const contract: DHHSContract = JSON.parse(docs[0].raw_content);
  
  // Create a source link
  await execute(`
    INSERT OR IGNORE INTO provider_source_links (provider_master_id, source_system, source_identifier, status)
    VALUES (?, 'dhhs_contract', ?, 'active')
  `, [providerId, rfpNumber]);
  
  // If contract has award value, create a payment record
  if (contract.awardedValue) {
    const fiscalYear = contract.awardDate 
      ? new Date(contract.awardDate).getFullYear()
      : new Date().getFullYear();
    
    await execute(`
      INSERT INTO payments (provider_master_id, fiscal_year, amount, funding_source, description, source_url)
      VALUES (?, ?, ?, 'DHHS Contract', ?, ?)
    `, [
      providerId,
      fiscalYear,
      contract.awardedValue,
      `${contract.rfpNumber}: ${contract.title}`,
      contract.sourceUrl,
    ]);
  }
  
  console.log(`Linked contract ${rfpNumber} to provider ${providerId}`);
}

// CLI execution
const isMain = process.argv[1]?.includes('dhhs-contracts');
if (isMain) {
  scrapeDHHSContracts()
    .then(result => {
      console.log('\nContracts with fraud indicators:');
      for (const contract of result.contracts.filter(c => c.fraudIndicators.length > 0)) {
        console.log(`\n${contract.rfpNumber}: ${contract.title}`);
        console.log(`  Vendor: ${contract.awardedVendor || 'Not awarded'}`);
        console.log(`  Value: $${contract.awardedValue?.toLocaleString() || 'Unknown'}`);
        console.log('  Fraud Indicators:');
        for (const indicator of contract.fraudIndicators) {
          console.log(`    [${indicator.severity.toUpperCase()}] ${indicator.type}: ${indicator.description}`);
        }
      }
    })
    .catch(console.error);
}

export default {
  scrapeDHHSContracts,
  getDHHSContracts,
  linkContractToProvider,
  KNOWN_CONTRACTORS,
  TARGET_KEYWORDS,
};
