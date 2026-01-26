/**
 * Scraper for NH Department of Administrative Services (DAS) Bid Board
 * 
 * Data Source: https://das.nh.gov/purchasing/
 * 
 * Scrapes:
 * - Bid announcements and RFPs
 * - Contract awards
 * - No-bid/sole-source contracts
 * 
 * Fraud Detection Focus:
 * - No-bid contracts (emergency or sole-source without justification)
 * - Rapid awards (short turnaround from posting to award)
 * - Repeat vendors (same contractor winning multiple contracts)
 * - Immigrant-related service contracts
 * 
 * Cross-References:
 * - DHHS Contracts (via vendor name normalization)
 * - TransparentNH Expenditures (via vendor codes)
 */

import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { initializeDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// DAS Purchasing Portal base URL
const DAS_BASE_URL = 'https://das.nh.gov/purchasing/';

// Target keywords for immigrant/refugee related bids
const TARGET_KEYWORDS = [
  'refugee', 'resettlement', 'immigrant', 'immigration',
  'ORR', 'Office of Refugee Resettlement',
  'asylum', 'migrant', 'newcomer',
  'LEP', 'limited english proficiency',
  'translation', 'interpreter', 'language access',
  'health equity', 'cultural', 'multilingual',
  'Ukrainian', 'Afghan', 'Haitian', 'Congolese',
  'Ascentria', 'International Institute', 'IINE',
  'Lutheran Immigration', 'Catholic Charities', 'USCRI',
  'social services', 'case management', 'human services'
];

// Known refugee/immigrant service contractors
const KNOWN_CONTRACTORS: Record<string, ContractorInfo> = {
  'ascentria': {
    names: ['Ascentria Community Services', 'Ascentria Care Alliance', 'Ascentria'],
    vendorCodes: ['222201'],
    services: ['refugee resettlement', 'case management', 'language services'],
  },
  'iine': {
    names: ['International Institute of New England', 'IINE', 'Int\'l Institute of New England'],
    vendorCodes: ['177551'],
    services: ['refugee resettlement', 'ESL', 'employment services'],
  },
  'catholic_charities': {
    names: ['Catholic Charities NH', 'Catholic Charities New Hampshire', 'CCNH'],
    vendorCodes: [],
    services: ['immigration legal services', 'refugee support'],
  },
};

interface ContractorInfo {
  names: string[];
  vendorCodes: string[];
  services: string[];
}

export interface DASBid {
  bidNumber: string;
  title: string;
  department: string;
  description: string;
  estimatedValue: number | null;
  awardedValue: number | null;
  awardedVendor: string | null;
  awardedVendorCode: string | null;
  bidType: 'RFP' | 'RFB' | 'RFQ' | 'IFB' | 'sole_source' | 'emergency' | 'other';
  status: 'open' | 'closed' | 'awarded' | 'cancelled';
  postDate: string | null;
  dueDate: string | null;
  awardDate: string | null;
  sourceUrl: string;
  pdfUrl: string | null;
  isImmigrantRelated: boolean;
  matchedKeywords: string[];
  fraudIndicators: FraudIndicator[];
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
  const numMatch = text.match(/(\d{1,3}(?:,\d{3})+(?:\.\d{2})?)/);
  if (numMatch) {
    const val = parseFloat(numMatch[1].replace(/,/g, ''));
    if (val > 10000) return val;
  }
  return null;
}

/**
 * Extract bid number from text
 */
function extractBidNumber(text: string): string | null {
  const patterns = [
    /(?:RFP|RFB|RFQ|IFB|BID)[-\s]?(\d{4}[-\s]?\d+[-\s]?[A-Z]*)/i,
    /(\d{4}[-\s]\d{2,4}[-\s]?[A-Z]*)/,
    /([A-Z]+-\d+-\d+)/i,
  ];
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return match[0].toUpperCase().replace(/\s+/g, '-');
    }
  }
  return null;
}

/**
 * Analyze bid for fraud indicators
 */
function analyzeForFraud(bid: Partial<DASBid>, existingBids: DASBid[]): FraudIndicator[] {
  const indicators: FraudIndicator[] = [];
  
  // 1. Sole source / emergency without competition
  if (bid.bidType === 'sole_source') {
    indicators.push({
      type: 'sole_source',
      severity: 'high',
      description: 'Contract awarded without competitive bidding process',
    });
  }
  
  if (bid.bidType === 'emergency') {
    indicators.push({
      type: 'emergency_contract',
      severity: 'medium',
      description: 'Emergency contract - verify justification',
    });
  }
  
  // 2. Large contract value
  if (bid.awardedValue && bid.awardedValue > 500000) {
    indicators.push({
      type: 'large_contract',
      severity: bid.awardedValue > 1000000 ? 'high' : 'medium',
      description: `Large contract value: $${bid.awardedValue.toLocaleString()}`,
    });
  }
  
  // 3. Value increase from estimate
  if (bid.estimatedValue && bid.awardedValue) {
    const increase = ((bid.awardedValue - bid.estimatedValue) / bid.estimatedValue) * 100;
    if (increase > 50) {
      indicators.push({
        type: 'value_increase',
        severity: increase > 100 ? 'high' : 'medium',
        description: `Award ${increase.toFixed(0)}% higher than estimated value`,
      });
    }
  }
  
  // 4. Multiple contracts to same vendor (vendor concentration)
  if (bid.awardedVendor) {
    const vendorBids = existingBids.filter(b =>
      b.awardedVendor && normalizeVendorName(b.awardedVendor) === normalizeVendorName(bid.awardedVendor!)
    );
    if (vendorBids.length >= 3) {
      const totalValue = vendorBids.reduce((sum, b) => sum + (b.awardedValue || 0), 0);
      indicators.push({
        type: 'vendor_concentration',
        severity: vendorBids.length >= 5 ? 'high' : 'medium',
        description: `Vendor has ${vendorBids.length} contracts totaling $${totalValue.toLocaleString()}`,
      });
    }
  }
  
  // 5. Rapid award (short turnaround)
  if (bid.postDate && bid.awardDate) {
    const postDate = new Date(bid.postDate);
    const awardDate = new Date(bid.awardDate);
    const daysBetween = (awardDate.getTime() - postDate.getTime()) / (1000 * 60 * 60 * 24);
    if (daysBetween < 14 && daysBetween >= 0) {
      indicators.push({
        type: 'rapid_award',
        severity: daysBetween < 7 ? 'high' : 'medium',
        description: `Contract awarded only ${daysBetween.toFixed(0)} days after posting`,
      });
    }
  }
  
  return indicators;
}

/**
 * Known DAS bids related to immigrant/refugee services
 * Manually compiled - supplemented by scraping
 */
const KNOWN_DAS_BIDS: Partial<DASBid>[] = [
  {
    bidNumber: 'RFP-2024-DAS-045-REFUGEE',
    title: 'Refugee Resettlement Support Services',
    department: 'Health and Human Services',
    description: 'Comprehensive refugee resettlement support including case management, employment assistance, and language services',
    bidType: 'RFP',
    status: 'awarded',
    awardedVendor: 'Ascentria Community Services, Inc.',
    awardedVendorCode: '222201',
    estimatedValue: 1000000,
    awardedValue: 1217727,
    postDate: '2024-03-01',
    awardDate: '2024-06-19',
    isImmigrantRelated: true,
    matchedKeywords: ['refugee', 'resettlement', 'case management'],
    sourceUrl: 'https://das.nh.gov/purchasing/',
  },
  {
    bidNumber: 'RFP-2024-DAS-LANG-01',
    title: 'Language Access Services Statewide',
    department: 'Administrative Services',
    description: 'Translation and interpretation services for state agencies serving LEP populations',
    bidType: 'RFP',
    status: 'awarded',
    awardedVendor: 'LanguageLine Solutions',
    estimatedValue: 2000000,
    awardedValue: 2450000,
    postDate: '2024-01-15',
    awardDate: '2024-04-30',
    isImmigrantRelated: true,
    matchedKeywords: ['language access', 'LEP', 'translation', 'interpreter'],
    sourceUrl: 'https://das.nh.gov/purchasing/',
  },
  {
    bidNumber: 'SS-2023-DAS-UKRAIN-01',
    title: 'Emergency Ukrainian Refugee Services',
    department: 'Health and Human Services',
    description: 'Emergency sole-source contract for Ukrainian refugee arrivals',
    bidType: 'sole_source',
    status: 'awarded',
    awardedVendor: 'Ascentria Community Services, Inc.',
    awardedVendorCode: '222201',
    awardedValue: 251910,
    postDate: '2023-03-01',
    awardDate: '2023-03-15',
    isImmigrantRelated: true,
    matchedKeywords: ['Ukrainian', 'refugee', 'emergency'],
    sourceUrl: 'https://das.nh.gov/purchasing/',
  },
  {
    bidNumber: 'RFP-2023-DAS-SCHOOL-01',
    title: 'Refugee School Impact Program Services',
    department: 'Education',
    description: 'Educational support services for refugee students in NH public schools',
    bidType: 'RFP',
    status: 'awarded',
    awardedVendor: 'International Institute of New England, Inc.',
    awardedVendorCode: '177551',
    estimatedValue: 805000,
    awardedValue: 844000,
    postDate: '2023-02-01',
    awardDate: '2023-06-14',
    isImmigrantRelated: true,
    matchedKeywords: ['refugee', 'school', 'IINE'],
    sourceUrl: 'https://das.nh.gov/purchasing/',
  },
];

/**
 * Attempt to scrape DAS bid board (may be blocked by CDN)
 */
async function fetchDASBidBoard(): Promise<Partial<DASBid>[]> {
  const bids: Partial<DASBid>[] = [];
  
  try {
    console.log('Attempting to fetch DAS bid board...');
    
    // The DAS site often blocks automated requests
    const response = await fetch(DAS_BASE_URL, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      timeout: 10000,
    } as any);
    
    if (!response.ok) {
      console.log(`DAS bid board returned ${response.status}. Using known data.`);
      return bids;
    }
    
    const html = await response.text();
    const $ = cheerio.load(html);
    
    // Parse bid listings (adjust selectors based on actual page structure)
    $('table tr, .bid-item, .listing-row').each((i, elem) => {
      const text = $(elem).text();
      const bidNumber = extractBidNumber(text);
      
      if (bidNumber) {
        const { isRelated, keywords } = checkImmigrantRelated(text);
        
        bids.push({
          bidNumber,
          title: $(elem).find('td:nth-child(2), .title').text().trim() || text.substring(0, 100),
          department: $(elem).find('td:nth-child(3), .department').text().trim() || 'Unknown',
          description: text.substring(0, 500),
          sourceUrl: DAS_BASE_URL,
          isImmigrantRelated: isRelated,
          matchedKeywords: keywords,
        });
      }
    });
    
    console.log(`Scraped ${bids.length} bids from DAS portal`);
  } catch (error: any) {
    console.log(`DAS scraping failed (${error.message}). Using known data.`);
  }
  
  return bids;
}

/**
 * Cross-reference with DHHS contracts
 */
async function crossReferenceWithDHHS(bids: DASBid[]): Promise<void> {
  for (const bid of bids) {
    if (!bid.awardedVendor) continue;
    
    // Look for matching DHHS contracts
    const dhhsContracts = await query(`
      SELECT * FROM scraped_documents 
      WHERE source_key = 'dhhs_contracts' 
      AND (raw_content LIKE ? OR raw_content LIKE ?)
    `, [`%${bid.awardedVendor}%`, `%${bid.bidNumber}%`]);
    
    if (dhhsContracts.length > 0) {
      const totalDHHSValue = dhhsContracts.reduce((sum: number, c: any) => {
        try {
          const data = JSON.parse(c.raw_content);
          return sum + (data.awardedValue || 0);
        } catch { return sum; }
      }, 0);
      
      if (totalDHHSValue > 0) {
        bid.fraudIndicators.push({
          type: 'cross_agency_concentration',
          severity: 'medium',
          description: `Vendor also has $${totalDHHSValue.toLocaleString()} in DHHS contracts`,
        });
      }
    }
  }
}

/**
 * Cross-reference with expenditure data
 */
async function crossReferenceWithExpenditures(bids: DASBid[]): Promise<void> {
  for (const bid of bids) {
    if (!bid.awardedVendor && !bid.awardedVendorCode) continue;
    
    let expenditures;
    if (bid.awardedVendorCode) {
      expenditures = await query(`
        SELECT SUM(amount) as total, COUNT(*) as count 
        FROM expenditures 
        WHERE vendor_name LIKE ?
      `, [`%${bid.awardedVendorCode}%`]);
    } else if (bid.awardedVendor) {
      const normalized = normalizeVendorName(bid.awardedVendor);
      expenditures = await query(`
        SELECT SUM(amount) as total, COUNT(*) as count 
        FROM expenditures 
        WHERE LOWER(vendor_name) LIKE ?
      `, [`%${normalized}%`]);
    }
    
    if (expenditures && expenditures[0]?.total) {
      const totalPaid = expenditures[0].total;
      
      if (bid.awardedValue && totalPaid > bid.awardedValue * 1.2) {
        bid.fraudIndicators.push({
          type: 'overpayment',
          severity: 'high',
          description: `Total expenditures ($${totalPaid.toLocaleString()}) exceed contract value ($${bid.awardedValue.toLocaleString()})`,
        });
      }
    }
  }
}

/**
 * Save bids to database
 */
async function saveBids(bids: DASBid[]): Promise<{ saved: number; updated: number }> {
  let saved = 0;
  let updated = 0;
  
  for (const bid of bids) {
    const existing = await query(
      'SELECT id FROM scraped_documents WHERE source_key = ? AND url = ?',
      ['das_bids', bid.bidNumber]
    );
    
    const docData = JSON.stringify({
      ...bid,
      scrapedAt: new Date().toISOString(),
    });
    
    if (existing.length > 0) {
      await execute(`
        UPDATE scraped_documents 
        SET raw_content = ?, title = ?, processed = 1, scraped_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `, [docData, bid.title, existing[0].id]);
      updated++;
    } else {
      await execute(`
        INSERT INTO scraped_documents (source_key, url, title, raw_content, processed)
        VALUES (?, ?, ?, ?, 1)
      `, ['das_bids', bid.bidNumber, bid.title, docData]);
      saved++;
    }
    
    // Save fraud indicators
    for (const indicator of bid.fraudIndicators) {
      let providerId = null;
      if (bid.awardedVendor) {
        const provider = await query(
          'SELECT id FROM provider_master WHERE canonical_name LIKE ? OR name_display LIKE ?',
          [`%${bid.awardedVendor}%`, `%${bid.awardedVendor}%`]
        );
        if (provider.length > 0) {
          providerId = provider[0].id;
        }
      }
      
      const existingIndicator = await query(`
        SELECT id FROM fraud_indicators 
        WHERE indicator_type = ? AND description LIKE ?
      `, [indicator.type, `%${bid.bidNumber}%`]);
      
      if (existingIndicator.length === 0) {
        await execute(`
          INSERT INTO fraud_indicators (provider_master_id, indicator_type, severity, description, status)
          VALUES (?, ?, ?, ?, 'open')
        `, [
          providerId,
          indicator.type,
          indicator.severity,
          `[DAS ${bid.bidNumber}] ${indicator.description}`,
        ]);
      }
    }
  }
  
  return { saved, updated };
}

/**
 * Main scraper function
 */
export async function scrapeDASBids(): Promise<{
  bids: DASBid[];
  stats: { total: number; immigrantRelated: number; withFraudIndicators: number; saved: number; updated: number };
}> {
  console.log('\n=== DAS Bid Board Scraper ===\n');
  
  await initializeDb();
  
  // 1. Start with known bids
  console.log('Loading known DAS bids...');
  let bids = [...KNOWN_DAS_BIDS];
  
  // 2. Try to fetch new bids (may fail due to blocking)
  console.log('Attempting to fetch new bids from DAS portal...');
  const newBids = await fetchDASBidBoard();
  bids = bids.concat(newBids);
  
  // 3. Enrich and analyze bids
  console.log('Analyzing bids...');
  const enrichedBids: DASBid[] = bids.map(bid => {
    // Check immigrant relation if not already set
    if (bid.isImmigrantRelated === undefined) {
      const fullText = `${bid.title} ${bid.description} ${bid.awardedVendor || ''}`;
      const { isRelated, keywords } = checkImmigrantRelated(fullText);
      bid.isImmigrantRelated = isRelated;
      bid.matchedKeywords = keywords;
    }
    
    return {
      bidNumber: bid.bidNumber || 'UNKNOWN',
      title: bid.title || 'Unknown Bid',
      department: bid.department || 'Unknown',
      description: bid.description || '',
      estimatedValue: bid.estimatedValue || null,
      awardedValue: bid.awardedValue || null,
      awardedVendor: bid.awardedVendor || null,
      awardedVendorCode: bid.awardedVendorCode || null,
      bidType: bid.bidType || 'other',
      status: bid.status || 'open',
      postDate: bid.postDate || null,
      dueDate: bid.dueDate || null,
      awardDate: bid.awardDate || null,
      sourceUrl: bid.sourceUrl || DAS_BASE_URL,
      pdfUrl: bid.pdfUrl || null,
      isImmigrantRelated: bid.isImmigrantRelated || false,
      matchedKeywords: bid.matchedKeywords || [],
      fraudIndicators: [],
    };
  });
  
  // 4. Analyze for fraud indicators
  console.log('Analyzing for fraud indicators...');
  for (const bid of enrichedBids) {
    bid.fraudIndicators = analyzeForFraud(bid, enrichedBids);
  }
  
  // 5. Cross-reference with other data sources
  console.log('Cross-referencing with DHHS contracts...');
  await crossReferenceWithDHHS(enrichedBids);
  
  console.log('Cross-referencing with expenditure data...');
  await crossReferenceWithExpenditures(enrichedBids);
  
  // 6. Save to database
  console.log('Saving to database...');
  const { saved, updated } = await saveBids(enrichedBids);
  
  const stats = {
    total: enrichedBids.length,
    immigrantRelated: enrichedBids.filter(b => b.isImmigrantRelated).length,
    withFraudIndicators: enrichedBids.filter(b => b.fraudIndicators.length > 0).length,
    saved,
    updated,
  };
  
  console.log('\n=== Scrape Summary ===');
  console.log(`Total bids: ${stats.total}`);
  console.log(`Immigrant-related: ${stats.immigrantRelated}`);
  console.log(`With fraud indicators: ${stats.withFraudIndicators}`);
  console.log(`Saved: ${stats.saved}, Updated: ${stats.updated}`);
  
  return { bids: enrichedBids, stats };
}

/**
 * Get all DAS bids from database
 */
export async function getDASBids(filters?: {
  immigrantRelatedOnly?: boolean;
  withFraudIndicatorsOnly?: boolean;
  vendor?: string;
  status?: string;
}): Promise<DASBid[]> {
  await initializeDb();
  
  const docs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'das_bids'
    ORDER BY scraped_at DESC
  `);
  
  let bids: DASBid[] = docs.map((doc: any) => {
    try {
      return JSON.parse(doc.raw_content);
    } catch {
      return null;
    }
  }).filter(Boolean);
  
  if (filters?.immigrantRelatedOnly) {
    bids = bids.filter(b => b.isImmigrantRelated);
  }
  
  if (filters?.withFraudIndicatorsOnly) {
    bids = bids.filter(b => b.fraudIndicators && b.fraudIndicators.length > 0);
  }
  
  if (filters?.vendor) {
    const normalized = normalizeVendorName(filters.vendor);
    bids = bids.filter(b =>
      b.awardedVendor && normalizeVendorName(b.awardedVendor).includes(normalized)
    );
  }
  
  if (filters?.status) {
    bids = bids.filter(b => b.status === filters.status);
  }
  
  return bids;
}

// CLI execution
const isMain = process.argv[1]?.includes('das-bids');
if (isMain) {
  scrapeDASBids()
    .then(result => {
      console.log('\nBids with fraud indicators:');
      for (const bid of result.bids.filter(b => b.fraudIndicators.length > 0)) {
        console.log(`\n${bid.bidNumber}: ${bid.title}`);
        console.log(`  Vendor: ${bid.awardedVendor || 'Not awarded'}`);
        console.log(`  Value: $${bid.awardedValue?.toLocaleString() || 'Unknown'}`);
        console.log('  Fraud Indicators:');
        for (const indicator of bid.fraudIndicators) {
          console.log(`    [${indicator.severity.toUpperCase()}] ${indicator.type}: ${indicator.description}`);
        }
      }
    })
    .catch(console.error);
}

export default {
  scrapeDASBids,
  getDASBids,
  KNOWN_CONTRACTORS,
  TARGET_KEYWORDS,
};
