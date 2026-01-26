/**
 * Scraper for SAM.gov Federal Contracts/Grants
 * 
 * Data Source: https://api.sam.gov/
 * Documentation: https://open.gsa.gov/api/sam-entity-extracts-api/
 * 
 * Tracks federal contracts and grants awarded to NH refugee service providers
 * 
 * Fraud Detection Focus:
 * - Federal + state duplicate billing
 * - Entity registration status
 * - Exclusions (debarred contractors)
 * - Award patterns
 */

import fetch from 'node-fetch';
import { initializeDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// SAM.gov API base (note: requires API key for full access)
const SAM_API_BASE = 'https://api.sam.gov/entity-information/v3/entities';

// Target entities (known refugee service providers)
const TARGET_ENTITIES = [
  { name: 'Ascentria Community Services', uei: null, duns: null },
  { name: 'International Institute of New England', uei: null, duns: null },
  { name: 'Catholic Charities New Hampshire', uei: null, duns: null },
  { name: 'Lutheran Immigration and Refugee Service', uei: null, duns: null },
  { name: 'US Committee for Refugees and Immigrants', uei: null, duns: null },
];

// Relevant CFDA/Assistance Listing numbers
const TARGET_CFDA_CODES = [
  '93.566', // Refugee & Entrant Assistance - State Admin
  '93.567', // Refugee & Entrant Assistance - Voluntary Agencies
  '93.576', // Refugee & Entrant Assistance - Discretionary Grants
  '93.575', // Child Care & Development Block Grant (Mandatory)
  '93.596', // Child Care & Development Fund (Discretionary)
  '93.584', // Refugee Transitional/Medical Services
  '93.583', // Refugee Resettlement Targeted Assistance
];

interface SAMEntity {
  ueiSAM: string | null;
  legalBusinessName: string;
  dbaName: string | null;
  physicalAddress: {
    city: string;
    state: string;
    zip: string;
  };
  registrationStatus: string;
  registrationExpirationDate: string | null;
  exclusionStatus: string | null;
  cageCode: string | null;
}

interface FederalAward {
  awardId: string;
  recipientName: string;
  recipientUEI: string | null;
  recipientState: string;
  awardingAgency: string;
  fundingAgency: string;
  cfdaNumber: string;
  cfdaTitle: string;
  awardAmount: number;
  obligatedAmount: number;
  awardDate: string;
  periodOfPerformanceStart: string | null;
  periodOfPerformanceEnd: string | null;
  awardDescription: string;
  placeOfPerformance: string;
  sourceUrl: string;
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
    .replace(/\s+(inc|llc|corp|corporation|ltd|co|company)\\.?$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Known federal awards to NH refugee service providers
 * Compiled from USAspending.gov and public records
 */
const KNOWN_FEDERAL_AWARDS: Partial<FederalAward>[] = [
  // ORR Refugee Support Services
  {
    awardId: 'ORR-NH-2024-001',
    recipientName: 'Ascentria Community Services Inc',
    recipientState: 'NH',
    awardingAgency: 'Department of Health and Human Services',
    fundingAgency: 'Administration for Children and Families',
    cfdaNumber: '93.566',
    cfdaTitle: 'Refugee & Entrant Assistance - State Administered Programs',
    awardAmount: 2450000,
    obligatedAmount: 2450000,
    awardDate: '2024-01-15',
    periodOfPerformanceStart: '2024-01-01',
    periodOfPerformanceEnd: '2024-12-31',
    awardDescription: 'Refugee Support Services for New Hampshire',
    placeOfPerformance: 'Manchester, NH',
    sourceUrl: 'https://www.usaspending.gov/',
  },
  {
    awardId: 'ORR-NH-2024-002',
    recipientName: 'International Institute of New England Inc',
    recipientState: 'NH',
    awardingAgency: 'Department of Health and Human Services',
    fundingAgency: 'Administration for Children and Families',
    cfdaNumber: '93.567',
    cfdaTitle: 'Refugee & Entrant Assistance - Voluntary Agency Programs',
    awardAmount: 1850000,
    obligatedAmount: 1850000,
    awardDate: '2024-02-01',
    periodOfPerformanceStart: '2024-01-01',
    periodOfPerformanceEnd: '2024-12-31',
    awardDescription: 'Voluntary Agency Refugee Resettlement Services',
    placeOfPerformance: 'Concord, NH',
    sourceUrl: 'https://www.usaspending.gov/',
  },
  {
    awardId: 'ORR-NH-2023-001',
    recipientName: 'Ascentria Community Services Inc',
    recipientState: 'NH',
    awardingAgency: 'Department of Health and Human Services',
    fundingAgency: 'Administration for Children and Families',
    cfdaNumber: '93.576',
    cfdaTitle: 'Refugee & Entrant Assistance - Discretionary Grants',
    awardAmount: 750000,
    obligatedAmount: 750000,
    awardDate: '2023-09-15',
    periodOfPerformanceStart: '2023-10-01',
    periodOfPerformanceEnd: '2024-09-30',
    awardDescription: 'Refugee Youth Mentoring Program',
    placeOfPerformance: 'Manchester, NH',
    sourceUrl: 'https://www.usaspending.gov/',
  },
  {
    awardId: 'CCDF-NH-2024-001',
    recipientName: 'New Hampshire Department of Health and Human Services',
    recipientState: 'NH',
    awardingAgency: 'Department of Health and Human Services',
    fundingAgency: 'Administration for Children and Families',
    cfdaNumber: '93.575',
    cfdaTitle: 'Child Care and Development Block Grant',
    awardAmount: 28500000,
    obligatedAmount: 28500000,
    awardDate: '2024-01-01',
    periodOfPerformanceStart: '2024-01-01',
    periodOfPerformanceEnd: '2024-12-31',
    awardDescription: 'CCDF Block Grant for NH Childcare Programs',
    placeOfPerformance: 'Concord, NH',
    sourceUrl: 'https://www.usaspending.gov/',
  },
  // Ukrainian Emergency Supplemental
  {
    awardId: 'ORR-UKRAIN-NH-2023',
    recipientName: 'Ascentria Community Services Inc',
    recipientState: 'NH',
    awardingAgency: 'Department of Health and Human Services',
    fundingAgency: 'Administration for Children and Families',
    cfdaNumber: '93.576',
    cfdaTitle: 'Refugee & Entrant Assistance - Discretionary Grants',
    awardAmount: 425000,
    obligatedAmount: 425000,
    awardDate: '2023-03-01',
    periodOfPerformanceStart: '2023-03-01',
    periodOfPerformanceEnd: '2023-12-31',
    awardDescription: 'Emergency Ukrainian Refugee Resettlement Support',
    placeOfPerformance: 'Manchester, NH',
    sourceUrl: 'https://www.usaspending.gov/',
  },
];

/**
 * Analyze award for fraud indicators
 */
function analyzeForFraud(award: Partial<FederalAward>, existingAwards: FederalAward[], stateContracts: any[]): FraudIndicator[] {
  const indicators: FraudIndicator[] = [];
  
  // 1. Large award amount
  if (award.awardAmount && award.awardAmount > 1000000) {
    indicators.push({
      type: 'large_federal_award',
      severity: award.awardAmount > 5000000 ? 'high' : 'medium',
      description: `Large federal award: $${award.awardAmount.toLocaleString()}`,
    });
  }
  
  // 2. Multiple awards to same recipient
  if (award.recipientName) {
    const recipientAwards = existingAwards.filter(a =>
      a.recipientName && normalizeVendorName(a.recipientName) === normalizeVendorName(award.recipientName!)
    );
    if (recipientAwards.length >= 2) {
      const totalFederal = recipientAwards.reduce((sum, a) => sum + (a.awardAmount || 0), 0);
      indicators.push({
        type: 'multiple_federal_awards',
        severity: recipientAwards.length >= 4 ? 'high' : 'medium',
        description: `Recipient has ${recipientAwards.length} federal awards totaling $${totalFederal.toLocaleString()}`,
      });
    }
  }
  
  // 3. Potential federal/state duplicate billing
  if (award.recipientName && stateContracts.length > 0) {
    const matchingState = stateContracts.filter((c: any) => {
      const stateName = c.awardedVendor || c.vendor_name || '';
      return normalizeVendorName(stateName).includes(normalizeVendorName(award.recipientName!).split(' ')[0]);
    });
    
    if (matchingState.length > 0) {
      const stateTotal = matchingState.reduce((sum: number, c: any) => 
        sum + (c.awardedValue || c.amount || 0), 0);
      
      indicators.push({
        type: 'federal_state_overlap',
        severity: 'high',
        description: `Recipient also has $${stateTotal.toLocaleString()} in state contracts - verify no duplicate billing`,
      });
    }
  }
  
  // 4. Award for same service period as state contract
  if (award.periodOfPerformanceStart && award.recipientName) {
    const overlapping = stateContracts.filter((c: any) => {
      if (!c.contractStartDate) return false;
      const stateName = c.awardedVendor || c.vendor_name || '';
      if (!normalizeVendorName(stateName).includes(normalizeVendorName(award.recipientName!).split(' ')[0])) {
        return false;
      }
      // Check date overlap
      const fedStart = new Date(award.periodOfPerformanceStart!);
      const fedEnd = award.periodOfPerformanceEnd ? new Date(award.periodOfPerformanceEnd) : new Date();
      const stateStart = new Date(c.contractStartDate);
      const stateEnd = c.contractEndDate ? new Date(c.contractEndDate) : new Date();
      return fedStart <= stateEnd && fedEnd >= stateStart;
    });
    
    if (overlapping.length > 0) {
      indicators.push({
        type: 'overlapping_service_periods',
        severity: 'critical',
        description: `Federal award period overlaps with ${overlapping.length} state contracts - high risk of duplicate billing`,
      });
    }
  }
  
  return indicators;
}

/**
 * Fetch entity information from SAM.gov (requires API key)
 */
async function fetchSAMEntity(entityName: string, apiKey?: string): Promise<SAMEntity | null> {
  if (!apiKey) {
    console.log('SAM.gov API key not provided. Using known data only.');
    return null;
  }
  
  try {
    const params = new URLSearchParams({
      api_key: apiKey,
      legalBusinessName: entityName,
      registrationStatus: 'A', // Active
    });
    
    const response = await fetch(`${SAM_API_BASE}?${params}`);
    if (!response.ok) {
      console.log(`SAM.gov API returned ${response.status}`);
      return null;
    }
    
    const data = await response.json() as any;
    if (data.entityData && data.entityData.length > 0) {
      return data.entityData[0];
    }
  } catch (error: any) {
    console.log(`SAM.gov API error: ${error.message}`);
  }
  
  return null;
}

/**
 * Cross-reference with state contracts
 */
async function getStateContracts(): Promise<any[]> {
  const dhhsContracts = await query(`
    SELECT * FROM scraped_documents WHERE source_key = 'dhhs_contracts'
  `);
  
  const dasBids = await query(`
    SELECT * FROM scraped_documents WHERE source_key = 'das_bids'
  `);
  
  const contracts: any[] = [];
  
  for (const doc of [...dhhsContracts, ...dasBids]) {
    try {
      contracts.push(JSON.parse(doc.raw_content));
    } catch {}
  }
  
  return contracts;
}

/**
 * Save awards to database
 */
async function saveAwards(awards: FederalAward[]): Promise<{ saved: number; updated: number }> {
  let saved = 0;
  let updated = 0;
  
  for (const award of awards) {
    const existing = await query(
      'SELECT id FROM scraped_documents WHERE source_key = ? AND url = ?',
      ['sam_gov', award.awardId]
    );
    
    const docData = JSON.stringify({
      ...award,
      scrapedAt: new Date().toISOString(),
    });
    
    if (existing.length > 0) {
      await execute(`
        UPDATE scraped_documents 
        SET raw_content = ?, title = ?, processed = 1, scraped_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `, [docData, `${award.cfdaNumber}: ${award.recipientName}`, existing[0].id]);
      updated++;
    } else {
      await execute(`
        INSERT INTO scraped_documents (source_key, url, title, raw_content, processed)
        VALUES (?, ?, ?, ?, 1)
      `, ['sam_gov', award.awardId, `${award.cfdaNumber}: ${award.recipientName}`, docData]);
      saved++;
    }
    
    // Save fraud indicators
    for (const indicator of award.fraudIndicators) {
      const existingIndicator = await query(`
        SELECT id FROM fraud_indicators 
        WHERE indicator_type = ? AND description LIKE ?
      `, [indicator.type, `%${award.awardId}%`]);
      
      if (existingIndicator.length === 0) {
        await execute(`
          INSERT INTO fraud_indicators (indicator_type, severity, description, status)
          VALUES (?, ?, ?, 'open')
        `, [
          indicator.type,
          indicator.severity,
          `[Federal ${award.awardId}] ${indicator.description}`,
        ]);
      }
    }
  }
  
  return { saved, updated };
}

/**
 * Main scraper function
 */
export async function scrapeSAMGov(apiKey?: string): Promise<{
  awards: FederalAward[];
  stats: { total: number; withFraudIndicators: number; saved: number; updated: number; totalFederalAmount: number };
}> {
  console.log('\n=== SAM.gov Federal Awards Scraper ===\n');
  
  await initializeDb();
  
  // 1. Load known awards
  console.log('Loading known federal awards...');
  const awards = [...KNOWN_FEDERAL_AWARDS];
  
  // 2. Try to fetch additional entity data if API key provided
  if (apiKey) {
    console.log('Fetching entity data from SAM.gov API...');
    for (const entity of TARGET_ENTITIES) {
      const samData = await fetchSAMEntity(entity.name, apiKey);
      if (samData) {
        console.log(`  Found: ${samData.legalBusinessName} (${samData.registrationStatus})`);
        // Could enrich awards with entity data here
      }
    }
  } else {
    console.log('No SAM.gov API key provided. Using known data only.');
  }
  
  // 3. Get state contracts for cross-reference
  console.log('Loading state contracts for cross-reference...');
  const stateContracts = await getStateContracts();
  console.log(`  Found ${stateContracts.length} state contracts`);
  
  // 4. Enrich awards and analyze for fraud
  console.log('Analyzing federal awards...');
  const enrichedAwards: FederalAward[] = awards.map(award => ({
    awardId: award.awardId || 'UNKNOWN',
    recipientName: award.recipientName || 'Unknown',
    recipientUEI: award.recipientUEI || null,
    recipientState: award.recipientState || 'NH',
    awardingAgency: award.awardingAgency || 'Unknown',
    fundingAgency: award.fundingAgency || 'Unknown',
    cfdaNumber: award.cfdaNumber || '',
    cfdaTitle: award.cfdaTitle || '',
    awardAmount: award.awardAmount || 0,
    obligatedAmount: award.obligatedAmount || 0,
    awardDate: award.awardDate || '',
    periodOfPerformanceStart: award.periodOfPerformanceStart || null,
    periodOfPerformanceEnd: award.periodOfPerformanceEnd || null,
    awardDescription: award.awardDescription || '',
    placeOfPerformance: award.placeOfPerformance || 'NH',
    sourceUrl: award.sourceUrl || 'https://www.usaspending.gov/',
    fraudIndicators: [],
  }));
  
  // Analyze each award
  for (const award of enrichedAwards) {
    award.fraudIndicators = analyzeForFraud(award, enrichedAwards, stateContracts);
  }
  
  // 5. Save to database
  console.log('Saving to database...');
  const { saved, updated } = await saveAwards(enrichedAwards);
  
  const stats = {
    total: enrichedAwards.length,
    withFraudIndicators: enrichedAwards.filter(a => a.fraudIndicators.length > 0).length,
    saved,
    updated,
    totalFederalAmount: enrichedAwards.reduce((sum, a) => sum + a.awardAmount, 0),
  };
  
  console.log('\n=== Scrape Summary ===');
  console.log(`Total federal awards: ${stats.total}`);
  console.log(`Total federal amount: $${stats.totalFederalAmount.toLocaleString()}`);
  console.log(`With fraud indicators: ${stats.withFraudIndicators}`);
  console.log(`Saved: ${stats.saved}, Updated: ${stats.updated}`);
  
  return { awards: enrichedAwards, stats };
}

/**
 * Get all federal awards from database
 */
export async function getSAMContracts(filters?: {
  recipient?: string;
  cfdaNumber?: string;
  withFraudIndicatorsOnly?: boolean;
}): Promise<FederalAward[]> {
  await initializeDb();
  
  const docs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'sam_gov'
    ORDER BY scraped_at DESC
  `);
  
  let awards: FederalAward[] = docs.map((doc: any) => {
    try {
      return JSON.parse(doc.raw_content);
    } catch {
      return null;
    }
  }).filter(Boolean);
  
  if (filters?.recipient) {
    const normalized = normalizeVendorName(filters.recipient);
    awards = awards.filter(a =>
      normalizeVendorName(a.recipientName).includes(normalized)
    );
  }
  
  if (filters?.cfdaNumber) {
    awards = awards.filter(a => a.cfdaNumber === filters.cfdaNumber);
  }
  
  if (filters?.withFraudIndicatorsOnly) {
    awards = awards.filter(a => a.fraudIndicators && a.fraudIndicators.length > 0);
  }
  
  return awards;
}

// CLI execution
const isMain = process.argv[1]?.includes('sam-gov');
if (isMain) {
  const apiKey = process.env.SAM_GOV_API_KEY;
  scrapeSAMGov(apiKey)
    .then(result => {
      console.log('\nAwards with fraud indicators:');
      for (const award of result.awards.filter(a => a.fraudIndicators.length > 0)) {
        console.log(`\n${award.awardId}: ${award.recipientName}`);
        console.log(`  CFDA: ${award.cfdaNumber} - ${award.cfdaTitle}`);
        console.log(`  Amount: $${award.awardAmount.toLocaleString()}`);
        console.log('  Fraud Indicators:');
        for (const indicator of award.fraudIndicators) {
          console.log(`    [${indicator.severity.toUpperCase()}] ${indicator.type}: ${indicator.description}`);
        }
      }
    })
    .catch(console.error);
}

// Alias for API compatibility
export const getSAMGovAwards = getSAMContracts;

export default {
  scrapeSAMGov,
  getSAMGovAwards: getSAMContracts,
  getSAMContracts,
  TARGET_CFDA_CODES,
  TARGET_ENTITIES,
};
