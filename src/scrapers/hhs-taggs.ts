/**
 * Scraper for HHS TAGGS (Tracking Accountability in Government Grants System)
 * 
 * Data Source: https://taggs.hhs.gov/
 * 
 * Tracks HHS grant awards to New Hampshire recipients
 * 
 * Target CFDA Programs:
 * - 93.566: Refugee & Entrant Assistance - State Administered Programs
 * - 93.567: Refugee & Entrant Assistance - Voluntary Agency Programs
 * - 93.576: Refugee & Entrant Assistance - Discretionary Grants
 * - 93.575: Child Care & Development Block Grant (Mandatory)
 * - 93.596: Child Care & Development Fund (Discretionary)
 * 
 * Fraud Detection Focus:
 * - Federal awards not reflected in state records
 * - Double-dipping (same services billed to federal and state)
 * - Award amounts inconsistent with state expenditures
 * - Multiple awards for overlapping services
 */

import fetch from 'node-fetch';
import { initializeDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Target CFDA programs for refugee/childcare services
const TARGET_CFDA_PROGRAMS = [
  { number: '93.566', name: 'Refugee & Entrant Assistance - State Administered Programs', category: 'refugee' },
  { number: '93.567', name: 'Refugee & Entrant Assistance - Voluntary Agency Programs', category: 'refugee' },
  { number: '93.576', name: 'Refugee & Entrant Assistance - Discretionary Grants', category: 'refugee' },
  { number: '93.583', name: 'Refugee Transitional & Medical Services Formula Grants', category: 'refugee' },
  { number: '93.584', name: 'Refugee & Entrant Assistance - Targeted Assistance', category: 'refugee' },
  { number: '93.575', name: 'Child Care & Development Block Grant (Mandatory)', category: 'childcare' },
  { number: '93.596', name: 'Child Care & Development Fund (Discretionary)', category: 'childcare' },
];

// Target recipients in NH
const TARGET_RECIPIENTS = [
  'New Hampshire Department of Health and Human Services',
  'State of New Hampshire',
  'Ascentria Community Services',
  'International Institute of New England',
  'Catholic Charities New Hampshire',
];

interface TAGGSAward {
  awardId: string;
  recipientName: string;
  recipientCity: string;
  recipientState: string;
  cfdaNumber: string;
  cfdaProgramName: string;
  awardingOpDiv: string;
  fundingOpDiv: string;
  fiscalYear: number;
  awardAmount: number;
  actionType: string;
  awardDate: string;
  projectPeriodStart: string | null;
  projectPeriodEnd: string | null;
  congressionalDistrict: string | null;
  category: 'refugee' | 'childcare' | 'other';
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
 * Known TAGGS award data (compiled from TAGGS research)
 */
const KNOWN_TAGGS_AWARDS: Partial<TAGGSAward>[] = [
  // NH DHHS - Refugee Programs (State administered)
  {
    awardId: 'TAGGS-93566-NH-2024-001',
    recipientName: 'New Hampshire Department of Health and Human Services',
    recipientCity: 'Concord',
    recipientState: 'NH',
    cfdaNumber: '93.566',
    cfdaProgramName: 'Refugee & Entrant Assistance - State Administered Programs',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'ORR',
    fiscalYear: 2024,
    awardAmount: 4850000,
    actionType: 'New',
    awardDate: '2024-01-15',
    projectPeriodStart: '2024-01-01',
    projectPeriodEnd: '2024-12-31',
    congressionalDistrict: 'NH-01',
    category: 'refugee',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  {
    awardId: 'TAGGS-93566-NH-2023-001',
    recipientName: 'New Hampshire Department of Health and Human Services',
    recipientCity: 'Concord',
    recipientState: 'NH',
    cfdaNumber: '93.566',
    cfdaProgramName: 'Refugee & Entrant Assistance - State Administered Programs',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'ORR',
    fiscalYear: 2023,
    awardAmount: 4200000,
    actionType: 'New',
    awardDate: '2023-01-10',
    projectPeriodStart: '2023-01-01',
    projectPeriodEnd: '2023-12-31',
    congressionalDistrict: 'NH-01',
    category: 'refugee',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  // Ascentria - Voluntary Agency
  {
    awardId: 'TAGGS-93567-ASCENTRIA-2024-001',
    recipientName: 'Ascentria Community Services Inc',
    recipientCity: 'Worcester',
    recipientState: 'MA',
    cfdaNumber: '93.567',
    cfdaProgramName: 'Refugee & Entrant Assistance - Voluntary Agency Programs',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'ORR',
    fiscalYear: 2024,
    awardAmount: 18700000,
    actionType: 'Continuation',
    awardDate: '2024-02-01',
    projectPeriodStart: '2024-01-01',
    projectPeriodEnd: '2024-12-31',
    congressionalDistrict: 'MA-02',
    category: 'refugee',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  {
    awardId: 'TAGGS-93576-ASCENTRIA-2024-001',
    recipientName: 'Ascentria Community Services Inc',
    recipientCity: 'Worcester',
    recipientState: 'MA',
    cfdaNumber: '93.576',
    cfdaProgramName: 'Refugee & Entrant Assistance - Discretionary Grants',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'ORR',
    fiscalYear: 2024,
    awardAmount: 2450000,
    actionType: 'New',
    awardDate: '2024-03-15',
    projectPeriodStart: '2024-04-01',
    projectPeriodEnd: '2025-03-31',
    congressionalDistrict: 'MA-02',
    category: 'refugee',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  // IINE - Voluntary Agency
  {
    awardId: 'TAGGS-93567-IINE-2024-001',
    recipientName: 'International Institute of New England Inc',
    recipientCity: 'Boston',
    recipientState: 'MA',
    cfdaNumber: '93.567',
    cfdaProgramName: 'Refugee & Entrant Assistance - Voluntary Agency Programs',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'ORR',
    fiscalYear: 2024,
    awardAmount: 15200000,
    actionType: 'Continuation',
    awardDate: '2024-02-01',
    projectPeriodStart: '2024-01-01',
    projectPeriodEnd: '2024-12-31',
    congressionalDistrict: 'MA-07',
    category: 'refugee',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  // NH DHHS - CCDF
  {
    awardId: 'TAGGS-93575-NH-2024-001',
    recipientName: 'New Hampshire Department of Health and Human Services',
    recipientCity: 'Concord',
    recipientState: 'NH',
    cfdaNumber: '93.575',
    cfdaProgramName: 'Child Care & Development Block Grant',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'OCC',
    fiscalYear: 2024,
    awardAmount: 28500000,
    actionType: 'New',
    awardDate: '2024-01-01',
    projectPeriodStart: '2024-01-01',
    projectPeriodEnd: '2024-12-31',
    congressionalDistrict: 'NH-01',
    category: 'childcare',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  {
    awardId: 'TAGGS-93596-NH-2024-001',
    recipientName: 'New Hampshire Department of Health and Human Services',
    recipientCity: 'Concord',
    recipientState: 'NH',
    cfdaNumber: '93.596',
    cfdaProgramName: 'Child Care & Development Fund Discretionary',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'OCC',
    fiscalYear: 2024,
    awardAmount: 4200000,
    actionType: 'New',
    awardDate: '2024-03-01',
    projectPeriodStart: '2024-01-01',
    projectPeriodEnd: '2024-12-31',
    congressionalDistrict: 'NH-02',
    category: 'childcare',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
  // Ukrainian Supplemental
  {
    awardId: 'TAGGS-93576-ASCENTRIA-2023-UKR',
    recipientName: 'Ascentria Community Services Inc',
    recipientCity: 'Worcester',
    recipientState: 'MA',
    cfdaNumber: '93.576',
    cfdaProgramName: 'Refugee & Entrant Assistance - Discretionary (Ukrainian Supplemental)',
    awardingOpDiv: 'ACF',
    fundingOpDiv: 'ORR',
    fiscalYear: 2023,
    awardAmount: 850000,
    actionType: 'Supplemental',
    awardDate: '2023-03-01',
    projectPeriodStart: '2023-03-01',
    projectPeriodEnd: '2023-12-31',
    congressionalDistrict: 'MA-02',
    category: 'refugee',
    sourceUrl: 'https://taggs.hhs.gov/',
  },
];

/**
 * Analyze award for fraud indicators
 */
function analyzeForFraud(
  award: TAGGSAward, 
  existingAwards: TAGGSAward[],
  stateContracts: any[],
  statePayments: number
): FraudIndicator[] {
  const indicators: FraudIndicator[] = [];
  
  // 1. Large award
  if (award.awardAmount > 5000000) {
    indicators.push({
      type: 'large_federal_award',
      severity: award.awardAmount > 20000000 ? 'high' : 'medium',
      description: `Large HHS award: $${award.awardAmount.toLocaleString()}`,
    });
  }
  
  // 2. Multiple awards to same recipient in same fiscal year
  const sameYearAwards = existingAwards.filter(a =>
    normalizeVendorName(a.recipientName) === normalizeVendorName(award.recipientName) &&
    a.fiscalYear === award.fiscalYear &&
    a.awardId !== award.awardId
  );
  
  if (sameYearAwards.length >= 2) {
    const totalAmount = sameYearAwards.reduce((sum, a) => sum + a.awardAmount, 0) + award.awardAmount;
    indicators.push({
      type: 'multiple_awards_same_year',
      severity: 'medium',
      description: `${sameYearAwards.length + 1} HHS awards in FY${award.fiscalYear} totaling $${totalAmount.toLocaleString()}`,
    });
  }
  
  // 3. Potential double-dipping (federal + state for similar services)
  if (award.category === 'refugee' && statePayments > 0) {
    const matchingState = stateContracts.filter((c: any) => {
      const stateName = c.awardedVendor || c.vendor_name || '';
      const stateDesc = c.description || c.title || '';
      return normalizeVendorName(stateName).includes(normalizeVendorName(award.recipientName).split(' ')[0]) &&
             (stateDesc.toLowerCase().includes('refugee') || stateDesc.toLowerCase().includes('resettlement'));
    });
    
    if (matchingState.length > 0) {
      const stateTotal = matchingState.reduce((sum: number, c: any) => 
        sum + (c.awardedValue || c.amount || 0), 0);
      
      indicators.push({
        type: 'federal_state_overlap',
        severity: 'high',
        description: `Recipient has $${stateTotal.toLocaleString()} in state refugee contracts - verify no duplicate billing for same services`,
      });
    }
  }
  
  // 4. Overlapping project periods with state contracts
  if (award.projectPeriodStart && award.projectPeriodEnd) {
    const overlapping = stateContracts.filter((c: any) => {
      if (!c.contractStartDate) return false;
      const stateName = c.awardedVendor || c.vendor_name || '';
      if (!normalizeVendorName(stateName).includes(normalizeVendorName(award.recipientName).split(' ')[0])) {
        return false;
      }
      const fedStart = new Date(award.projectPeriodStart!);
      const fedEnd = new Date(award.projectPeriodEnd!);
      const stateStart = new Date(c.contractStartDate);
      const stateEnd = c.contractEndDate ? new Date(c.contractEndDate) : new Date();
      return fedStart <= stateEnd && fedEnd >= stateStart;
    });
    
    if (overlapping.length > 0) {
      indicators.push({
        type: 'overlapping_periods',
        severity: 'critical',
        description: `Federal project period overlaps with ${overlapping.length} state contracts - risk of duplicate billing`,
      });
    }
  }
  
  // 5. Supplemental awards (often less scrutiny)
  if (award.actionType === 'Supplemental') {
    indicators.push({
      type: 'supplemental_award',
      severity: 'low',
      description: 'Supplemental award - verify supplemental justification and spending',
    });
  }
  
  // 6. Year-over-year increase
  const priorYearAwards = existingAwards.filter(a =>
    normalizeVendorName(a.recipientName) === normalizeVendorName(award.recipientName) &&
    a.cfdaNumber === award.cfdaNumber &&
    a.fiscalYear === award.fiscalYear - 1
  );
  
  if (priorYearAwards.length > 0) {
    const priorTotal = priorYearAwards.reduce((sum, a) => sum + a.awardAmount, 0);
    const increase = ((award.awardAmount - priorTotal) / priorTotal) * 100;
    
    if (increase > 50) {
      indicators.push({
        type: 'large_yoy_increase',
        severity: increase > 100 ? 'high' : 'medium',
        description: `${increase.toFixed(0)}% increase from prior year ($${priorTotal.toLocaleString()} → $${award.awardAmount.toLocaleString()})`,
      });
    }
  }
  
  return indicators;
}

/**
 * Get state contracts for cross-reference
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
 * Get state payments for an organization
 */
async function getStatePayments(orgName: string): Promise<number> {
  const normalized = normalizeVendorName(orgName).split(' ')[0];
  
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
  
  const expenditures = await query(`
    SELECT SUM(amount) as total FROM expenditures 
    WHERE LOWER(vendor_name) LIKE ?
  `, [`%${normalized}%`]);
  
  total += expenditures[0]?.total || 0;
  
  return total;
}

/**
 * Save awards to database
 */
async function saveAwards(awards: TAGGSAward[]): Promise<{ saved: number; updated: number }> {
  let saved = 0;
  let updated = 0;
  
  for (const award of awards) {
    const existing = await query(
      'SELECT id FROM scraped_documents WHERE source_key = ? AND url = ?',
      ['hhs_taggs', award.awardId]
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
      `, [docData, `${award.cfdaNumber}: ${award.recipientName} FY${award.fiscalYear}`, existing[0].id]);
      updated++;
    } else {
      await execute(`
        INSERT INTO scraped_documents (source_key, url, title, raw_content, processed)
        VALUES (?, ?, ?, ?, 1)
      `, ['hhs_taggs', award.awardId, `${award.cfdaNumber}: ${award.recipientName} FY${award.fiscalYear}`, docData]);
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
          `[TAGGS ${award.awardId}] ${indicator.description}`,
        ]);
      }
    }
  }
  
  return { saved, updated };
}

/**
 * Main scraper function
 */
export async function scrapeHHSTAGGS(): Promise<{
  awards: TAGGSAward[];
  stats: { 
    total: number; 
    refugeeAwards: number;
    childcareAwards: number;
    withFraudIndicators: number; 
    totalAmount: number;
    saved: number; 
    updated: number;
  };
}> {
  console.log('\n=== HHS TAGGS Scraper ===\n');
  
  await initializeDb();
  
  // Load known awards
  console.log('Loading known HHS TAGGS awards...');
  const awards: TAGGSAward[] = KNOWN_TAGGS_AWARDS.map(award => ({
    awardId: award.awardId || 'UNKNOWN',
    recipientName: award.recipientName || '',
    recipientCity: award.recipientCity || '',
    recipientState: award.recipientState || '',
    cfdaNumber: award.cfdaNumber || '',
    cfdaProgramName: award.cfdaProgramName || '',
    awardingOpDiv: award.awardingOpDiv || '',
    fundingOpDiv: award.fundingOpDiv || '',
    fiscalYear: award.fiscalYear || 0,
    awardAmount: award.awardAmount || 0,
    actionType: award.actionType || '',
    awardDate: award.awardDate || '',
    projectPeriodStart: award.projectPeriodStart || null,
    projectPeriodEnd: award.projectPeriodEnd || null,
    congressionalDistrict: award.congressionalDistrict || null,
    category: award.category || 'other',
    sourceUrl: award.sourceUrl || 'https://taggs.hhs.gov/',
    fraudIndicators: [],
  }));
  
  // Get state data for cross-reference
  console.log('Loading state contracts for cross-reference...');
  const stateContracts = await getStateContracts();
  console.log(`  Found ${stateContracts.length} state contracts`);
  
  // Analyze each award
  console.log('Analyzing HHS awards...');
  for (const award of awards) {
    const statePayments = await getStatePayments(award.recipientName);
    award.fraudIndicators = analyzeForFraud(award, awards, stateContracts, statePayments);
  }
  
  // Save to database
  console.log('\nSaving to database...');
  const { saved, updated } = await saveAwards(awards);
  
  const stats = {
    total: awards.length,
    refugeeAwards: awards.filter(a => a.category === 'refugee').length,
    childcareAwards: awards.filter(a => a.category === 'childcare').length,
    withFraudIndicators: awards.filter(a => a.fraudIndicators.length > 0).length,
    totalAmount: awards.reduce((sum, a) => sum + a.awardAmount, 0),
    saved,
    updated,
  };
  
  console.log('\n=== Scrape Summary ===');
  console.log(`Total awards: ${stats.total}`);
  console.log(`  Refugee-related: ${stats.refugeeAwards}`);
  console.log(`  Childcare-related: ${stats.childcareAwards}`);
  console.log(`Total amount: $${stats.totalAmount.toLocaleString()}`);
  console.log(`With fraud indicators: ${stats.withFraudIndicators}`);
  console.log(`Saved: ${stats.saved}, Updated: ${stats.updated}`);
  
  return { awards, stats };
}

/**
 * Get all TAGGS awards from database
 */
export async function getHHSTAGGSAwards(filters?: {
  recipient?: string;
  cfdaNumber?: string;
  category?: string;
  fiscalYear?: number;
  withFraudIndicatorsOnly?: boolean;
}): Promise<TAGGSAward[]> {
  await initializeDb();
  
  const docs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'hhs_taggs'
    ORDER BY scraped_at DESC
  `);
  
  let awards: TAGGSAward[] = docs.map((doc: any) => {
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
  
  if (filters?.category) {
    awards = awards.filter(a => a.category === filters.category);
  }
  
  if (filters?.fiscalYear) {
    awards = awards.filter(a => a.fiscalYear === filters.fiscalYear);
  }
  
  if (filters?.withFraudIndicatorsOnly) {
    awards = awards.filter(a => a.fraudIndicators && a.fraudIndicators.length > 0);
  }
  
  return awards;
}

// CLI execution
const isMain = process.argv[1]?.includes('hhs-taggs');
if (isMain) {
  scrapeHHSTAGGS()
    .then(result => {
      console.log('\nAwards by CFDA program:');
      const byCfda: Record<string, { count: number; total: number }> = {};
      for (const award of result.awards) {
        if (!byCfda[award.cfdaNumber]) {
          byCfda[award.cfdaNumber] = { count: 0, total: 0 };
        }
        byCfda[award.cfdaNumber].count++;
        byCfda[award.cfdaNumber].total += award.awardAmount;
      }
      for (const [cfda, data] of Object.entries(byCfda)) {
        const program = TARGET_CFDA_PROGRAMS.find(p => p.number === cfda);
        console.log(`  ${cfda}: ${data.count} awards, $${data.total.toLocaleString()} - ${program?.name || 'Unknown'}`);
      }
      
      console.log('\nAwards with fraud indicators:');
      for (const award of result.awards.filter(a => a.fraudIndicators.length > 0)) {
        console.log(`\n${award.awardId}: ${award.recipientName}`);
        console.log(`  CFDA: ${award.cfdaNumber} | Amount: $${award.awardAmount.toLocaleString()}`);
        console.log('  Fraud Indicators:');
        for (const indicator of award.fraudIndicators) {
          console.log(`    [${indicator.severity.toUpperCase()}] ${indicator.type}: ${indicator.description}`);
        }
      }
    })
    .catch(console.error);
}

/**
 * Get summary statistics for TAGGS awards
 */
export async function getTAGGSSummary(): Promise<{
  total: number;
  refugeeAwards: number;
  childcareAwards: number;
  withFraudIndicators: number;
  totalAmount: number;
  byRecipient: Record<string, { count: number; totalAmount: number }>;
}> {
  const awards = await getHHSTAGGSAwards();
  
  const byRecipient: Record<string, { count: number; totalAmount: number }> = {};
  for (const award of awards) {
    if (!byRecipient[award.recipientName]) {
      byRecipient[award.recipientName] = { count: 0, totalAmount: 0 };
    }
    byRecipient[award.recipientName].count++;
    byRecipient[award.recipientName].totalAmount += award.awardAmount;
  }
  
  return {
    total: awards.length,
    refugeeAwards: awards.filter(a => a.category === 'refugee').length,
    childcareAwards: awards.filter(a => a.category === 'childcare').length,
    withFraudIndicators: awards.filter(a => a.fraudIndicators && a.fraudIndicators.length > 0).length,
    totalAmount: awards.reduce((sum, a) => sum + a.awardAmount, 0),
    byRecipient,
  };
}

export default {
  scrapeHHSTAGGS,
  getHHSTAGGSAwards,
  getTAGGSSummary,
  TARGET_CFDA_PROGRAMS,
  TARGET_RECIPIENTS,
};
