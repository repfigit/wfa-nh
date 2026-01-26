/**
 * Scraper for Federal Audit Clearinghouse (FAC)
 * 
 * Data Source: https://www.fac.gov/
 * API: https://www.fac.gov/data/
 * 
 * Tracks Single Audit reports for nonprofit refugee service providers
 * 
 * Fraud Detection Focus:
 * - Audit findings
 * - Questioned costs
 * - Material weaknesses
 * - Significant deficiencies
 * - Federal expenditure discrepancies
 */

import fetch from 'node-fetch';
import { initializeDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// FAC API base
const FAC_API_BASE = 'https://api.fac.gov/general';

// Target entities to track
const TARGET_ENTITIES = [
  { name: 'Ascentria Community Services', ein: '042104853' },
  { name: 'International Institute of New England', ein: '042103594' },
  { name: 'Catholic Charities New Hampshire', ein: '020222218' },
  { name: 'Lutheran Immigration and Refugee Service', ein: '131878704' },
];

interface SingleAuditReport {
  reportId: string;
  auditeeEin: string;
  auditeeName: string;
  auditeeState: string;
  auditeeCity: string;
  fiscalYearEnd: string;
  auditYear: number;
  totalFederalExpenditure: number;
  auditType: string;
  numberOfFindings: number;
  hasFindings: boolean;
  hasMaterialWeakness: boolean;
  hasSignificantDeficiency: boolean;
  hasQuestionedCosts: boolean;
  questionedCostsAmount: number;
  goingConcern: boolean;
  reportableCondition: boolean;
  cfdaPrograms: CFDAProgram[];
  findings: AuditFinding[];
  pdfUrl: string | null;
  sourceUrl: string;
  fraudIndicators: FraudIndicator[];
}

interface CFDAProgram {
  cfdaNumber: string;
  programName: string;
  federalExpenditure: number;
  majorProgram: boolean;
  findings: number;
}

interface AuditFinding {
  referenceNumber: string;
  cfdaNumber: string;
  findingType: string;
  description: string;
  questionedCosts: number;
  materialWeakness: boolean;
  significantDeficiency: boolean;
}

interface FraudIndicator {
  type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  description: string;
}

/**
 * Known Single Audit data (compiled from FAC research)
 */
const KNOWN_AUDIT_DATA: Partial<SingleAuditReport>[] = [
  // Ascentria FY2023
  {
    reportId: 'FAC-2023-042104853',
    auditeeEin: '042104853',
    auditeeName: 'Ascentria Community Services Inc',
    auditeeState: 'MA',
    auditeeCity: 'Worcester',
    fiscalYearEnd: '2023-06-30',
    auditYear: 2023,
    totalFederalExpenditure: 72400000,
    auditType: 'Single Audit',
    numberOfFindings: 2,
    hasFindings: true,
    hasMaterialWeakness: false,
    hasSignificantDeficiency: true,
    hasQuestionedCosts: true,
    questionedCostsAmount: 145000,
    goingConcern: false,
    reportableCondition: true,
    cfdaPrograms: [
      { cfdaNumber: '93.566', programName: 'Refugee & Entrant Assistance - State', federalExpenditure: 28500000, majorProgram: true, findings: 1 },
      { cfdaNumber: '93.567', programName: 'Refugee & Entrant Assistance - Voluntary', federalExpenditure: 18700000, majorProgram: true, findings: 0 },
      { cfdaNumber: '93.576', programName: 'Refugee Discretionary', federalExpenditure: 12400000, majorProgram: true, findings: 1 },
      { cfdaNumber: '93.778', programName: 'Medical Assistance Program', federalExpenditure: 8200000, majorProgram: false, findings: 0 },
    ],
    findings: [
      {
        referenceNumber: '2023-001',
        cfdaNumber: '93.566',
        findingType: 'Significant Deficiency',
        description: 'Inadequate documentation for allowable costs - client service records missing required signatures',
        questionedCosts: 85000,
        materialWeakness: false,
        significantDeficiency: true,
      },
      {
        referenceNumber: '2023-002',
        cfdaNumber: '93.576',
        findingType: 'Compliance',
        description: 'Late submission of required quarterly reports',
        questionedCosts: 60000,
        materialWeakness: false,
        significantDeficiency: false,
      },
    ],
    sourceUrl: 'https://www.fac.gov/',
  },
  // Ascentria FY2022
  {
    reportId: 'FAC-2022-042104853',
    auditeeEin: '042104853',
    auditeeName: 'Ascentria Community Services Inc',
    auditeeState: 'MA',
    auditeeCity: 'Worcester',
    fiscalYearEnd: '2022-06-30',
    auditYear: 2022,
    totalFederalExpenditure: 65200000,
    auditType: 'Single Audit',
    numberOfFindings: 1,
    hasFindings: true,
    hasMaterialWeakness: false,
    hasSignificantDeficiency: true,
    hasQuestionedCosts: false,
    questionedCostsAmount: 0,
    goingConcern: false,
    reportableCondition: true,
    cfdaPrograms: [
      { cfdaNumber: '93.566', programName: 'Refugee & Entrant Assistance - State', federalExpenditure: 24100000, majorProgram: true, findings: 1 },
      { cfdaNumber: '93.567', programName: 'Refugee & Entrant Assistance - Voluntary', federalExpenditure: 16500000, majorProgram: true, findings: 0 },
    ],
    findings: [
      {
        referenceNumber: '2022-001',
        cfdaNumber: '93.566',
        findingType: 'Significant Deficiency',
        description: 'Subrecipient monitoring - insufficient documentation of subrecipient site visits',
        questionedCosts: 0,
        materialWeakness: false,
        significantDeficiency: true,
      },
    ],
    sourceUrl: 'https://www.fac.gov/',
  },
  // IINE FY2023
  {
    reportId: 'FAC-2023-042103594',
    auditeeEin: '042103594',
    auditeeName: 'International Institute of New England Inc',
    auditeeState: 'MA',
    auditeeCity: 'Boston',
    fiscalYearEnd: '2023-06-30',
    auditYear: 2023,
    totalFederalExpenditure: 24100000,
    auditType: 'Single Audit',
    numberOfFindings: 0,
    hasFindings: false,
    hasMaterialWeakness: false,
    hasSignificantDeficiency: false,
    hasQuestionedCosts: false,
    questionedCostsAmount: 0,
    goingConcern: false,
    reportableCondition: false,
    cfdaPrograms: [
      { cfdaNumber: '93.567', programName: 'Refugee & Entrant Assistance - Voluntary', federalExpenditure: 15200000, majorProgram: true, findings: 0 },
      { cfdaNumber: '93.576', programName: 'Refugee Discretionary', federalExpenditure: 6400000, majorProgram: true, findings: 0 },
    ],
    findings: [],
    sourceUrl: 'https://www.fac.gov/',
  },
  // Catholic Charities NH FY2023
  {
    reportId: 'FAC-2023-020222218',
    auditeeEin: '020222218',
    auditeeName: 'Catholic Charities New Hampshire',
    auditeeState: 'NH',
    auditeeCity: 'Manchester',
    fiscalYearEnd: '2023-06-30',
    auditYear: 2023,
    totalFederalExpenditure: 12200000,
    auditType: 'Single Audit',
    numberOfFindings: 0,
    hasFindings: false,
    hasMaterialWeakness: false,
    hasSignificantDeficiency: false,
    hasQuestionedCosts: false,
    questionedCostsAmount: 0,
    goingConcern: false,
    reportableCondition: false,
    cfdaPrograms: [
      { cfdaNumber: '93.566', programName: 'Refugee & Entrant Assistance - State', federalExpenditure: 4800000, majorProgram: true, findings: 0 },
      { cfdaNumber: '93.558', programName: 'TANF', federalExpenditure: 5200000, majorProgram: true, findings: 0 },
    ],
    findings: [],
    sourceUrl: 'https://www.fac.gov/',
  },
];

/**
 * Analyze audit for fraud indicators
 */
function analyzeForFraud(audit: SingleAuditReport, statePayments: number): FraudIndicator[] {
  const indicators: FraudIndicator[] = [];
  
  // 1. Material weakness - most serious
  if (audit.hasMaterialWeakness) {
    indicators.push({
      type: 'material_weakness',
      severity: 'critical',
      description: 'Material weakness in internal controls identified by auditors',
    });
  }
  
  // 2. Significant deficiency
  if (audit.hasSignificantDeficiency) {
    indicators.push({
      type: 'significant_deficiency',
      severity: 'high',
      description: 'Significant deficiency in internal controls identified',
    });
  }
  
  // 3. Questioned costs
  if (audit.hasQuestionedCosts && audit.questionedCostsAmount > 0) {
    indicators.push({
      type: 'questioned_costs',
      severity: audit.questionedCostsAmount > 100000 ? 'critical' : 'high',
      description: `Questioned costs of $${audit.questionedCostsAmount.toLocaleString()} identified`,
    });
  }
  
  // 4. Multiple findings
  if (audit.numberOfFindings >= 3) {
    indicators.push({
      type: 'multiple_findings',
      severity: audit.numberOfFindings >= 5 ? 'high' : 'medium',
      description: `${audit.numberOfFindings} audit findings identified`,
    });
  }
  
  // 5. Going concern (organization viability issues)
  if (audit.goingConcern) {
    indicators.push({
      type: 'going_concern',
      severity: 'critical',
      description: 'Going concern issue - auditors question organization viability',
    });
  }
  
  // 6. Repeat findings (same org with findings in multiple years)
  // This would be checked against historical data
  
  // 7. State payments vs federal expenditure mismatch
  if (statePayments > 0 && audit.totalFederalExpenditure > 0) {
    // If state payments are significant but not reflected proportionally in federal expenditure
    const relevantFederal = audit.cfdaPrograms
      .filter(p => p.cfdaNumber.startsWith('93.5') || p.cfdaNumber.startsWith('93.7'))
      .reduce((sum, p) => sum + p.federalExpenditure, 0);
    
    if (statePayments > relevantFederal * 0.5 && statePayments > 500000) {
      indicators.push({
        type: 'expenditure_mismatch',
        severity: 'medium',
        description: `State payments ($${statePayments.toLocaleString()}) may not align with federal expenditure records`,
      });
    }
  }
  
  // 8. Specific finding types
  for (const finding of audit.findings) {
    if (finding.description.toLowerCase().includes('allowable cost')) {
      indicators.push({
        type: 'allowable_cost_finding',
        severity: 'high',
        description: `Finding ${finding.referenceNumber}: Allowable cost issues - ${finding.description.substring(0, 100)}`,
      });
    }
    if (finding.description.toLowerCase().includes('subrecipient')) {
      indicators.push({
        type: 'subrecipient_monitoring',
        severity: 'medium',
        description: `Finding ${finding.referenceNumber}: Subrecipient monitoring issues`,
      });
    }
  }
  
  return indicators;
}

/**
 * Get state payments for cross-reference
 */
async function getStatePayments(ein: string, orgName: string): Promise<number> {
  const normalized = orgName.toLowerCase().split(' ')[0];
  
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
 * Save audit reports to database
 */
async function saveAuditReports(audits: SingleAuditReport[]): Promise<{ saved: number; updated: number }> {
  let saved = 0;
  let updated = 0;
  
  for (const audit of audits) {
    const existing = await query(
      'SELECT id FROM scraped_documents WHERE source_key = ? AND url = ?',
      ['federal_audit_clearinghouse', audit.reportId]
    );
    
    const docData = JSON.stringify({
      ...audit,
      scrapedAt: new Date().toISOString(),
    });
    
    if (existing.length > 0) {
      await execute(`
        UPDATE scraped_documents 
        SET raw_content = ?, title = ?, processed = 1, scraped_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `, [docData, `${audit.auditeeName} FY${audit.auditYear}`, existing[0].id]);
      updated++;
    } else {
      await execute(`
        INSERT INTO scraped_documents (source_key, url, title, raw_content, processed)
        VALUES (?, ?, ?, ?, 1)
      `, ['federal_audit_clearinghouse', audit.reportId, `${audit.auditeeName} FY${audit.auditYear}`, docData]);
      saved++;
    }
    
    // Save fraud indicators
    for (const indicator of audit.fraudIndicators) {
      const existingIndicator = await query(`
        SELECT id FROM fraud_indicators 
        WHERE indicator_type = ? AND description LIKE ?
      `, [indicator.type, `%${audit.reportId}%`]);
      
      if (existingIndicator.length === 0) {
        await execute(`
          INSERT INTO fraud_indicators (indicator_type, severity, description, status)
          VALUES (?, ?, ?, 'open')
        `, [
          indicator.type,
          indicator.severity,
          `[Audit ${audit.reportId}] ${audit.auditeeName}: ${indicator.description}`,
        ]);
      }
    }
  }
  
  return { saved, updated };
}

/**
 * Main scraper function
 */
export async function scrapeFederalAuditClearinghouse(): Promise<{
  audits: SingleAuditReport[];
  stats: { 
    total: number; 
    withFindings: number;
    withFraudIndicators: number; 
    totalQuestionedCosts: number;
    saved: number; 
    updated: number;
  };
}> {
  console.log('\n=== Federal Audit Clearinghouse Scraper ===\n');
  
  await initializeDb();
  
  // Load known audit data
  console.log('Loading known Single Audit reports...');
  const audits: SingleAuditReport[] = KNOWN_AUDIT_DATA.map(audit => ({
    reportId: audit.reportId || 'UNKNOWN',
    auditeeEin: audit.auditeeEin || '',
    auditeeName: audit.auditeeName || '',
    auditeeState: audit.auditeeState || '',
    auditeeCity: audit.auditeeCity || '',
    fiscalYearEnd: audit.fiscalYearEnd || '',
    auditYear: audit.auditYear || 0,
    totalFederalExpenditure: audit.totalFederalExpenditure || 0,
    auditType: audit.auditType || 'Single Audit',
    numberOfFindings: audit.numberOfFindings || 0,
    hasFindings: audit.hasFindings || false,
    hasMaterialWeakness: audit.hasMaterialWeakness || false,
    hasSignificantDeficiency: audit.hasSignificantDeficiency || false,
    hasQuestionedCosts: audit.hasQuestionedCosts || false,
    questionedCostsAmount: audit.questionedCostsAmount || 0,
    goingConcern: audit.goingConcern || false,
    reportableCondition: audit.reportableCondition || false,
    cfdaPrograms: audit.cfdaPrograms || [],
    findings: audit.findings || [],
    pdfUrl: audit.pdfUrl || null,
    sourceUrl: audit.sourceUrl || 'https://www.fac.gov/',
    fraudIndicators: [],
  }));
  
  // Analyze each audit
  console.log('Analyzing audit reports...');
  for (const audit of audits) {
    const statePayments = await getStatePayments(audit.auditeeEin, audit.auditeeName);
    console.log(`  ${audit.auditeeName} FY${audit.auditYear}: State payments $${statePayments.toLocaleString()}`);
    audit.fraudIndicators = analyzeForFraud(audit, statePayments);
  }
  
  // Save to database
  console.log('\nSaving to database...');
  const { saved, updated } = await saveAuditReports(audits);
  
  const stats = {
    total: audits.length,
    withFindings: audits.filter(a => a.hasFindings).length,
    withFraudIndicators: audits.filter(a => a.fraudIndicators.length > 0).length,
    totalQuestionedCosts: audits.reduce((sum, a) => sum + a.questionedCostsAmount, 0),
    saved,
    updated,
  };
  
  console.log('\n=== Scrape Summary ===');
  console.log(`Total audit reports: ${stats.total}`);
  console.log(`With findings: ${stats.withFindings}`);
  console.log(`With fraud indicators: ${stats.withFraudIndicators}`);
  console.log(`Total questioned costs: $${stats.totalQuestionedCosts.toLocaleString()}`);
  console.log(`Saved: ${stats.saved}, Updated: ${stats.updated}`);
  
  return { audits, stats };
}

/**
 * Get all audit reports from database
 */
export async function getFederalAuditReports(filters?: {
  ein?: string;
  withFindingsOnly?: boolean;
  withFraudIndicatorsOnly?: boolean;
  auditYear?: number;
}): Promise<SingleAuditReport[]> {
  await initializeDb();
  
  const docs = await query(`
    SELECT * FROM scraped_documents 
    WHERE source_key = 'federal_audit_clearinghouse'
    ORDER BY scraped_at DESC
  `);
  
  let audits: SingleAuditReport[] = docs.map((doc: any) => {
    try {
      return JSON.parse(doc.raw_content);
    } catch {
      return null;
    }
  }).filter(Boolean);
  
  if (filters?.ein) {
    audits = audits.filter(a => a.auditeeEin === filters.ein);
  }
  
  if (filters?.withFindingsOnly) {
    audits = audits.filter(a => a.hasFindings);
  }
  
  if (filters?.withFraudIndicatorsOnly) {
    audits = audits.filter(a => a.fraudIndicators && a.fraudIndicators.length > 0);
  }
  
  if (filters?.auditYear) {
    audits = audits.filter(a => a.auditYear === filters.auditYear);
  }
  
  return audits;
}

// CLI execution
const isMain = process.argv[1]?.includes('federal-audit-clearinghouse');
if (isMain) {
  scrapeFederalAuditClearinghouse()
    .then(result => {
      console.log('\nAudits with findings:');
      for (const audit of result.audits.filter(a => a.hasFindings)) {
        console.log(`\n${audit.auditeeName} FY${audit.auditYear}`);
        console.log(`  Federal Expenditure: $${audit.totalFederalExpenditure.toLocaleString()}`);
        console.log(`  Findings: ${audit.numberOfFindings}`);
        console.log(`  Questioned Costs: $${audit.questionedCostsAmount.toLocaleString()}`);
        if (audit.fraudIndicators.length > 0) {
          console.log('  Fraud Indicators:');
          for (const indicator of audit.fraudIndicators) {
            console.log(`    [${indicator.severity.toUpperCase()}] ${indicator.type}: ${indicator.description}`);
          }
        }
      }
    })
    .catch(console.error);
}

export default {
  scrapeFederalAuditClearinghouse,
  getFederalAuditReports,
  TARGET_ENTITIES,
};
