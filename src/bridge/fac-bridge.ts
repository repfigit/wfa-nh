/**
 * Bridge Federal Audit Clearinghouse (FAC) data from scraped_documents into master tables
 *
 * This bridges:
 * - Auditees → contractors table
 * - Audit findings → fraud_indicators table
 * - Links to existing provider_master records where possible
 */

import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

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
 * Bridge Federal Audit Clearinghouse data from scraped_documents
 */
export async function bridgeFAC() {
  console.log('Bridging Federal Audit Clearinghouse data...');

  // 1. Fetch data from scraped_documents
  const docs = await query<{ id: number; raw_content: string }>(
    `SELECT id, raw_content FROM scraped_documents
     WHERE source_key = 'fac_audits'
     ORDER BY scraped_at DESC`
  );

  if (docs.length === 0) {
    console.log('  No FAC audit reports found in scraped_documents.');
    return { contractorsImported: 0, fraudIndicatorsCreated: 0 };
  }

  console.log(`  Processing ${docs.length} FAC audit reports...`);

  let contractorsImported = 0;
  let contractorsUpdated = 0;
  let fraudIndicatorsCreated = 0;

  for (const doc of docs) {
    try {
      const audit: SingleAuditReport = JSON.parse(doc.raw_content);

      const canonicalName = normalizeName(audit.auditeeName);

      // 2. Upsert contractor (auditee)
      const existingContractor = await query<{ id: number }>(
        'SELECT id FROM contractors WHERE name = ? OR name = ?',
        [audit.auditeeName, canonicalName]
      );

      let contractorId: number;

      if (existingContractor.length > 0) {
        contractorId = existingContractor[0].id;
        contractorsUpdated++;
      } else {
        const result = await execute(
          `INSERT INTO contractors (name, city, state, is_immigrant_related, created_at)
           VALUES (?, ?, ?, 0, datetime('now'))`,
          [audit.auditeeName, audit.auditeeCity, audit.auditeeState]
        );
        contractorId = result.lastId!;
        contractorsImported++;
      }

      // 3. Try to match to provider_master
      let providerMasterId: number | null = null;
      const provider = await query<{ id: number }>(
        `SELECT id FROM provider_master
         WHERE canonical_name = ?
         OR name_display = ?
         LIMIT 1`,
        [canonicalName, audit.auditeeName]
      );
      providerMasterId = provider[0]?.id || null;

      // 4. Create fraud indicators from audit findings
      for (const indicator of audit.fraudIndicators) {
        const existingIndicator = await query(
          `SELECT id FROM fraud_indicators
           WHERE indicator_type = ?
           AND description LIKE ?
           AND (provider_master_id = ? OR provider_master_id IS NULL)
           LIMIT 1`,
          [indicator.type, `%${audit.reportId}%`, providerMasterId]
        );

        if (existingIndicator.length === 0) {
          await execute(
            `INSERT INTO fraud_indicators (
              provider_master_id, indicator_type, severity, description, status, created_at
            ) VALUES (?, ?, ?, ?, 'open', datetime('now'))`,
            [
              providerMasterId,
              indicator.type,
              indicator.severity,
              `[FAC ${audit.reportId}] ${indicator.description}`
            ]
          );
          fraudIndicatorsCreated++;
        }
      }

      // 5. Create fraud indicators for specific audit findings
      if (audit.hasFindings) {
        for (const finding of audit.findings) {
          let severity: 'low' | 'medium' | 'high' | 'critical' = 'medium';

          if (finding.materialWeakness) {
            severity = 'critical';
          } else if (finding.significantDeficiency) {
            severity = 'high';
          } else if (finding.questionedCosts > 50000) {
            severity = 'high';
          }

          const existingFinding = await query(
            `SELECT id FROM fraud_indicators
             WHERE indicator_type = 'audit_finding'
             AND description LIKE ?
             AND (provider_master_id = ? OR provider_master_id IS NULL)
             LIMIT 1`,
            [`%${finding.referenceNumber}%`, providerMasterId]
          );

          if (existingFinding.length === 0) {
            await execute(
              `INSERT INTO fraud_indicators (
                provider_master_id, indicator_type, severity, description, status, created_at
              ) VALUES (?, 'audit_finding', ?, ?, 'open', datetime('now'))`,
              [
                providerMasterId,
                severity,
                `[FAC ${audit.reportId} - ${finding.referenceNumber}] ${finding.findingType}: ${finding.description}${finding.questionedCosts > 0 ? ` - Questioned costs: $${finding.questionedCosts.toLocaleString()}` : ''}`
              ]
            );
            fraudIndicatorsCreated++;
          }
        }
      }

      // Mark document as processed
      await execute(
        'UPDATE scraped_documents SET processed = 1, processed_at = datetime(\'now\') WHERE id = ?',
        [doc.id]
      );

    } catch (err) {
      console.error(`  Error processing FAC audit record:`, err);
    }
  }

  console.log(`\n✅ FAC bridge complete:`);
  console.log(`   Contractors: ${contractorsImported} new, ${contractorsUpdated} existing`);
  console.log(`   Fraud Indicators: ${fraudIndicatorsCreated} created`);

  return {
    contractorsImported,
    contractorsUpdated,
    fraudIndicatorsCreated
  };
}

export default { bridgeFAC };
