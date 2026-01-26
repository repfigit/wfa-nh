/**
 * Bridge SAM.gov Federal Contracts/Grants data from scraped_documents into master tables
 *
 * This bridges:
 * - Federal award recipients → contractors table
 * - Federal award amounts → expenditures table
 * - Links to existing provider_master records where possible
 * - Creates fraud_indicators for federal + state duplicate billing
 */

import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

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
 * Bridge SAM.gov Federal Awards data from scraped_documents
 */
export async function bridgeSAMGov() {
  console.log('Bridging SAM.gov Federal Awards data...');

  // 1. Fetch data from scraped_documents
  const docs = await query<{ id: number; raw_content: string }>(
    `SELECT id, raw_content FROM scraped_documents
     WHERE source_key = 'sam_gov'
     ORDER BY scraped_at DESC`
  );

  if (docs.length === 0) {
    console.log('  No SAM.gov awards found in scraped_documents.');
    return { contractorsImported: 0, expendituresImported: 0, fraudIndicatorsCreated: 0 };
  }

  console.log(`  Processing ${docs.length} SAM.gov award records...`);

  let contractorsImported = 0;
  let contractorsUpdated = 0;
  let expendituresImported = 0;
  let fraudIndicatorsCreated = 0;

  for (const doc of docs) {
    try {
      const award: FederalAward = JSON.parse(doc.raw_content);

      const canonicalName = normalizeName(award.recipientName);

      // 2. Upsert contractor (recipient)
      const existingContractor = await query<{ id: number }>(
        'SELECT id FROM contractors WHERE name = ? OR name = ?',
        [award.recipientName, canonicalName]
      );

      let contractorId: number;

      if (existingContractor.length > 0) {
        contractorId = existingContractor[0].id;
        contractorsUpdated++;
      } else {
        const result = await execute(
          `INSERT INTO contractors (name, city, state, is_immigrant_related, created_at)
           VALUES (?, NULL, ?, 1, datetime('now'))`,
          [award.recipientName, award.recipientState]
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
        [canonicalName, award.recipientName]
      );
      providerMasterId = provider[0]?.id || null;

      // 4. Create expenditure record for federal award
      const fiscalYear = new Date(award.awardDate).getFullYear();

      const existingExpenditure = await query(
        `SELECT id FROM expenditures
         WHERE vendor_name = ?
         AND fiscal_year = ?
         AND amount = ?
         AND source_url = 'sam_gov'
         LIMIT 1`,
        [award.recipientName, fiscalYear, award.awardAmount]
      );

      if (existingExpenditure.length === 0) {
        await execute(
          `INSERT INTO expenditures (
            provider_master_id, contractor_id, fiscal_year, vendor_name,
            amount, payment_date, description, source_url, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, 'sam_gov', datetime('now'))`,
          [
            providerMasterId,
            contractorId,
            fiscalYear,
            award.recipientName,
            award.awardAmount,
            award.awardDate,
            `${award.cfdaNumber}: ${award.cfdaTitle} - ${award.awardDescription}`
          ]
        );
        expendituresImported++;
      }

      // 5. Create fraud indicators from award
      for (const indicator of award.fraudIndicators) {
        const existingIndicator = await query(
          `SELECT id FROM fraud_indicators
           WHERE indicator_type = ?
           AND description LIKE ?
           AND (provider_master_id = ? OR provider_master_id IS NULL)
           LIMIT 1`,
          [indicator.type, `%${award.awardId}%`, providerMasterId]
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
              `[SAM.gov ${award.awardId}] ${indicator.description}`
            ]
          );
          fraudIndicatorsCreated++;
        }
      }

      // 6. Check for federal + state duplicate billing
      if (providerMasterId) {
        const stateExpenditures = await query<{ amount: number; fiscal_year: number }>(
          `SELECT amount, fiscal_year FROM expenditures
           WHERE provider_master_id = ?
           AND fiscal_year = ?
           AND source_url IN ('transparent_nh', 'dhhs_contracts')`,
          [providerMasterId, fiscalYear]
        );

        if (stateExpenditures.length > 0) {
          const totalState = stateExpenditures.reduce((sum, e) => sum + e.amount, 0);

          // Flag if there's both federal and state funding
          const existingOverlap = await query(
            `SELECT id FROM fraud_indicators
             WHERE indicator_type = 'federal_state_overlap'
             AND description LIKE ?
             AND provider_master_id = ?
             LIMIT 1`,
            [`%${fiscalYear}%`, providerMasterId]
          );

          if (existingOverlap.length === 0) {
            await execute(
              `INSERT INTO fraud_indicators (
                provider_master_id, indicator_type, severity, description, status, created_at
              ) VALUES (?, 'federal_state_overlap', 'medium', ?, 'open', datetime('now'))`,
              [
                providerMasterId,
                `Federal ($${award.awardAmount.toLocaleString()}) + State ($${totalState.toLocaleString()}) in FY${fiscalYear} - verify no duplicate billing for same services`
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
      console.error(`  Error processing SAM.gov award record:`, err);
    }
  }

  console.log(`\n✅ SAM.gov bridge complete:`);
  console.log(`   Contractors: ${contractorsImported} new, ${contractorsUpdated} existing`);
  console.log(`   Expenditures: ${expendituresImported} imported`);
  console.log(`   Fraud Indicators: ${fraudIndicatorsCreated} created`);

  return {
    contractorsImported,
    contractorsUpdated,
    expendituresImported,
    fraudIndicatorsCreated
  };
}

export default { bridgeSAMGov };
