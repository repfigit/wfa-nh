/**
 * Bridge HHS TAGGS (Tracking Accountability in Government Grants System) data
 * from scraped_documents into master tables
 *
 * This bridges:
 * - Award recipients → contractors table
 * - Award amounts → expenditures table
 * - Links to existing provider_master records where possible
 * - Creates fraud_indicators for double-dipping or discrepancies
 */

import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

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
 * Bridge HHS TAGGS data from scraped_documents
 */
export async function bridgeHHSTAGGS() {
  console.log('Bridging HHS TAGGS data...');

  // 1. Fetch data from scraped_documents
  const docs = await query<{ id: number; raw_content: string }>(
    `SELECT id, raw_content FROM scraped_documents
     WHERE source_key = 'hhs_taggs'
     ORDER BY scraped_at DESC`
  );

  if (docs.length === 0) {
    console.log('  No HHS TAGGS awards found in scraped_documents.');
    return { contractorsImported: 0, expendituresImported: 0, fraudIndicatorsCreated: 0 };
  }

  console.log(`  Processing ${docs.length} HHS TAGGS award records...`);

  let contractorsImported = 0;
  let contractorsUpdated = 0;
  let expendituresImported = 0;
  let fraudIndicatorsCreated = 0;

  for (const doc of docs) {
    try {
      const award: TAGGSAward = JSON.parse(doc.raw_content);

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
           VALUES (?, ?, ?, ?, datetime('now'))`,
          [
            award.recipientName,
            award.recipientCity,
            award.recipientState,
            award.category === 'refugee' ? 1 : 0
          ]
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

      // 4. Create expenditure record
      const existingExpenditure = await query(
        `SELECT id FROM expenditures
         WHERE vendor_name = ?
         AND fiscal_year = ?
         AND amount = ?
         AND source_url = 'hhs_taggs'
         LIMIT 1`,
        [award.recipientName, award.fiscalYear, award.awardAmount]
      );

      if (existingExpenditure.length === 0) {
        await execute(
          `INSERT INTO expenditures (
            provider_master_id, contractor_id, fiscal_year, vendor_name,
            amount, payment_date, description, source_url, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, 'hhs_taggs', datetime('now'))`,
          [
            providerMasterId,
            contractorId,
            award.fiscalYear,
            award.recipientName,
            award.awardAmount,
            award.awardDate,
            `${award.cfdaNumber}: ${award.cfdaProgramName} - ${award.actionType}`
          ]
        );
        expendituresImported++;
      }

      // 5. Create fraud indicators
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
              `[TAGGS ${award.awardId}] ${indicator.description}`
            ]
          );
          fraudIndicatorsCreated++;
        }
      }

      // 6. Check for double-dipping: federal + state awards for same service
      if (providerMasterId) {
        const stateExpenditures = await query<{ amount: number; fiscal_year: number }>(
          `SELECT amount, fiscal_year FROM expenditures
           WHERE provider_master_id = ?
           AND fiscal_year = ?
           AND source_url != 'hhs_taggs'`,
          [providerMasterId, award.fiscalYear]
        );

        if (stateExpenditures.length > 0) {
          const totalState = stateExpenditures.reduce((sum, e) => sum + e.amount, 0);
          const combined = totalState + award.awardAmount;

          // Flag if combined federal + state is unusually high
          if (combined > 5000000) {
            const existingDoubleDip = await query(
              `SELECT id FROM fraud_indicators
               WHERE indicator_type = 'federal_state_overlap'
               AND description LIKE ?
               AND provider_master_id = ?
               LIMIT 1`,
              [`%${award.fiscalYear}%`, providerMasterId]
            );

            if (existingDoubleDip.length === 0) {
              await execute(
                `INSERT INTO fraud_indicators (
                  provider_master_id, indicator_type, severity, description, status, created_at
                ) VALUES (?, 'federal_state_overlap', 'medium', ?, 'open', datetime('now'))`,
                [
                  providerMasterId,
                  `Federal ($${award.awardAmount.toLocaleString()}) + State ($${totalState.toLocaleString()}) = $${combined.toLocaleString()} in FY${award.fiscalYear} - verify no duplicate billing`
                ]
              );
              fraudIndicatorsCreated++;
            }
          }
        }
      }

      // Mark document as processed
      await execute(
        'UPDATE scraped_documents SET processed = 1, processed_at = datetime(\'now\') WHERE id = ?',
        [doc.id]
      );

    } catch (err) {
      console.error(`  Error processing HHS TAGGS award record:`, err);
    }
  }

  console.log(`\n✅ HHS TAGGS bridge complete:`);
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

export default { bridgeHHSTAGGS };
