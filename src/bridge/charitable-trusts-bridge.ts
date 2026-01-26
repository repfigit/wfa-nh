/**
 * Bridge NH Charitable Trusts / Form 990 data from scraped_documents into master tables
 *
 * This bridges:
 * - Nonprofits → contractors table
 * - Links to existing provider_master records where possible
 * - Creates fraud_indicators for financial anomalies (high overhead, compensation issues, etc.)
 */

import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

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

interface FraudIndicator {
  type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  description: string;
}

/**
 * Bridge Charitable Trusts / Form 990 data from scraped_documents
 */
export async function bridgeCharitableTrusts() {
  console.log('Bridging Charitable Trusts / Form 990 data...');

  // 1. Fetch data from scraped_documents
  const docs = await query<{ id: number; raw_content: string }>(
    `SELECT id, raw_content FROM scraped_documents
     WHERE source_key = 'charitable_trusts'
     ORDER BY scraped_at DESC`
  );

  if (docs.length === 0) {
    console.log('  No charitable trusts data found in scraped_documents.');
    return { contractorsImported: 0, fraudIndicatorsCreated: 0 };
  }

  console.log(`  Processing ${docs.length} nonprofit profile records...`);

  let contractorsImported = 0;
  let contractorsUpdated = 0;
  let fraudIndicatorsCreated = 0;

  for (const doc of docs) {
    try {
      const nonprofit: NonprofitProfile = JSON.parse(doc.raw_content);

      const canonicalName = normalizeName(nonprofit.name);

      // 2. Upsert contractor (nonprofit)
      const existingContractor = await query<{ id: number }>(
        'SELECT id FROM contractors WHERE name = ? OR name = ?',
        [nonprofit.name, canonicalName]
      );

      let contractorId: number;

      if (existingContractor.length > 0) {
        contractorId = existingContractor[0].id;
        contractorsUpdated++;
      } else {
        const result = await execute(
          `INSERT INTO contractors (name, city, state, is_immigrant_related, created_at)
           VALUES (?, ?, ?, 1, datetime('now'))`,
          [nonprofit.name, nonprofit.city, nonprofit.state]
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
        [canonicalName, nonprofit.name]
      );
      providerMasterId = provider[0]?.id || null;

      // 4. Create fraud indicators from nonprofit analysis
      for (const indicator of nonprofit.fraudIndicators) {
        const existingIndicator = await query(
          `SELECT id FROM fraud_indicators
           WHERE indicator_type = ?
           AND description LIKE ?
           AND (provider_master_id = ? OR provider_master_id IS NULL)
           LIMIT 1`,
          [indicator.type, `%${nonprofit.ein}%`, providerMasterId]
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
              `[990 EIN ${nonprofit.ein}] ${indicator.description}`
            ]
          );
          fraudIndicatorsCreated++;
        }
      }

      // 5. Check for revenue vs payment mismatch
      if (providerMasterId && nonprofit.governmentGrants) {
        const expenditures = await query<{ amount: number; fiscal_year: number }>(
          `SELECT amount, fiscal_year FROM expenditures
           WHERE provider_master_id = ?
           AND fiscal_year = ?`,
          [providerMasterId, nonprofit.latestFilingYear]
        );

        if (expenditures.length > 0) {
          const totalPayments = expenditures.reduce((sum, e) => sum + e.amount, 0);
          const discrepancy = Math.abs(totalPayments - nonprofit.governmentGrants);
          const discrepancyPercent = (discrepancy / nonprofit.governmentGrants) * 100;

          // Flag if there's a significant mismatch
          if (discrepancyPercent > 20) {
            const existingMismatch = await query(
              `SELECT id FROM fraud_indicators
               WHERE indicator_type = 'revenue_payment_mismatch'
               AND description LIKE ?
               AND provider_master_id = ?
               LIMIT 1`,
              [`%${nonprofit.latestFilingYear}%`, providerMasterId]
            );

            if (existingMismatch.length === 0) {
              await execute(
                `INSERT INTO fraud_indicators (
                  provider_master_id, indicator_type, severity, description, status, created_at
                ) VALUES (?, 'revenue_payment_mismatch', 'medium', ?, 'open', datetime('now'))`,
                [
                  providerMasterId,
                  `Form 990 reports $${nonprofit.governmentGrants.toLocaleString()} in government grants for FY${nonprofit.latestFilingYear}, but state records show $${totalPayments.toLocaleString()} (${discrepancyPercent.toFixed(0)}% difference) - verify accuracy`
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
      console.error(`  Error processing charitable trusts record:`, err);
    }
  }

  console.log(`\n✅ Charitable Trusts bridge complete:`);
  console.log(`   Contractors: ${contractorsImported} new, ${contractorsUpdated} existing`);
  console.log(`   Fraud Indicators: ${fraudIndicatorsCreated} created`);

  return {
    contractorsImported,
    contractorsUpdated,
    fraudIndicatorsCreated
  };
}

export default { bridgeCharitableTrusts };
