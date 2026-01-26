/**
 * Bridge DHHS Contracts data from scraped_documents into master tables
 *
 * This bridges:
 * - Awarded vendors → contractors table
 * - Contract details → contracts table
 * - Links vendors to existing provider_master records where possible
 * - Creates fraud_indicators for cross-reference anomalies
 */

import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

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
 * Bridge DHHS Contracts data from scraped_documents
 */
export async function bridgeDHHSContracts() {
  console.log('Bridging DHHS Contracts data...');

  // 1. Fetch data from scraped_documents
  const docs = await query<{ id: number; raw_content: string }>(
    `SELECT id, raw_content FROM scraped_documents
     WHERE source_key = 'dhhs_contracts'
     ORDER BY scraped_at DESC`
  );

  if (docs.length === 0) {
    console.log('  No DHHS contracts found in scraped_documents.');
    return { contractorsImported: 0, contractsImported: 0, fraudIndicatorsCreated: 0 };
  }

  console.log(`  Processing ${docs.length} DHHS contract records...`);

  let contractorsImported = 0;
  let contractorsUpdated = 0;
  let contractsImported = 0;
  let fraudIndicatorsCreated = 0;

  for (const doc of docs) {
    try {
      const contract: DHHSContract = JSON.parse(doc.raw_content);

      if (!contract.awardedVendor) {
        continue; // Skip contracts without awarded vendors
      }

      const canonicalVendorName = normalizeName(contract.awardedVendor);

      // 2. Upsert contractor
      const existingContractor = await query<{ id: number }>(
        'SELECT id FROM contractors WHERE name = ? OR name = ?',
        [contract.awardedVendor, canonicalVendorName]
      );

      let contractorId: number;

      if (existingContractor.length > 0) {
        contractorId = existingContractor[0].id;
        contractorsUpdated++;
      } else {
        const result = await execute(
          `INSERT INTO contractors (vendor_code, name, city, state, is_immigrant_related, created_at)
           VALUES (?, ?, NULL, 'NH', ?, datetime('now'))`,
          [contract.awardedVendorCode, contract.awardedVendor, contract.isImmigrantRelated ? 1 : 0]
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
        [canonicalVendorName, contract.awardedVendor]
      );
      providerMasterId = provider[0]?.id || null;

      // 4. Create or update contract record
      const existingContract = await query<{ id: number }>(
        'SELECT id FROM contracts WHERE contract_number = ?',
        [contract.rfpNumber]
      );

      if (existingContract.length === 0) {
        await execute(
          `INSERT INTO contracts (
            contractor_id, provider_master_id, contract_number, title,
            start_date, end_date, amount, source_url, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
          [
            contractorId,
            providerMasterId,
            contract.rfpNumber,
            contract.title,
            contract.contractStartDate,
            contract.contractEndDate,
            contract.awardedValue,
            contract.sourceUrl
          ]
        );
        contractsImported++;
      }

      // 5. Create fraud indicators
      for (const indicator of contract.fraudIndicators) {
        // Check if this specific indicator already exists
        const existingIndicator = await query(
          `SELECT id FROM fraud_indicators
           WHERE indicator_type = ?
           AND description LIKE ?
           AND (provider_master_id = ? OR provider_master_id IS NULL)
           LIMIT 1`,
          [indicator.type, `%${contract.rfpNumber}%`, providerMasterId]
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
              `[${contract.rfpNumber}] ${indicator.description}`
            ]
          );
          fraudIndicatorsCreated++;
        }
      }

      // Mark document as processed
      await execute(
        'UPDATE scraped_documents SET processed = 1, processed_at = datetime(\'now\') WHERE id = ?',
        [doc.id]
      );

    } catch (err) {
      console.error(`  Error processing DHHS contract record:`, err);
    }
  }

  console.log(`\n✅ DHHS Contracts bridge complete:`);
  console.log(`   Contractors: ${contractorsImported} new, ${contractorsUpdated} existing`);
  console.log(`   Contracts: ${contractsImported} imported`);
  console.log(`   Fraud Indicators: ${fraudIndicatorsCreated} created`);

  return {
    contractorsImported,
    contractorsUpdated,
    contractsImported,
    fraudIndicatorsCreated
  };
}

export default { bridgeDHHSContracts };
