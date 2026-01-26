/**
 * Bridge DAS (Department of Administrative Services) Bid Board data
 * from scraped_documents into master tables
 *
 * This bridges:
 * - Awarded bidders → contractors table
 * - Bid awards → contracts table
 * - Links to existing provider_master records where possible
 * - Creates fraud_indicators for no-bid contracts and rapid awards
 */

import { query, execute } from '../db/db-adapter.js';
import { normalizeName } from '../matcher/entity-resolver.js';

interface DASBid {
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
 * Bridge DAS Bids data from scraped_documents
 */
export async function bridgeDASBids() {
  console.log('Bridging DAS Bid Board data...');

  // 1. Fetch data from scraped_documents
  const docs = await query<{ id: number; raw_content: string }>(
    `SELECT id, raw_content FROM scraped_documents
     WHERE source_key = 'das_bids'
     ORDER BY scraped_at DESC`
  );

  if (docs.length === 0) {
    console.log('  No DAS bids found in scraped_documents.');
    return { contractorsImported: 0, contractsImported: 0, fraudIndicatorsCreated: 0 };
  }

  console.log(`  Processing ${docs.length} DAS bid records...`);

  let contractorsImported = 0;
  let contractorsUpdated = 0;
  let contractsImported = 0;
  let fraudIndicatorsCreated = 0;

  for (const doc of docs) {
    try {
      const bid: DASBid = JSON.parse(doc.raw_content);

      // Skip bids without awarded vendors
      if (!bid.awardedVendor || bid.status !== 'awarded') {
        continue;
      }

      const canonicalVendorName = normalizeName(bid.awardedVendor);

      // 2. Upsert contractor
      const existingContractor = await query<{ id: number }>(
        'SELECT id FROM contractors WHERE name = ? OR name = ?',
        [bid.awardedVendor, canonicalVendorName]
      );

      let contractorId: number;

      if (existingContractor.length > 0) {
        contractorId = existingContractor[0].id;
        contractorsUpdated++;
      } else {
        const result = await execute(
          `INSERT INTO contractors (vendor_code, name, city, state, is_immigrant_related, created_at)
           VALUES (?, ?, NULL, 'NH', ?, datetime('now'))`,
          [bid.awardedVendorCode, bid.awardedVendor, bid.isImmigrantRelated ? 1 : 0]
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
        [canonicalVendorName, bid.awardedVendor]
      );
      providerMasterId = provider[0]?.id || null;

      // 4. Create or update contract record
      const existingContract = await query<{ id: number }>(
        'SELECT id FROM contracts WHERE contract_number = ?',
        [bid.bidNumber]
      );

      if (existingContract.length === 0) {
        await execute(
          `INSERT INTO contracts (
            contractor_id, provider_master_id, contract_number, title,
            start_date, end_date, amount, source_url, created_at
          ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, datetime('now'))`,
          [
            contractorId,
            providerMasterId,
            bid.bidNumber,
            bid.title,
            bid.awardDate,
            bid.awardedValue,
            bid.sourceUrl
          ]
        );
        contractsImported++;
      }

      // 5. Create fraud indicators
      for (const indicator of bid.fraudIndicators) {
        const existingIndicator = await query(
          `SELECT id FROM fraud_indicators
           WHERE indicator_type = ?
           AND description LIKE ?
           AND (provider_master_id = ? OR provider_master_id IS NULL)
           LIMIT 1`,
          [indicator.type, `%${bid.bidNumber}%`, providerMasterId]
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
              `[DAS ${bid.bidNumber}] ${indicator.description}`
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
      console.error(`  Error processing DAS bid record:`, err);
    }
  }

  console.log(`\n✅ DAS Bids bridge complete:`);
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

export default { bridgeDASBids };
