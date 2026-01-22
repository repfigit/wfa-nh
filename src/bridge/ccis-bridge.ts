import { query, execute } from '../db/db-adapter.js';
import {
  normalizeName,
  normalizeAddress,
  normalizePhone,
  normalizeZip,
  addProviderAlias,
} from '../matcher/entity-resolver.js';

const PROGRAM_TYPES: Record<string, string> = {
  'Licensed Child Care Center': 'center',
  'Licensed Family Child Care': 'family',
  'License-Exempt Child Care Program': 'exempt',
  'Licensed Plus': 'licensed_plus',
  'Family Resource Center': 'resource_center',
};

/**
 * Bridge structured CCIS data from source_ccis into the master tables
 */
export async function bridgeCCIS(documentId: number) {
  console.log(`Bridging NH CCIS source data...`);

  // 1. Fetch data from the dedicated source table
  const providers = await query('SELECT * FROM source_ccis');
  console.log(`Processing ${providers.length} records from source_ccis.`);

  let imported = 0;
  let updated = 0;

  for (const raw of providers) {
    try {
      const canonicalName = normalizeName(raw.program_name);
      const addressNormalized = raw.street ? normalizeAddress(raw.street) : null;
      const phoneNormalized = normalizePhone(raw.phone);
      const zip5 = normalizeZip(raw.zip);
      const cityNormalized = raw.city?.toUpperCase().trim() || null;

      const providerType = raw.record_type
        ? (PROGRAM_TYPES[raw.record_type] || raw.record_type.toLowerCase())
        : (raw.license_type || null);

      const ccisId = raw.provider_number || `CCIS-${canonicalName.substring(0, 20).replace(/\s+/g, '-')}`;

      // Check for existing
      const existing = await query(
        `SELECT id FROM provider_master WHERE ccis_provider_id = ? OR (canonical_name = ? AND city = ?)`,
        [ccisId, canonicalName, cityNormalized || '']
      );

      if (existing.length > 0) {
        const masterId = existing[0].id;
        await execute(`
          UPDATE provider_master SET
            canonical_name = ?, name_display = ?, address_normalized = ?, address_display = ?,
            city = ?, zip = ?, zip5 = ?, phone_normalized = ?, email = ?,
            provider_type = ?, capacity = ?, quality_rating = ?,
            last_verified_date = datetime('now'), updated_at = datetime('now')
          WHERE id = ?
        `, [
          canonicalName, raw.program_name, addressNormalized, raw.street,
          cityNormalized, raw.zip, zip5, phoneNormalized, raw.email,
          providerType, parseInt(raw.capacity) || null, raw.gsq_step,
          masterId
        ]);
        updated++;
      } else {
        const result = await execute(`
          INSERT INTO provider_master (
            ccis_provider_id, canonical_name, name_display, address_normalized, address_display,
            city, state, zip, zip5, phone_normalized, email, provider_type, capacity,
            quality_rating, is_active, first_seen_date, last_verified_date
          ) VALUES (?, ?, ?, ?, ?, ?, 'NH', ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
        `, [
          ccisId, canonicalName, raw.program_name, addressNormalized, raw.street,
          cityNormalized, raw.zip, zip5, phoneNormalized, raw.email,
          providerType, parseInt(raw.capacity) || null, raw.gsq_step
        ]);
        
        const masterId = result.lastId!;
        await execute(`INSERT OR IGNORE INTO provider_source_links (provider_master_id, source_system, source_identifier, source_name, match_method, status)
                       VALUES (?, 'ccis', ?, ?, 'primary_source', 'active')`, [masterId, ccisId, raw.program_name]);
        
        if (raw.program_name !== canonicalName) {
          await addProviderAlias(masterId, raw.program_name, 'variant', 'ccis', 1.0);
        }
        
        imported++;
      }
    } catch (err) {
      console.error(`Error bridging record:`, err);
    }
  }

  // Mark audit doc as processed
  await execute('UPDATE scraped_documents SET processed = 1, processed_at = datetime(\'now\') WHERE id = ?', [documentId]);

  console.log(`Bridge complete: ${imported} imported, ${updated} updated.`);
  return { imported, updated };
}
