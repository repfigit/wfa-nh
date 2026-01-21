/**
 * Migration Script: Migrate existing provider data to provider_master
 *
 * This script migrates data from the legacy `providers` table to the new
 * `provider_master` table, establishing the CCIS-driven data architecture.
 *
 * Run with: npx tsx src/db/migrate-to-master.ts
 */

import { initializeDb, saveDb } from './database.js';
import { query, execute } from './db-adapter.js';
import {
  normalizeName,
  normalizeAddress,
  normalizePhone,
  normalizeZip,
  jaroWinkler,
  logMatchAudit,
  createSourceLink,
} from '../matcher/entity-resolver.js';

interface MigrationStats {
  providersProcessed: number;
  masterRecordsCreated: number;
  sourceLinksCreated: number;
  aliasesCreated: number;
  duplicatesFound: number;
  errors: string[];
}

/**
 * Main migration function
 */
export async function migrateToMaster(): Promise<MigrationStats> {
  console.log('Starting migration to provider_master...');

  await initializeDb();

  const stats: MigrationStats = {
    providersProcessed: 0,
    masterRecordsCreated: 0,
    sourceLinksCreated: 0,
    aliasesCreated: 0,
    duplicatesFound: 0,
    errors: [],
  };

  try {
    // Step 1: Get all existing providers
    console.log('Fetching existing providers...');
    const providers = await query(`
      SELECT * FROM providers ORDER BY id
    `);
    console.log(`Found ${providers.length} providers to migrate`);

    // Step 2: Create a map to track normalized names for deduplication
    const normalizedNameMap = new Map<string, number>(); // normalized name -> provider_master id

    // Step 3: Process each provider
    for (const provider of providers) {
      stats.providersProcessed++;

      try {
        const name = provider.name as string;
        const canonicalName = normalizeName(name);
        const addressNormalized = provider.address ? normalizeAddress(provider.address as string) : null;
        const zip5 = normalizeZip(provider.zip as string | null);
        const phoneNormalized = normalizePhone(provider.phone as string | null);
        const city = (provider.city as string | null)?.toUpperCase().trim() || null;

        // Check if we already have a provider_master with this canonical name + city
        const existingMaster = await query(`
          SELECT id, canonical_name FROM provider_master
          WHERE canonical_name = ? AND (city = ? OR city IS NULL)
          LIMIT 1
        `, [canonicalName, city || '']);

        let masterId: number;

        if (existingMaster.length > 0) {
          // Already exists - this is a potential duplicate
          masterId = existingMaster[0].id as number;
          stats.duplicatesFound++;

          console.log(`  Duplicate found: "${name}" matches existing master #${masterId}`);

          // Add as alias if name is different
          if (name !== existingMaster[0].canonical_name) {
            await execute(`
              INSERT OR IGNORE INTO provider_aliases (
                provider_master_id, alias_name, alias_normalized, alias_type, source
              ) VALUES (?, ?, ?, 'variant', 'legacy_migration')
            `, [masterId, name, canonicalName]);
            stats.aliasesCreated++;
          }

        } else {
          // Check our local map for recently created records
          const mapKey = `${canonicalName}|${city || ''}`;
          if (normalizedNameMap.has(mapKey)) {
            masterId = normalizedNameMap.get(mapKey)!;
            stats.duplicatesFound++;

            // Add as alias
            await execute(`
              INSERT OR IGNORE INTO provider_aliases (
                provider_master_id, alias_name, alias_normalized, alias_type, source
              ) VALUES (?, ?, ?, 'variant', 'legacy_migration')
            `, [masterId, name, canonicalName]);
            stats.aliasesCreated++;

          } else {
            // Create new provider_master record
            const ccisId = provider.provider_id || provider.ccdf_provider_id || `LEGACY-${provider.id}`;

            const result = await execute(`
              INSERT INTO provider_master (
                ccis_provider_id, canonical_name, name_display,
                address_normalized, address_display, city, state, zip, zip5,
                phone_normalized, email, provider_type, license_number,
                capacity, accepts_ccdf, is_immigrant_owned, is_active,
                first_seen_date, last_verified_date
              ) VALUES (?, ?, ?, ?, ?, ?, 'NH', ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
            `, [
              ccisId,
              canonicalName,
              name,
              addressNormalized,
              provider.address || null,
              city,
              provider.zip || null,
              zip5 || null,
              phoneNormalized || null,
              provider.email || null,
              provider.provider_type || null,
              provider.license_number || null,
              provider.capacity || null,
              provider.accepts_ccdf || 0,
              provider.is_immigrant_owned || 0,
            ]);

            masterId = result.lastId as number;
            normalizedNameMap.set(mapKey, masterId);
            stats.masterRecordsCreated++;

            console.log(`  Created master #${masterId}: "${name}"`);
          }
        }

        // Create source link from legacy provider to master
        await execute(`
          INSERT OR IGNORE INTO provider_source_links (
            provider_master_id, source_system, source_identifier, source_name,
            match_method, match_score, status
          ) VALUES (?, 'legacy', ?, ?, 'migration', 1.0, 'active')
        `, [masterId, `legacy-${provider.id}`, name]);
        stats.sourceLinksCreated++;

        // Log the migration
        await logMatchAudit(
          masterId,
          'legacy',
          `legacy-${provider.id}`,
          name,
          'matched',
          1.0,
          'migration'
        );

      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : 'Unknown error';
        stats.errors.push(`Provider ${provider.id} (${provider.name}): ${errorMsg}`);
        console.error(`  Error migrating provider ${provider.id}:`, errorMsg);
      }

      // Progress update every 100 records
      if (stats.providersProcessed % 100 === 0) {
        console.log(`Progress: ${stats.providersProcessed}/${providers.length} providers processed`);
      }
    }

    // Step 4: Link existing expenditures to provider_master
    console.log('\nLinking expenditures to provider_master...');
    await linkExpendituresToMaster();

    await saveDb();

    console.log('\n=== Migration Complete ===');
    console.log(`Providers processed: ${stats.providersProcessed}`);
    console.log(`Master records created: ${stats.masterRecordsCreated}`);
    console.log(`Source links created: ${stats.sourceLinksCreated}`);
    console.log(`Aliases created: ${stats.aliasesCreated}`);
    console.log(`Duplicates found: ${stats.duplicatesFound}`);
    console.log(`Errors: ${stats.errors.length}`);

    if (stats.errors.length > 0) {
      console.log('\nErrors:');
      stats.errors.slice(0, 10).forEach(e => console.log(`  - ${e}`));
      if (stats.errors.length > 10) {
        console.log(`  ... and ${stats.errors.length - 10} more`);
      }
    }

  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Unknown error';
    stats.errors.push(`Fatal error: ${errorMsg}`);
    console.error('Migration failed:', errorMsg);
  }

  return stats;
}

/**
 * Link existing expenditures to provider_master via vendor name matching
 */
async function linkExpendituresToMaster(): Promise<void> {
  // Get expenditures that have a provider_id but could be linked to master
  const expendituresWithProvider = await query(`
    SELECT e.id, e.vendor_name, e.provider_id, p.name as provider_name
    FROM expenditures e
    LEFT JOIN providers p ON e.provider_id = p.id
    WHERE e.provider_id IS NOT NULL
  `);

  console.log(`Found ${expendituresWithProvider.length} expenditures with provider links`);

  let linked = 0;
  for (const exp of expendituresWithProvider) {
    // Find the corresponding provider_master record
    const providerName = exp.provider_name || exp.vendor_name;
    if (!providerName) continue;

    const canonicalName = normalizeName(providerName as string);

    const master = await query(`
      SELECT pm.id FROM provider_master pm
      LEFT JOIN provider_source_links psl ON pm.id = psl.provider_master_id
      WHERE pm.canonical_name = ?
         OR psl.source_identifier = ?
      LIMIT 1
    `, [canonicalName, `legacy-${exp.provider_id}`]);

    if (master.length > 0) {
      // Note: We would add provider_master_id column to expenditures here
      // For now, the link is tracked via provider_source_links
      linked++;
    }
  }

  console.log(`Linked ${linked} expenditures to provider_master`);
}

/**
 * Verify migration integrity
 */
export async function verifyMigration(): Promise<{
  masterCount: number;
  legacyCount: number;
  linksCount: number;
  orphanedLegacy: number;
  duplicateMasters: number;
}> {
  await initializeDb();

  const masterCount = await query('SELECT COUNT(*) as count FROM provider_master');
  const legacyCount = await query('SELECT COUNT(*) as count FROM providers');
  const linksCount = await query('SELECT COUNT(*) as count FROM provider_source_links');

  // Find legacy providers without a source link
  const orphanedLegacy = await query(`
    SELECT COUNT(*) as count FROM providers p
    WHERE NOT EXISTS (
      SELECT 1 FROM provider_source_links psl
      WHERE psl.source_system = 'legacy'
      AND psl.source_identifier = 'legacy-' || p.id
    )
  `);

  // Find potential duplicate masters
  const duplicateMasters = await query(`
    SELECT COUNT(*) as count FROM (
      SELECT canonical_name, city, COUNT(*) as cnt
      FROM provider_master
      GROUP BY canonical_name, city
      HAVING cnt > 1
    )
  `);

  return {
    masterCount: masterCount[0]?.count as number || 0,
    legacyCount: legacyCount[0]?.count as number || 0,
    linksCount: linksCount[0]?.count as number || 0,
    orphanedLegacy: orphanedLegacy[0]?.count as number || 0,
    duplicateMasters: duplicateMasters[0]?.count as number || 0,
  };
}

// Run migration if executed directly
const isMainModule = import.meta.url === `file://${process.argv[1]}`;
if (isMainModule) {
  migrateToMaster()
    .then(async (stats) => {
      console.log('\nVerifying migration...');
      const verification = await verifyMigration();
      console.log('Verification:', verification);
      process.exit(stats.errors.length > 0 ? 1 : 0);
    })
    .catch((error) => {
      console.error('Migration script failed:', error);
      process.exit(1);
    });
}

export default {
  migrateToMaster,
  verifyMigration,
};
