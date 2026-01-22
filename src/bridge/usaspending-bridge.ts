import { query, execute } from '../db/db-adapter.js';
import { resolveEntity } from '../matcher/entity-resolver.js';

/**
 * Bridge USAspending data into expenditures
 */
export async function bridgeUSASpending(documentId: number) {
  console.log('Bridging USAspending data...');

  const records = await query('SELECT * FROM source_usaspending');
  let imported = 0;

  for (const raw of records) {
    try {
      const recipient = raw.recipient_name;
      if (!recipient) continue;

      const amount = parseFloat(raw.amount) || 0;
      const date = raw.start_date;
      const fy = date ? (new Date(date).getMonth() >= 9 ? new Date(date).getFullYear() + 1 : new Date(date).getFullYear()) : new Date().getFullYear();

      // 1. Resolve Entity
      const match = await resolveEntity({
        name: recipient,
        sourceSystem: 'usaspending',
        sourceIdentifier: raw.award_id
      });

      const masterId = match.matched ? match.providerId : null;

      // 2. Insert into Expenditures
      await execute(`
        INSERT INTO expenditures (provider_master_id, fiscal_year, vendor_name, amount, payment_date, description, source_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `, [
        masterId,
        fy,
        recipient,
        amount,
        date,
        raw.description || `Federal Grant ${raw.award_id}`,
        `USAspending:${raw.award_id}`
      ]);

      imported++;
    } catch (err) {
      console.error(`Error bridging record:`, err);
    }
  }

  return { imported };
}
