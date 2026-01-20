/**
 * Seed database with NH childcare/daycare provider data
 * Works with both SQLite (local) and Turso (production)
 */

import { initializeDb, closeDb, IS_LOCAL } from './database.js';
import { query, execute, saveSqliteDb } from './db-adapter.js';

export async function seedDatabase(skipInit = false) {
  if (!skipInit) {
    console.log('Initializing database...');
    await initializeDb();
  }

  // Helper to run insert and get last ID
  const runInsert = async (sql: string, params: any[]): Promise<number> => {
    try {
      const result = await execute(sql, params);
      return result.lastId || 0;
    } catch (err) {
      console.error('Insert error:', err);
      return 0;
    }
  };

  // Clear existing data for clean seed
  console.log('Clearing existing data...');
  await execute('DELETE FROM fraud_indicators');
  await execute('DELETE FROM payments');
  await execute('DELETE FROM contracts');
  await execute('DELETE FROM expenditures');
  await execute('DELETE FROM providers');
  await execute('DELETE FROM contractors');

  // ========================================
  // CHILDCARE PROVIDERS (Sample Data)
  // ========================================
  console.log('\nInserting childcare providers...');
  
  const providers = [
    {
      name: 'Little Stars Family Daycare',
      dba_name: null,
      address: '123 Elm Street',
      city: 'Manchester',
      zip: '03101',
      license_type: 'Family Group',
      capacity: 12,
      provider_type: 'Family Childcare',
      is_immigrant_owned: 1,
      owner_name: 'Fatima Hassan',
      owner_background: 'Somali immigrant, arrived 2015',
      language_services: 'Somali, Arabic, English',
      accepts_ccdf: 1,
      notes: 'Family daycare in immigrant community. High CCDF scholarship usage.',
    },
    {
      name: 'Sunrise Early Learning Center',
      dba_name: 'Sunrise Daycare',
      address: '456 Main Street',
      city: 'Nashua',
      zip: '03060',
      license_type: 'Center-based',
      capacity: 45,
      provider_type: 'Childcare Center',
      is_immigrant_owned: 1,
      owner_name: 'Ahmed Mohamed',
      owner_background: 'Somali-American, community leader',
      language_services: 'Somali, Arabic, Swahili, English',
      accepts_ccdf: 1,
      notes: 'Larger center serving refugee community. Multiple CCDF contracts.',
    },
    {
      name: 'Happy Hearts Home Daycare',
      dba_name: null,
      address: '789 Oak Avenue',
      city: 'Concord',
      zip: '03301',
      license_type: 'Family',
      capacity: 6,
      provider_type: 'Family Childcare',
      is_immigrant_owned: 1,
      owner_name: 'Amina Osman',
      owner_background: 'Refugee from Somalia, 2018',
      language_services: 'Somali, English',
      accepts_ccdf: 1,
      notes: 'Small home daycare, primarily serves Somali families.',
    },
    {
      name: 'New Horizons Childcare',
      dba_name: null,
      address: '321 Pine Road',
      city: 'Manchester',
      zip: '03103',
      license_type: 'Center-based',
      capacity: 30,
      provider_type: 'Childcare Center',
      is_immigrant_owned: 1,
      owner_name: 'Bisharo Abdi',
      owner_background: 'Bhutanese-Nepali community',
      language_services: 'Nepali, Hindi, English',
      accepts_ccdf: 1,
      notes: 'Serves Bhutanese refugee community in Manchester.',
    },
    {
      name: 'Bright Futures Family Care',
      dba_name: null,
      address: '555 Maple Street',
      city: 'Manchester',
      zip: '03104',
      license_type: 'Family Group',
      capacity: 12,
      provider_type: 'Family Childcare',
      is_immigrant_owned: 1,
      owner_name: 'Halima Jama',
      owner_background: 'Somali immigrant',
      language_services: 'Somali, Arabic, English',
      accepts_ccdf: 1,
      notes: 'Recently opened, rapid enrollment growth.',
    },
    {
      name: 'ABC Learning Academy',
      dba_name: null,
      address: '100 School Street',
      city: 'Nashua',
      zip: '03064',
      license_type: 'Center-based',
      capacity: 75,
      provider_type: 'Childcare Center',
      is_immigrant_owned: 0,
      owner_name: 'Susan Miller',
      owner_background: 'Local business owner',
      language_services: 'English, Spanish',
      accepts_ccdf: 1,
      notes: 'Established center, mix of private pay and CCDF.',
    },
    {
      name: 'Community Kids Daycare',
      dba_name: 'CK Daycare',
      address: '200 Community Drive',
      city: 'Manchester',
      zip: '03102',
      license_type: 'Center-based',
      capacity: 50,
      provider_type: 'Childcare Center',
      is_immigrant_owned: 1,
      owner_name: 'Ibrahim Hassan',
      owner_background: 'Congolese refugee',
      language_services: 'French, Swahili, Lingala, English',
      accepts_ccdf: 1,
      notes: 'Serves African immigrant community.',
    },
  ];

  const providerIds: Record<string, number> = {};
  
  for (const p of providers) {
    const id = await runInsert(`
      INSERT INTO providers (name, dba_name, address, city, state, zip, license_type, capacity, provider_type, 
        is_immigrant_owned, owner_name, owner_background, language_services, accepts_ccdf, notes)
      VALUES (?, ?, ?, ?, 'NH', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [p.name, p.dba_name, p.address, p.city, p.zip, p.license_type, p.capacity, p.provider_type,
        p.is_immigrant_owned, p.owner_name, p.owner_background, p.language_services, p.accepts_ccdf, p.notes]);
    providerIds[p.name] = id;
    console.log(`  - Added provider: ${p.name} (ID: ${id})`);
  }

  // ========================================
  // CCDF PAYMENTS (Sample Data)
  // ========================================
  console.log('\nInserting CCDF payments...');
  
  const payments = [
    // Little Stars - high payments, suspicious pattern
    { provider: 'Little Stars Family Daycare', fy: 2024, month: 1, amount: 8500, children: 12 },
    { provider: 'Little Stars Family Daycare', fy: 2024, month: 2, amount: 9200, children: 12 },
    { provider: 'Little Stars Family Daycare', fy: 2024, month: 3, amount: 11500, children: 15 },
    { provider: 'Little Stars Family Daycare', fy: 2024, month: 4, amount: 12800, children: 16 },
    { provider: 'Little Stars Family Daycare', fy: 2024, month: 5, amount: 14200, children: 18 },
    { provider: 'Little Stars Family Daycare', fy: 2024, month: 6, amount: 15500, children: 20 },
    
    // Sunrise - large center, high but reasonable
    { provider: 'Sunrise Early Learning Center', fy: 2024, month: 1, amount: 28000, children: 35 },
    { provider: 'Sunrise Early Learning Center', fy: 2024, month: 2, amount: 32000, children: 40 },
    { provider: 'Sunrise Early Learning Center', fy: 2024, month: 3, amount: 35000, children: 44 },
    { provider: 'Sunrise Early Learning Center', fy: 2024, month: 4, amount: 36000, children: 45 },
    { provider: 'Sunrise Early Learning Center', fy: 2024, month: 5, amount: 38000, children: 48 },
    { provider: 'Sunrise Early Learning Center', fy: 2024, month: 6, amount: 42000, children: 52 },
    
    // Happy Hearts - small, steady
    { provider: 'Happy Hearts Home Daycare', fy: 2024, month: 1, amount: 4200, children: 6 },
    { provider: 'Happy Hearts Home Daycare', fy: 2024, month: 2, amount: 4200, children: 6 },
    { provider: 'Happy Hearts Home Daycare', fy: 2024, month: 3, amount: 4200, children: 6 },
    { provider: 'Happy Hearts Home Daycare', fy: 2024, month: 4, amount: 4500, children: 6 },
    { provider: 'Happy Hearts Home Daycare', fy: 2024, month: 5, amount: 4500, children: 6 },
    { provider: 'Happy Hearts Home Daycare', fy: 2024, month: 6, amount: 4500, children: 6 },
    
    // New Horizons - medium, growing
    { provider: 'New Horizons Childcare', fy: 2024, month: 1, amount: 18000, children: 22 },
    { provider: 'New Horizons Childcare', fy: 2024, month: 2, amount: 20000, children: 25 },
    { provider: 'New Horizons Childcare', fy: 2024, month: 3, amount: 22000, children: 28 },
    { provider: 'New Horizons Childcare', fy: 2024, month: 4, amount: 24000, children: 30 },
    { provider: 'New Horizons Childcare', fy: 2024, month: 5, amount: 25000, children: 30 },
    { provider: 'New Horizons Childcare', fy: 2024, month: 6, amount: 26000, children: 30 },
    
    // Bright Futures - new, rapid growth suspicious
    { provider: 'Bright Futures Family Care', fy: 2024, month: 1, amount: 2500, children: 3 },
    { provider: 'Bright Futures Family Care', fy: 2024, month: 2, amount: 5000, children: 6 },
    { provider: 'Bright Futures Family Care', fy: 2024, month: 3, amount: 8500, children: 10 },
    { provider: 'Bright Futures Family Care', fy: 2024, month: 4, amount: 12000, children: 14 },
    { provider: 'Bright Futures Family Care', fy: 2024, month: 5, amount: 15000, children: 18 },
    { provider: 'Bright Futures Family Care', fy: 2024, month: 6, amount: 18000, children: 22 },
    
    // ABC Learning - established, normal pattern
    { provider: 'ABC Learning Academy', fy: 2024, month: 1, amount: 25000, children: 30 },
    { provider: 'ABC Learning Academy', fy: 2024, month: 2, amount: 26000, children: 32 },
    { provider: 'ABC Learning Academy', fy: 2024, month: 3, amount: 27000, children: 33 },
    { provider: 'ABC Learning Academy', fy: 2024, month: 4, amount: 27500, children: 34 },
    { provider: 'ABC Learning Academy', fy: 2024, month: 5, amount: 28000, children: 35 },
    { provider: 'ABC Learning Academy', fy: 2024, month: 6, amount: 28000, children: 35 },
    
    // Community Kids - high, some concerns
    { provider: 'Community Kids Daycare', fy: 2024, month: 1, amount: 32000, children: 40 },
    { provider: 'Community Kids Daycare', fy: 2024, month: 2, amount: 35000, children: 44 },
    { provider: 'Community Kids Daycare', fy: 2024, month: 3, amount: 40000, children: 50 },
    { provider: 'Community Kids Daycare', fy: 2024, month: 4, amount: 45000, children: 55 },
    { provider: 'Community Kids Daycare', fy: 2024, month: 5, amount: 48000, children: 60 },
    { provider: 'Community Kids Daycare', fy: 2024, month: 6, amount: 52000, children: 65 },
  ];

  for (const payment of payments) {
    const providerId = providerIds[payment.provider];
    if (!providerId) continue;
    
    await runInsert(`
      INSERT INTO payments (provider_id, fiscal_year, fiscal_month, amount, children_served, 
        payment_type, funding_source, program_type)
      VALUES (?, ?, ?, ?, ?, 'CCDF Scholarship', 'Federal CCDF', 'Child Care Subsidy')
    `, [providerId, payment.fy, payment.month, payment.amount, payment.children]);
  }
  console.log(`  - Added ${payments.length} payment records`);

  // ========================================
  // FRAUD INDICATORS
  // ========================================
  console.log('\nAnalyzing for fraud indicators...');

  // Get providers with payments
  const providerStats = await query(`
    SELECT p.id, p.name, p.capacity, p.is_immigrant_owned,
      SUM(pay.amount) as total_payments,
      MAX(pay.children_served) as max_children,
      COUNT(pay.id) as payment_count,
      AVG(pay.amount) as avg_payment
    FROM providers p
    LEFT JOIN payments pay ON p.id = pay.provider_id
    GROUP BY p.id, p.name, p.capacity, p.is_immigrant_owned
  `);

  let fraudCount = 0;
  
  for (const p of providerStats) {
    const capacity = parseInt(p.capacity) || 0;
    const maxChildren = parseInt(p.max_children) || 0;
    const totalPayments = parseFloat(p.total_payments) || 0;
    const paymentCount = parseInt(p.payment_count) || 0;
    const isImmigrantOwned = p.is_immigrant_owned === 1 || p.is_immigrant_owned === '1';

    // Check for over-capacity billing
    if (capacity && maxChildren > capacity) {
      const overPercent = ((maxChildren - capacity) / capacity * 100).toFixed(0);
      const severity = maxChildren > capacity * 1.5 ? 'high' : 'medium';
      
      await runInsert(`
        INSERT INTO fraud_indicators (provider_id, indicator_type, severity, description, evidence, status)
        VALUES (?, 'over_capacity', ?, ?, ?, 'open')
      `, [
        p.id,
        severity,
        `Provider "${p.name}" billed for ${maxChildren} children but licensed capacity is only ${capacity} (${overPercent}% over)`,
        `Max children served: ${maxChildren}, Licensed capacity: ${capacity}`
      ]);
      fraudCount++;
      console.log(`  - Flagged over-capacity: ${p.name} (${overPercent}% over)`);
    }

    // Check for unusually high payments relative to capacity
    if (capacity && totalPayments) {
      const paymentPerCapacity = totalPayments / capacity / 6;
      if (paymentPerCapacity > 1000) {
        await runInsert(`
          INSERT INTO fraud_indicators (provider_id, indicator_type, severity, description, evidence, status)
          VALUES (?, 'high_payment_rate', 'medium', ?, ?, 'open')
        `, [
          p.id,
          `Provider "${p.name}" has unusually high payment rate of $${paymentPerCapacity.toFixed(0)}/child/month`,
          `Total payments: $${totalPayments.toLocaleString()}, Capacity: ${capacity}`
        ]);
        fraudCount++;
        console.log(`  - Flagged high payment rate: ${p.name}`);
      }
    }

    // Check for rapid payment growth
    if (paymentCount >= 6) {
      const firstPayment = await query(`
        SELECT amount FROM payments WHERE provider_id = ? ORDER BY fiscal_month ASC LIMIT 1
      `, [p.id]);
      const lastPayment = await query(`
        SELECT amount FROM payments WHERE provider_id = ? ORDER BY fiscal_month DESC LIMIT 1
      `, [p.id]);

      const firstAmount = parseFloat(firstPayment[0]?.amount) || 0;
      const lastAmount = parseFloat(lastPayment[0]?.amount) || 0;

      if (firstAmount > 0 && lastAmount > firstAmount * 2) {
        const growthPercent = ((lastAmount - firstAmount) / firstAmount * 100).toFixed(0);
        const severity = lastAmount > firstAmount * 4 ? 'high' : 'medium';
        
        await runInsert(`
          INSERT INTO fraud_indicators (provider_id, indicator_type, severity, description, evidence, status)
          VALUES (?, 'rapid_growth', ?, ?, ?, 'open')
        `, [
          p.id,
          severity,
          `Provider "${p.name}" shows ${growthPercent}% payment growth in 6 months - potential fraud indicator`,
          `First month: $${firstAmount.toLocaleString()}, Latest month: $${lastAmount.toLocaleString()}`
        ]);
        fraudCount++;
        console.log(`  - Flagged rapid growth: ${p.name} (${growthPercent}%)`);
      }
    }

    // Flag immigrant-owned providers with high payments for review
    if (isImmigrantOwned && totalPayments > 50000) {
      await runInsert(`
        INSERT INTO fraud_indicators (provider_id, indicator_type, severity, description, evidence, status)
        VALUES (?, 'review_recommended', 'low', ?, ?, 'open')
      `, [
        p.id,
        `Immigrant-owned provider "${p.name}" with significant CCDF payments - recommend compliance review`,
        `Total payments: $${totalPayments.toLocaleString()}, Owner background flagged for review`
      ]);
      fraudCount++;
    }
  }

  console.log(`  - Created ${fraudCount} fraud indicators`);

  // Save database (SQLite only)
  if (IS_LOCAL()) {
    await saveSqliteDb();
  }
  
  // ========================================
  // SUMMARY
  // ========================================
  console.log('\n' + '='.repeat(50));
  console.log('DATABASE SEED COMPLETE');
  console.log('='.repeat(50));
  
  // Get stats
  const stats = {
    providers: parseInt((await query('SELECT COUNT(*) as count FROM providers'))[0]?.count) || 0,
    payments: parseInt((await query('SELECT COUNT(*) as count FROM payments'))[0]?.count) || 0,
    totalPayments: parseFloat((await query('SELECT SUM(amount) as total FROM payments'))[0]?.total) || 0,
    fraudIndicators: parseInt((await query('SELECT COUNT(*) as count FROM fraud_indicators'))[0]?.count) || 0,
    immigrantOwned: parseInt((await query('SELECT COUNT(*) as count FROM providers WHERE is_immigrant_owned = 1'))[0]?.count) || 0,
  };

  console.log(`\nProviders: ${stats.providers} (${stats.immigrantOwned} immigrant-owned)`);
  console.log(`Payments: ${stats.payments}`);
  console.log(`Total Payment Value: $${stats.totalPayments.toLocaleString()}`);
  console.log(`Fraud Indicators: ${stats.fraudIndicators}`);
  console.log('\nCCDF Program Context:');
  console.log('  - NH receives ~$30M annually in CCDF funds');
  console.log('  - ~500 providers participate in scholarship program');
  console.log('  - Federal oversight increasing due to fraud in other states');

  if (!skipInit) {
    closeDb();
  }
}

// Run directly if called as a script
const isMain = process.argv[1]?.includes('seed');

if (isMain) {
  seedDatabase().catch(console.error);
}
