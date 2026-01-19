// Simple test script to check Turso database status
const { createClient } = require('@libsql/client');
const fs = require('fs');
const path = require('path');

// Load environment variables
const envPath = path.join(__dirname, '.env.local');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  const lines = envContent.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const [key, ...valueParts] = trimmed.split('=');
      if (key && valueParts.length > 0) {
        const value = valueParts.join('=').replace(/^["']|["']$/g, '');
        process.env[key] = value;
      }
    }
  }
}

async function testDatabase() {
  console.log('Testing Turso database...');

  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoUrl || !tursoToken) {
    console.error('Environment variables not set');
    return;
  }

  try {
    const client = createClient({
      url: tursoUrl,
      authToken: tursoToken,
    });

    // Check tables
    const tablesResult = await client.execute(`
      SELECT name FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `);

    console.log(`Found ${tablesResult.rows.length} tables:`, tablesResult.rows.map(row => row.name));

    // Check data
    if (tablesResult.rows.some(row => row.name === 'providers')) {
      const providersResult = await client.execute('SELECT COUNT(*) as count FROM providers');
      console.log(`Providers: ${providersResult.rows[0].count}`);
    }

    if (tablesResult.rows.some(row => row.name === 'payments')) {
      const paymentsResult = await client.execute('SELECT COUNT(*) as count FROM payments');
      console.log(`Payments: ${paymentsResult.rows[0].count}`);
    }

    if (tablesResult.rows.some(row => row.name === 'fraud_indicators')) {
      const fraudResult = await client.execute('SELECT COUNT(*) as count FROM fraud_indicators');
      console.log(`Fraud indicators: ${fraudResult.rows[0].count}`);
    }

    client.close();
    console.log('Database test completed');

  } catch (error) {
    console.error('Database test failed:', error.message);
  }
}

testDatabase();