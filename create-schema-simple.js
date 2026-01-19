// Simple synchronous schema creation
const { createClient } = require('@libsql/client');
const fs = require('fs');
const path = require('path');

// Load environment variables
const envPath = path.join(__dirname, '../../.env.local');
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

async function createSchema() {
  console.log('Creating schema...');

  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoUrl || !tursoToken) {
    console.error('Environment variables not set');
    process.exit(1);
  }

  try {
    const client = createClient({
      url: tursoUrl,
      authToken: tursoToken,
    });

    // Create providers table
    await client.execute(`CREATE TABLE IF NOT EXISTS providers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_id TEXT UNIQUE,
      name TEXT NOT NULL,
      dba_name TEXT,
      address TEXT,
      city TEXT,
      state TEXT DEFAULT 'NH',
      zip TEXT,
      phone TEXT,
      email TEXT,
      license_number TEXT,
      license_type TEXT,
      license_status TEXT,
      capacity INTEGER,
      provider_type TEXT,
      is_immigrant_owned INTEGER DEFAULT 0,
      owner_name TEXT,
      owner_background TEXT,
      language_services TEXT,
      accepts_ccdf INTEGER DEFAULT 0,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`);

    console.log('Created providers table');

    // Create payments table
    await client.execute(`CREATE TABLE IF NOT EXISTS payments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_id INTEGER NOT NULL,
      payment_date TEXT,
      fiscal_year INTEGER,
      fiscal_month INTEGER,
      amount REAL NOT NULL,
      payment_type TEXT,
      funding_source TEXT,
      children_served INTEGER,
      attendance_hours REAL,
      program_type TEXT,
      description TEXT,
      source_url TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (provider_id) REFERENCES providers(id)
    )`);

    console.log('Created payments table');

    // Create fraud_indicators table
    await client.execute(`CREATE TABLE IF NOT EXISTS fraud_indicators (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_id INTEGER,
      payment_id INTEGER,
      contract_id INTEGER,
      contractor_id INTEGER,
      expenditure_id INTEGER,
      indicator_type TEXT NOT NULL,
      severity TEXT NOT NULL,
      description TEXT NOT NULL,
      evidence TEXT,
      status TEXT DEFAULT 'open',
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`);

    console.log('Created fraud_indicators table');

    // Create indexes
    await client.execute(`CREATE INDEX IF NOT EXISTS idx_providers_name ON providers(name)`);
    await client.execute(`CREATE INDEX IF NOT EXISTS idx_payments_provider ON payments(provider_id)`);
    await client.execute(`CREATE INDEX IF NOT EXISTS idx_fraud_provider ON fraud_indicators(provider_id)`);

    console.log('Created indexes');

    client.close();
    console.log('Schema creation completed successfully!');

  } catch (error) {
    console.error('Schema creation failed:', error.message);
    process.exit(1);
  }
}

createSchema();