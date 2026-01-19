/**
 * Simple schema creation script for Turso
 */

import { createClient } from '@libsql/client';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Load environment variables from .env.local
function loadEnvFile() {
  const envPath = path.join(__dirname, '../../.env.local');
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    const lines = envContent.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith('#')) {
        const [key, ...valueParts] = trimmed.split('=');
        if (key && valueParts.length > 0) {
          const value = valueParts.join('=').replace(/^["']|["']$/g, ''); // Remove quotes
          process.env[key] = value;
        }
      }
    }
  }
}

async function createSchema() {
  console.log('Creating Turso schema...');

  loadEnvFile();

  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoUrl || !tursoToken) {
    throw new Error('Environment variables not set');
  }

  const client = createClient({
    url: tursoUrl,
    authToken: tursoToken,
  });

  // Simple schema creation statements
  const statements = [
    `CREATE TABLE IF NOT EXISTS providers (
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
    )`,

    `CREATE TABLE IF NOT EXISTS payments (
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
    )`,

    `CREATE TABLE IF NOT EXISTS fraud_indicators (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_id INTEGER,
      payment_id INTEGER,
      contract_id INTEGER,
      contractor_id INTEGER,
      expenditure_id INTEGER,
      indicator_type TEXT NOT NULL,
      severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
      description TEXT NOT NULL,
      evidence TEXT,
      status TEXT DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved', 'dismissed')),
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (provider_id) REFERENCES providers(id),
      FOREIGN KEY (payment_id) REFERENCES payments(id),
      FOREIGN KEY (contract_id) REFERENCES contracts(id),
      FOREIGN KEY (contractor_id) REFERENCES contractors(id),
      FOREIGN KEY (expenditure_id) REFERENCES expenditures(id)
    )`,

    // Create indexes
    `CREATE INDEX IF NOT EXISTS idx_providers_name ON providers(name)`,
    `CREATE INDEX IF NOT EXISTS idx_providers_city ON providers(city)`,
    `CREATE INDEX IF NOT EXISTS idx_providers_immigrant ON providers(is_immigrant_owned)`,
    `CREATE INDEX IF NOT EXISTS idx_payments_provider ON payments(provider_id)`,
    `CREATE INDEX IF NOT EXISTS idx_payments_fiscal ON payments(fiscal_year, fiscal_month)`,
    `CREATE INDEX IF NOT EXISTS idx_fraud_provider ON fraud_indicators(provider_id)`,
    `CREATE INDEX IF NOT EXISTS idx_fraud_severity ON fraud_indicators(severity)`,
    `CREATE INDEX IF NOT EXISTS idx_fraud_status ON fraud_indicators(status)`,
  ];

  for (const sql of statements) {
    try {
      await client.execute(sql);
      console.log('✓ Created table/index');
    } catch (error: any) {
      console.error('Error:', error.message);
      console.error('SQL:', sql.substring(0, 100) + '...');
    }
  }

  client.close();
  console.log('Schema creation completed');
}

// Run it
createSchema().catch(console.error);