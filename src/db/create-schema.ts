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

    `CREATE TABLE IF NOT EXISTS contractors (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      vendor_code TEXT UNIQUE,
      name TEXT NOT NULL,
      dba_name TEXT,
      address TEXT,
      city TEXT,
      state TEXT,
      zip TEXT,
      is_immigrant_related INTEGER DEFAULT 0,
      is_immigrant_owned INTEGER DEFAULT 0,
      owner_background TEXT,
      vendor_id TEXT,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`,

    `CREATE TABLE IF NOT EXISTS contracts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      contractor_id INTEGER REFERENCES contractors(id),
      provider_id INTEGER REFERENCES providers(id),
      contract_number TEXT,
      title TEXT,
      description TEXT,
      department TEXT DEFAULT 'Health and Human Services',
      agency TEXT,
      start_date TEXT,
      end_date TEXT,
      original_amount REAL,
      current_amount REAL,
      amendment_count INTEGER DEFAULT 0,
      funding_source TEXT,
      procurement_type TEXT,
      gc_approval_date TEXT,
      gc_agenda_item TEXT,
      status TEXT,
      contract_type TEXT,
      source_url TEXT,
      source_document TEXT,
      approval_date TEXT,
      gc_item_number TEXT,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`,

    `CREATE TABLE IF NOT EXISTS expenditures (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_id INTEGER REFERENCES providers(id),
      contractor_id INTEGER REFERENCES contractors(id),
      contract_id INTEGER REFERENCES contracts(id),
      fiscal_year INTEGER,
      department TEXT,
      agency TEXT,
      activity TEXT,
      expense_class TEXT,
      vendor_name TEXT NOT NULL,
      amount REAL NOT NULL,
      payment_date TEXT,
      check_number TEXT,
      description TEXT,
      source_url TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

    // Provider Master tables (CCIS as authoritative source)
    `CREATE TABLE IF NOT EXISTS provider_master (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ccis_provider_id TEXT UNIQUE,
      canonical_name TEXT NOT NULL,
      name_display TEXT NOT NULL,
      address_normalized TEXT,
      address_display TEXT,
      city TEXT,
      state TEXT DEFAULT 'NH',
      zip TEXT,
      zip5 TEXT,
      phone_normalized TEXT,
      email TEXT,
      provider_type TEXT,
      license_number TEXT,
      capacity INTEGER,
      accepts_ccdf INTEGER DEFAULT 0,
      quality_rating TEXT,
      is_active INTEGER DEFAULT 1,
      is_immigrant_owned INTEGER DEFAULT 0,
      first_seen_date TEXT,
      last_verified_date TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`,

    `CREATE TABLE IF NOT EXISTS provider_aliases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_master_id INTEGER NOT NULL REFERENCES provider_master(id),
      alias_name TEXT NOT NULL,
      alias_normalized TEXT NOT NULL,
      alias_type TEXT,
      source TEXT,
      confidence REAL DEFAULT 1.0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(provider_master_id, alias_normalized)
    )`,

    `CREATE TABLE IF NOT EXISTS provider_source_links (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_master_id INTEGER NOT NULL REFERENCES provider_master(id),
      source_system TEXT NOT NULL,
      source_identifier TEXT NOT NULL,
      source_name TEXT,
      match_method TEXT,
      match_score REAL,
      match_details TEXT,
      verified_by TEXT,
      verified_at TEXT,
      status TEXT DEFAULT 'active',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(source_system, source_identifier)
    )`,

    `CREATE TABLE IF NOT EXISTS match_audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider_master_id INTEGER REFERENCES provider_master(id),
      source_system TEXT NOT NULL,
      source_identifier TEXT NOT NULL,
      source_name TEXT,
      action TEXT NOT NULL,
      match_score REAL,
      match_method TEXT,
      match_details TEXT,
      performed_by TEXT DEFAULT 'system',
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`,

    `CREATE TABLE IF NOT EXISTS pending_matches (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_system TEXT NOT NULL,
      source_identifier TEXT NOT NULL,
      source_name TEXT,
      source_address TEXT,
      source_city TEXT,
      source_zip TEXT,
      candidate_provider_id INTEGER REFERENCES provider_master(id),
      match_score REAL,
      match_details TEXT,
      status TEXT DEFAULT 'pending',
      reviewed_by TEXT,
      reviewed_at TEXT,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(source_system, source_identifier, candidate_provider_id)
    )`,

    // Provider master indexes
    `CREATE INDEX IF NOT EXISTS idx_pm_ccis_id ON provider_master(ccis_provider_id)`,
    `CREATE INDEX IF NOT EXISTS idx_pm_name ON provider_master(canonical_name)`,
    `CREATE INDEX IF NOT EXISTS idx_pm_city_zip ON provider_master(city, zip5)`,
    `CREATE INDEX IF NOT EXISTS idx_pm_license ON provider_master(license_number)`,
    `CREATE INDEX IF NOT EXISTS idx_pa_normalized ON provider_aliases(alias_normalized)`,
    `CREATE INDEX IF NOT EXISTS idx_pa_master ON provider_aliases(provider_master_id)`,
    `CREATE INDEX IF NOT EXISTS idx_psl_master ON provider_source_links(provider_master_id)`,
    `CREATE INDEX IF NOT EXISTS idx_psl_source ON provider_source_links(source_system, source_identifier)`,
    `CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_matches(status)`,
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