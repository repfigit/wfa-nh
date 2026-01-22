/**
 * Consolidated Turso/libSQL Schema for NH Childcare Payments Tracker
 */

export const sqliteSchema = `
-- 1. PROVIDER MASTER DATA (Authoritative)
CREATE TABLE IF NOT EXISTS provider_master (
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
);

CREATE INDEX IF NOT EXISTS idx_pm_name ON provider_master(canonical_name);
CREATE INDEX IF NOT EXISTS idx_pm_city ON provider_master(city);

-- 2. PROVIDER ALIASES & LINKS
CREATE TABLE IF NOT EXISTS provider_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER NOT NULL REFERENCES provider_master(id),
  alias_name TEXT NOT NULL,
  alias_normalized TEXT NOT NULL,
  alias_type TEXT CHECK (alias_type IN ('dba', 'former_name', 'variant', 'vendor_name')),
  source TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(provider_master_id, alias_normalized)
);

CREATE TABLE IF NOT EXISTS provider_source_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER NOT NULL REFERENCES provider_master(id),
  source_system TEXT NOT NULL,
  source_identifier TEXT NOT NULL,
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(source_system, source_identifier)
);

-- 3. TRANSACTIONAL DATA (Payments, Contracts, Expenditures)
CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER REFERENCES provider_master(id),
  payment_date TEXT,
  fiscal_year INTEGER,
  amount REAL NOT NULL,
  funding_source TEXT,
  children_served INTEGER,
  description TEXT,
  source_url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contractors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_code TEXT UNIQUE,
  name TEXT NOT NULL,
  city TEXT,
  state TEXT,
  is_immigrant_related INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contracts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contractor_id INTEGER REFERENCES contractors(id),
  provider_master_id INTEGER REFERENCES provider_master(id),
  contract_number TEXT,
  title TEXT,
  start_date TEXT,
  end_date TEXT,
  amount REAL,
  source_url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS expenditures (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER REFERENCES provider_master(id),
  contractor_id INTEGER REFERENCES contractors(id),
  fiscal_year INTEGER,
  vendor_name TEXT NOT NULL,
  amount REAL NOT NULL,
  payment_date TEXT,
  description TEXT,
  source_url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 4. ANALYSIS & AUDIT
CREATE TABLE IF NOT EXISTS fraud_indicators (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER REFERENCES provider_master(id),
  indicator_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
  description TEXT NOT NULL,
  status TEXT DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved', 'dismissed')),
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS match_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER REFERENCES provider_master(id),
  source_system TEXT NOT NULL,
  source_identifier TEXT NOT NULL,
  action TEXT NOT NULL,
  match_score REAL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pending_matches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_system TEXT NOT NULL,
  source_identifier TEXT NOT NULL,
  source_name TEXT,
  candidate_provider_id INTEGER REFERENCES provider_master(id),
  status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 5. INFRASTRUCTURE & SCRAPING
CREATE TABLE IF NOT EXISTS scraped_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_key TEXT NOT NULL,
  url TEXT NOT NULL,
  title TEXT,
  raw_content TEXT,
  scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
  processed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  completed_at TEXT,
  records_processed INTEGER DEFAULT 0,
  error_message TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 6. BROWSEABLE SOURCE LANDING TABLES
-- Columns match the CSV export from NH CCIS website exactly
CREATE TABLE IF NOT EXISTS source_ccis (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  program_name TEXT,
  status TEXT,
  phone TEXT,
  email TEXT,
  region TEXT,
  county TEXT,
  street TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  record_type TEXT,
  gsq_step TEXT,
  provider_number TEXT,
  license_date TEXT,
  license_type TEXT,
  accepts_scholarship TEXT,
  accredited TEXT,
  capacity TEXT,
  age_groups TEXT,
  enrollment TEXT,
  loaded_at TEXT DEFAULT CURRENT_TIMESTAMP
);
`;

export default {
  sqliteSchema,
};
