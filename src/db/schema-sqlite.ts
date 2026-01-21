/**
 * Turso/libSQL Schema for NH Childcare Payments Tracker
 * Note: Turso uses SQLite syntax
 */

export const sqliteSchema = `
-- Childcare Providers (Daycares)
CREATE TABLE IF NOT EXISTS providers (
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
  website TEXT,
  license_number TEXT,
  license_type TEXT,
  license_status TEXT,
  capacity INTEGER,
  age_range TEXT,
  hours_operation TEXT,
  provider_type TEXT,
  is_immigrant_owned INTEGER DEFAULT 0,
  owner_name TEXT,
  owner_background TEXT,
  language_services TEXT,
  accepts_ccdf INTEGER DEFAULT 0,
  ccdf_provider_id TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_providers_name ON providers(name);
CREATE INDEX IF NOT EXISTS idx_providers_city ON providers(city);
CREATE INDEX IF NOT EXISTS idx_providers_license ON providers(license_number);
CREATE INDEX IF NOT EXISTS idx_providers_immigrant ON providers(is_immigrant_owned);
CREATE INDEX IF NOT EXISTS idx_providers_ccdf ON providers(accepts_ccdf);

-- CCDF Scholarship Payments
CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_id INTEGER REFERENCES providers(id),
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
  check_number TEXT,
  source_url TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_payments_provider ON payments(provider_id);
CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_payments_fiscal ON payments(fiscal_year, fiscal_month);
CREATE INDEX IF NOT EXISTS idx_payments_amount ON payments(amount);

-- Contractors (Vendors from contracts)
CREATE TABLE IF NOT EXISTS contractors (
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
);

CREATE INDEX IF NOT EXISTS idx_contractors_name ON contractors(name);
CREATE INDEX IF NOT EXISTS idx_contractors_vendor_code ON contractors(vendor_code);

-- State Contracts
CREATE TABLE IF NOT EXISTS contracts (
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
);

CREATE INDEX IF NOT EXISTS idx_contracts_contractor ON contracts(contractor_id);
CREATE INDEX IF NOT EXISTS idx_contracts_provider ON contracts(provider_id);
CREATE INDEX IF NOT EXISTS idx_contracts_dates ON contracts(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_contracts_amount ON contracts(current_amount);

-- Expenditure Records (from TransparentNH)
CREATE TABLE IF NOT EXISTS expenditures (
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
);

CREATE INDEX IF NOT EXISTS idx_expenditures_contractor ON expenditures(contractor_id);
CREATE INDEX IF NOT EXISTS idx_expenditures_provider ON expenditures(provider_id);
CREATE INDEX IF NOT EXISTS idx_expenditures_fiscal_year ON expenditures(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_expenditures_vendor ON expenditures(vendor_name);

-- Fraud Indicators
CREATE TABLE IF NOT EXISTS fraud_indicators (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_id INTEGER REFERENCES providers(id),
  contract_id INTEGER REFERENCES contracts(id),
  contractor_id INTEGER REFERENCES contractors(id),
  expenditure_id INTEGER REFERENCES expenditures(id),
  payment_id INTEGER REFERENCES payments(id),
  indicator_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
  description TEXT NOT NULL,
  evidence TEXT,
  amount_flagged REAL,
  status TEXT DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved', 'dismissed')),
  reviewed_by TEXT,
  reviewed_at TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fraud_provider ON fraud_indicators(provider_id);
CREATE INDEX IF NOT EXISTS idx_fraud_contract ON fraud_indicators(contract_id);
CREATE INDEX IF NOT EXISTS idx_fraud_severity ON fraud_indicators(severity);
CREATE INDEX IF NOT EXISTS idx_fraud_status ON fraud_indicators(status);
CREATE INDEX IF NOT EXISTS idx_fraud_type ON fraud_indicators(indicator_type);

-- Data Sources Reference
CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL,
  type TEXT NOT NULL,
  last_scraped TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Scrape Logs
CREATE TABLE IF NOT EXISTS scrape_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data_source_id INTEGER REFERENCES data_sources(id),
  started_at TEXT DEFAULT CURRENT_TIMESTAMP,
  completed_at TEXT,
  records_found INTEGER DEFAULT 0,
  records_imported INTEGER DEFAULT 0,
  status TEXT DEFAULT 'running',
  error_message TEXT
);

-- Scrape Jobs (real-time progress tracking)
CREATE TABLE IF NOT EXISTS scrape_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scraper_name TEXT NOT NULL,
  status TEXT DEFAULT 'pending',
  progress INTEGER DEFAULT 0,
  total_steps INTEGER DEFAULT 100,
  current_step TEXT,
  records_found INTEGER DEFAULT 0,
  records_imported INTEGER DEFAULT 0,
  errors TEXT,
  started_at TEXT DEFAULT CURRENT_TIMESTAMP,
  completed_at TEXT,
  result_summary TEXT
);

-- Scraped Documents
CREATE TABLE IF NOT EXISTS scraped_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data_source_id INTEGER REFERENCES data_sources(id),
  url TEXT NOT NULL,
  document_type TEXT,
  document_date TEXT,
  title TEXT,
  raw_content TEXT,
  parsed_data TEXT,
  scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
  processed INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_scraped_url ON scraped_documents(url);

-- Audit log
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  record_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  old_values TEXT,
  new_values TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Keyword Analysis for detecting childcare-related entries
CREATE TABLE IF NOT EXISTS keywords (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  keyword TEXT NOT NULL UNIQUE,
  category TEXT,
  weight REAL DEFAULT 1.0
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_providers_immigrant ON providers(is_immigrant_owned);
CREATE INDEX IF NOT EXISTS idx_providers_city ON providers(city);
CREATE INDEX IF NOT EXISTS idx_providers_ccdf ON providers(accepts_ccdf);
CREATE INDEX IF NOT EXISTS idx_payments_provider ON payments(provider_id);
CREATE INDEX IF NOT EXISTS idx_payments_fiscal ON payments(fiscal_year, fiscal_month);
CREATE INDEX IF NOT EXISTS idx_expenditures_vendor ON expenditures(vendor_name);
CREATE INDEX IF NOT EXISTS idx_expenditures_fiscal ON expenditures(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_fraud_provider ON fraud_indicators(provider_id);
CREATE INDEX IF NOT EXISTS idx_fraud_severity ON fraud_indicators(severity);
CREATE INDEX IF NOT EXISTS idx_fraud_status ON fraud_indicators(status);
CREATE INDEX IF NOT EXISTS idx_contracts_contractor ON contracts(contractor_id);

-- Ingestion tracking for scheduled collection
CREATE TABLE IF NOT EXISTS ingestion_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
  started_at TEXT NOT NULL,
  completed_at TEXT,
  records_processed INTEGER DEFAULT 0,
  records_imported INTEGER DEFAULT 0,
  details TEXT,
  error_message TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingestion_source ON ingestion_runs(source);
CREATE INDEX IF NOT EXISTS idx_ingestion_status ON ingestion_runs(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_started ON ingestion_runs(started_at);

-- =====================================================
-- PROVIDER MASTER DATA (CCIS as authoritative source)
-- =====================================================

-- Provider Master Record (CCIS-sourced, authoritative)
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

CREATE INDEX IF NOT EXISTS idx_pm_ccis_id ON provider_master(ccis_provider_id);
CREATE INDEX IF NOT EXISTS idx_pm_name ON provider_master(canonical_name);
CREATE INDEX IF NOT EXISTS idx_pm_city_zip ON provider_master(city, zip5);
CREATE INDEX IF NOT EXISTS idx_pm_license ON provider_master(license_number);
CREATE INDEX IF NOT EXISTS idx_pm_immigrant ON provider_master(is_immigrant_owned);

-- Provider Aliases (alternative names, DBAs, historical names)
CREATE TABLE IF NOT EXISTS provider_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER NOT NULL REFERENCES provider_master(id),
  alias_name TEXT NOT NULL,
  alias_normalized TEXT NOT NULL,
  alias_type TEXT CHECK (alias_type IN ('dba', 'former_name', 'variant', 'abbreviation', 'vendor_name')),
  source TEXT,
  confidence REAL DEFAULT 1.0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(provider_master_id, alias_normalized)
);

CREATE INDEX IF NOT EXISTS idx_pa_normalized ON provider_aliases(alias_normalized);
CREATE INDEX IF NOT EXISTS idx_pa_master ON provider_aliases(provider_master_id);

-- External Source Links (bridge table for all external identifiers)
CREATE TABLE IF NOT EXISTS provider_source_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER NOT NULL REFERENCES provider_master(id),
  source_system TEXT NOT NULL CHECK (source_system IN (
    'ccis', 'licensing', 'transparent_nh', 'das_contracts',
    'propublica_990', 'usaspending', 'manual', 'legacy'
  )),
  source_identifier TEXT NOT NULL,
  source_name TEXT,
  match_method TEXT,
  match_score REAL,
  match_details TEXT,
  verified_by TEXT,
  verified_at TEXT,
  status TEXT DEFAULT 'active' CHECK (status IN ('active', 'superseded', 'rejected')),
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(source_system, source_identifier)
);

CREATE INDEX IF NOT EXISTS idx_psl_master ON provider_source_links(provider_master_id);
CREATE INDEX IF NOT EXISTS idx_psl_source ON provider_source_links(source_system, source_identifier);
CREATE INDEX IF NOT EXISTS idx_psl_status ON provider_source_links(status);

-- Match Audit Log (tracks all matching decisions)
CREATE TABLE IF NOT EXISTS match_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_master_id INTEGER REFERENCES provider_master(id),
  source_system TEXT NOT NULL,
  source_identifier TEXT NOT NULL,
  source_name TEXT,
  action TEXT NOT NULL CHECK (action IN ('matched', 'created', 'rejected', 'manual_link', 'manual_unlink', 'updated')),
  match_score REAL,
  match_method TEXT,
  match_details TEXT,
  performed_by TEXT DEFAULT 'system',
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mal_master ON match_audit_log(provider_master_id);
CREATE INDEX IF NOT EXISTS idx_mal_source ON match_audit_log(source_system, source_identifier);
CREATE INDEX IF NOT EXISTS idx_mal_action ON match_audit_log(action);

-- Pending Matches (for manual review)
CREATE TABLE IF NOT EXISTS pending_matches (
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
  status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'skipped')),
  reviewed_by TEXT,
  reviewed_at TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(source_system, source_identifier, candidate_provider_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_matches(status);
CREATE INDEX IF NOT EXISTS idx_pending_source ON pending_matches(source_system);
`;

import { dataSources } from './data-sources.js';

export const sqliteDataSources = `
INSERT OR IGNORE INTO data_sources (name, url, type, notes) VALUES
${dataSources.map(ds => `  ('${ds.name}', '${ds.url}', '${ds.type}', '${ds.notes}')`).join(',\n')};
`;

export const sqliteKeywords = `
INSERT OR IGNORE INTO keywords (keyword, category, weight) VALUES
  ('daycare', 'childcare', 2.0),
  ('day care', 'childcare', 2.0),
  ('child care', 'childcare', 2.0),
  ('childcare', 'childcare', 2.0),
  ('early learning', 'childcare', 1.5),
  ('early childhood', 'childcare', 1.5),
  ('preschool', 'childcare', 1.5),
  ('head start', 'childcare', 2.0),
  ('ccdf', 'funding', 2.5),
  ('child development', 'childcare', 1.5),
  ('nursery', 'childcare', 1.0),
  ('after school', 'childcare', 1.0),
  ('before school', 'childcare', 1.0),
  ('somali', 'immigration', 2.0),
  ('refugee', 'immigration', 2.0),
  ('immigrant', 'immigration', 1.5),
  ('resettlement', 'immigration', 1.5),
  ('multicultural', 'immigration', 1.0),
  ('translation', 'immigration', 1.0),
  ('interpreter', 'immigration', 1.0),
  ('dhhs', 'department', 1.5),
  ('health and human services', 'department', 1.5),
  ('community', 'generic', 0.5),
  ('family daycare', 'provider_type', 1.0),
  ('family childcare', 'provider_type', 1.0),
  ('home daycare', 'provider_type', 1.0),
  ('in-home', 'provider_type', 1.0),
  ('newcomer', 'immigration', 1.5);
`;

export default {
  sqliteSchema,
  sqliteDataSources,
  sqliteKeywords,
};
