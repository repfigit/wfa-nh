// Database schema for NH Childcare/Daycare Payments Tracker

import { dataSources } from './data-sources.js';

export const schema = `
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
);

CREATE INDEX IF NOT EXISTS idx_providers_name ON providers(name);
CREATE INDEX IF NOT EXISTS idx_providers_city ON providers(city);
CREATE INDEX IF NOT EXISTS idx_providers_license ON providers(license_number);
CREATE INDEX IF NOT EXISTS idx_providers_immigrant ON providers(is_immigrant_owned);
CREATE INDEX IF NOT EXISTS idx_providers_ccdf ON providers(accepts_ccdf);

-- CCDF/Scholarship Payments to Providers
CREATE TABLE IF NOT EXISTS payments (
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
);

CREATE INDEX IF NOT EXISTS idx_payments_provider ON payments(provider_id);
CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_payments_fiscal ON payments(fiscal_year, fiscal_month);
CREATE INDEX IF NOT EXISTS idx_payments_amount ON payments(amount);

-- Keep contractors table for refugee resettlement agencies
CREATE TABLE IF NOT EXISTS contractors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_code TEXT UNIQUE,
  name TEXT NOT NULL,
  address TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  is_immigrant_related INTEGER DEFAULT 0,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contractors_name ON contractors(name);
CREATE INDEX IF NOT EXISTS idx_contractors_vendor_code ON contractors(vendor_code);

-- Contracts (for larger childcare program contracts)
CREATE TABLE IF NOT EXISTS contracts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contractor_id INTEGER,
  provider_id INTEGER,
  contract_number TEXT,
  title TEXT NOT NULL,
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
  source_url TEXT,
  source_document TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (contractor_id) REFERENCES contractors(id),
  FOREIGN KEY (provider_id) REFERENCES providers(id)
);

CREATE INDEX IF NOT EXISTS idx_contracts_contractor ON contracts(contractor_id);
CREATE INDEX IF NOT EXISTS idx_contracts_provider ON contracts(provider_id);
CREATE INDEX IF NOT EXISTS idx_contracts_dates ON contracts(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_contracts_amount ON contracts(current_amount);

-- Expenditures (from TransparentNH)
CREATE TABLE IF NOT EXISTS expenditures (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contractor_id INTEGER,
  provider_id INTEGER,
  contract_id INTEGER,
  fiscal_year INTEGER,
  department TEXT,
  agency TEXT,
  activity TEXT,
  expense_class TEXT,
  vendor_name TEXT NOT NULL,
  amount REAL NOT NULL,
  payment_date TEXT,
  description TEXT,
  source_url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (contractor_id) REFERENCES contractors(id),
  FOREIGN KEY (provider_id) REFERENCES providers(id),
  FOREIGN KEY (contract_id) REFERENCES contracts(id)
);

CREATE INDEX IF NOT EXISTS idx_expenditures_contractor ON expenditures(contractor_id);
CREATE INDEX IF NOT EXISTS idx_expenditures_provider ON expenditures(provider_id);
CREATE INDEX IF NOT EXISTS idx_expenditures_fiscal_year ON expenditures(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_expenditures_vendor ON expenditures(vendor_name);

-- Fraud Indicators
CREATE TABLE IF NOT EXISTS fraud_indicators (
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
);

CREATE INDEX IF NOT EXISTS idx_fraud_provider ON fraud_indicators(provider_id);
CREATE INDEX IF NOT EXISTS idx_fraud_contract ON fraud_indicators(contract_id);
CREATE INDEX IF NOT EXISTS idx_fraud_severity ON fraud_indicators(severity);
CREATE INDEX IF NOT EXISTS idx_fraud_status ON fraud_indicators(status);
CREATE INDEX IF NOT EXISTS idx_fraud_type ON fraud_indicators(indicator_type);

-- Data Sources
CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL,
  type TEXT NOT NULL,
  last_scraped TEXT,
  notes TEXT
);

-- Scraped Documents
CREATE TABLE IF NOT EXISTS scraped_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data_source_id INTEGER,
  url TEXT NOT NULL,
  document_type TEXT,
  document_date TEXT,
  title TEXT,
  raw_content TEXT,
  parsed_data TEXT,
  scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
  processed INTEGER DEFAULT 0,
  FOREIGN KEY (data_source_id) REFERENCES data_sources(id)
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

-- Keywords for identifying immigrant-related providers
CREATE TABLE IF NOT EXISTS keywords (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  keyword TEXT NOT NULL UNIQUE,
  category TEXT,
  weight REAL DEFAULT 1.0
);

-- Ingestion run tracking for scheduled data collection
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
`;

export const seedData = `
-- Insert keywords for childcare fraud detection
INSERT OR IGNORE INTO keywords (keyword, category, weight) VALUES
  ('refugee', 'immigration', 2.0),
  ('immigrant', 'immigration', 2.0),
  ('somali', 'ethnicity', 2.5),
  ('arabic', 'language', 2.0),
  ('swahili', 'language', 2.0),
  ('nepali', 'language', 2.0),
  ('bhutanese', 'ethnicity', 2.0),
  ('african', 'ethnicity', 1.5),
  ('congolese', 'ethnicity', 2.0),
  ('sudanese', 'ethnicity', 2.0),
  ('islamic', 'cultural', 1.5),
  ('muslim', 'cultural', 1.5),
  ('halal', 'cultural', 1.5),
  ('multilingual', 'language', 1.0),
  ('bilingual', 'language', 1.0),
  ('cultural', 'cultural', 0.8),
  ('community', 'generic', 0.5),
  ('family daycare', 'provider_type', 1.0),
  ('family childcare', 'provider_type', 1.0),
  ('home daycare', 'provider_type', 1.0),
  ('in-home', 'provider_type', 1.0),
  ('resettlement', 'immigration', 2.0),
  ('newcomer', 'immigration', 1.5);

-- Insert data sources for childcare tracking
${dataSources.map(ds => `INSERT OR IGNORE INTO data_sources (name, url, type, notes) VALUES ('${ds.name}', '${ds.url}', '${ds.type}', '${ds.notes}');`).join('\n')}
`;
