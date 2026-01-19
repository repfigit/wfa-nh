/**
 * Turso/libSQL Schema for NH Childcare Payments Tracker
 * Note: Turso uses SQLite syntax, not PostgreSQL
 */

export const postgresSchema = `
-- Childcare Providers (Daycares)
CREATE TABLE IF NOT EXISTS providers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
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

-- CCDF Scholarship Payments
CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_id INTEGER REFERENCES providers(id),
  fiscal_year INTEGER,
  fiscal_month INTEGER,
  amount REAL,
  children_served INTEGER,
  payment_type TEXT,
  funding_source TEXT,
  program_type TEXT,
  check_number TEXT,
  payment_date TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Contractors (Vendors from contracts)
CREATE TABLE IF NOT EXISTS contractors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  dba_name TEXT,
  address TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  is_immigrant_owned INTEGER DEFAULT 0,
  owner_background TEXT,
  vendor_id TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- State Contracts
CREATE TABLE IF NOT EXISTS contracts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  contractor_id INTEGER REFERENCES contractors(id),
  contract_number TEXT,
  description TEXT,
  department TEXT,
  agency TEXT,
  start_date TEXT,
  end_date TEXT,
  original_amount REAL,
  current_amount REAL,
  status TEXT,
  contract_type TEXT,
  source_url TEXT,
  approval_date TEXT,
  gc_item_number TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

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
  vendor_name TEXT,
  amount REAL,
  payment_date TEXT,
  check_number TEXT,
  description TEXT,
  source_url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Fraud Indicators
CREATE TABLE IF NOT EXISTS fraud_indicators (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider_id INTEGER REFERENCES providers(id),
  contract_id INTEGER REFERENCES contracts(id),
  contractor_id INTEGER REFERENCES contractors(id),
  expenditure_id INTEGER REFERENCES expenditures(id),
  payment_id INTEGER REFERENCES payments(id),
  indicator_type TEXT NOT NULL,
  severity TEXT DEFAULT 'medium',
  description TEXT,
  evidence TEXT,
  amount_flagged REAL,
  status TEXT DEFAULT 'open',
  reviewed_by TEXT,
  reviewed_at TEXT,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Data Sources Reference
CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  url TEXT,
  type TEXT,
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
`;

import { dataSources } from './data-sources.js';

export const postgresDataSources = `
INSERT OR IGNORE INTO data_sources (name, url, type, notes) VALUES
${dataSources.map(ds => `  ('${ds.name}', '${ds.url}', '${ds.type}', '${ds.notes}')`).join(',\n')};
`;

export const postgresKeywords = `
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
  postgresSchema,
  postgresDataSources,
  postgresKeywords,
};
