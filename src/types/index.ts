// Types for NH Immigrant Contracts Tracker

export interface Contractor {
  id: number;
  vendor_code: string | null;
  name: string;
  address: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  is_immigrant_related: boolean;
  notes: string | null;
  created_at: string;
  updated_at: string;
}

export interface Contract {
  id: number;
  contractor_id: number;
  contract_number: string | null;
  title: string;
  description: string | null;
  department: string | null;
  agency: string | null;
  start_date: string | null;
  end_date: string | null;
  original_amount: number | null;
  current_amount: number | null;
  amendment_count: number;
  funding_source: string | null;
  procurement_type: string | null;
  gc_approval_date: string | null;
  gc_agenda_item: string | null;
  source_url: string | null;
  source_document: string | null;
  created_at: string;
  updated_at: string;
}

export interface Expenditure {
  id: number;
  contractor_id: number | null;
  contract_id: number | null;
  fiscal_year: number;
  department: string | null;
  agency: string | null;
  activity: string | null;
  expense_class: string | null;
  vendor_name: string;
  amount: number;
  payment_date: string | null;
  description: string | null;
  source_url: string | null;
  created_at: string;
}

export interface FraudIndicator {
  id: number;
  contract_id: number | null;
  contractor_id: number | null;
  expenditure_id: number | null;
  indicator_type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  description: string;
  evidence: string | null;
  status: 'open' | 'investigating' | 'resolved' | 'dismissed';
  notes: string | null;
  created_at: string;
  updated_at: string;
}

export interface DataSource {
  id: number;
  name: string;
  url: string;
  type: 'procurement' | 'expenditure' | 'gc_agenda' | 'dhhs' | 'other';
  last_scraped: string | null;
  scrape_frequency: string | null;
  notes: string | null;
}

export interface ScrapedDocument {
  id: number;
  data_source_id: number;
  url: string;
  document_type: string | null;
  document_date: string | null;
  title: string | null;
  raw_content: string | null;
  parsed_data: string | null;
  scraped_at: string;
  processed: boolean;
}

// Fraud indicator types
export const FRAUD_INDICATOR_TYPES = {
  SOLE_SOURCE: 'sole_source',
  RAPID_AMENDMENTS: 'rapid_amendments', 
  LARGE_INCREASE: 'large_increase',
  NO_COMPETITION: 'no_competition',
  UNUSUAL_TIMING: 'unusual_timing',
  MISSING_DOCUMENTATION: 'missing_documentation',
  EXCESSIVE_COSTS: 'excessive_costs',
  DUPLICATE_PAYMENTS: 'duplicate_payments',
  SHELL_COMPANY: 'shell_company',
  CONFLICT_OF_INTEREST: 'conflict_of_interest',
} as const;

export type FraudIndicatorType = typeof FRAUD_INDICATOR_TYPES[keyof typeof FRAUD_INDICATOR_TYPES];

// Search/filter types
export interface ContractSearchParams {
  query?: string;
  contractor_id?: number;
  department?: string;
  min_amount?: number;
  max_amount?: number;
  start_date?: string;
  end_date?: string;
  has_fraud_indicators?: boolean;
  procurement_type?: string;
  limit?: number;
  offset?: number;
}

export interface ContractWithDetails extends Contract {
  contractor_name: string;
  fraud_indicator_count: number;
  total_expenditures: number;
}

export interface ContractorWithStats extends Contractor {
  total_contracts: number;
  total_contract_value: number;
  total_expenditures: number;
  fraud_indicator_count: number;
}

export interface DashboardStats {
  total_contractors: number;
  total_contracts: number;
  total_contract_value: number;
  total_expenditures: number;
  fraud_indicators_count: number;
  fraud_indicators_by_severity: {
    low: number;
    medium: number;
    high: number;
    critical: number;
  };
  recent_contracts: ContractWithDetails[];
  top_contractors: ContractorWithStats[];
}
