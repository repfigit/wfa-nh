/**
 * CSV Importer for TransparentNH Data
 * Imports and normalizes CSV exports from TransparentNH expenditure register
 * Inspired by wfa-nh-gemini scraper.py
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Column name mappings - TransparentNH uses various column names
const COLUMN_MAPPINGS: Record<string, string> = {
  // Vendor variations
  'vendor_name': 'vendor',
  'payee_name': 'vendor',
  'payee': 'vendor',
  'vendor_payee': 'vendor',
  'vendor': 'vendor',
  
  // Amount variations
  'expenditure_amount': 'amount',
  'payment_amount': 'amount',
  'contract_amount': 'amount',
  'amount': 'amount',
  
  // Date variations
  'payment_date': 'payment_date',
  'date': 'payment_date',
  'transaction_date': 'payment_date',
  'contract_date': 'payment_date',
  
  // Description variations
  'account_desc': 'description',
  'description': 'description',
  'purpose': 'description',
  'expense_description': 'description',
  
  // Agency variations
  'agency_name': 'agency',
  'department': 'agency',
  'agency': 'agency',
  'dept': 'agency',
  
  // Other common fields
  'fiscal_year': 'fiscal_year',
  'fy': 'fiscal_year',
  'activity': 'activity',
  'expense_class': 'expense_class',
  'class': 'expense_class',
};

// Keywords to identify childcare-related expenditures
const CHILDCARE_KEYWORDS = [
  'daycare', 'day care', 'child care', 'childcare', 'early learning',
  'preschool', 'pre-school', 'nursery', 'head start', 'headstart',
  'after school', 'afterschool', 'youth center', 'kindergarten',
  'ccdf', 'child development', 'early childhood',
];

const IMMIGRANT_KEYWORDS = [
  'immigrant', 'refugee', 'asylum', 'resettlement', 'translation',
  'interpreter', 'multicultural', 'esl', 'newcomer', 'foreign',
];

interface ParsedRow {
  vendor: string;
  amount: number;
  payment_date?: string;
  description?: string;
  agency?: string;
  fiscal_year?: number;
  activity?: string;
  expense_class?: string;
}

interface ImportResult {
  success: boolean;
  totalRows: number;
  importedRows: number;
  skippedRows: number;
  matchedProviders: number;
  newExpenditures: number;
  errors: string[];
}

/**
 * Parse a CSV string into rows
 */
function parseCSV(csvContent: string): { headers: string[]; rows: string[][] } {
  const lines = csvContent.split(/\r?\n/).filter(line => line.trim());
  if (lines.length === 0) {
    return { headers: [], rows: [] };
  }
  
  // Parse header row
  const headers = parseCSVLine(lines[0]).map(h => 
    h.toLowerCase().trim().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '')
  );
  
  // Parse data rows
  const rows: string[][] = [];
  for (let i = 1; i < lines.length; i++) {
    const row = parseCSVLine(lines[i]);
    if (row.length > 0) {
      rows.push(row);
    }
  }
  
  return { headers, rows };
}

/**
 * Parse a single CSV line, handling quoted values
 */
function parseCSVLine(line: string): string[] {
  const values: string[] = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      values.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  values.push(current.trim());
  return values;
}

/**
 * Normalize column headers using mappings
 */
function normalizeHeaders(headers: string[]): Map<string, number> {
  const normalized = new Map<string, number>();
  
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    const mappedName = COLUMN_MAPPINGS[header];
    
    if (mappedName && !normalized.has(mappedName)) {
      normalized.set(mappedName, i);
    }
  }
  
  return normalized;
}

/**
 * Parse amount string to number
 */
function parseAmount(amountStr: string): number {
  if (!amountStr) return 0;
  
  // Remove currency symbols, commas, parentheses (for negative)
  const cleaned = amountStr.replace(/[$,()]/g, '').trim();
  const amount = parseFloat(cleaned);
  
  // Handle parentheses as negative
  if (amountStr.includes('(') && amountStr.includes(')')) {
    return -Math.abs(amount);
  }
  
  return isNaN(amount) ? 0 : amount;
}

/**
 * Parse date string to ISO format
 */
function parseDate(dateStr: string): string | undefined {
  if (!dateStr) return undefined;
  
  // Try various date formats
  const formats = [
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/, // MM/DD/YYYY
    /^(\d{4})-(\d{2})-(\d{2})$/,       // YYYY-MM-DD
    /^(\d{1,2})-(\d{1,2})-(\d{4})$/,   // MM-DD-YYYY
  ];
  
  for (const format of formats) {
    const match = dateStr.match(format);
    if (match) {
      if (format === formats[0] || format === formats[2]) {
        // MM/DD/YYYY or MM-DD-YYYY
        const [, month, day, year] = match;
        return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
      } else {
        // YYYY-MM-DD
        return dateStr;
      }
    }
  }
  
  // Try native Date parsing
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) {
    return date.toISOString().split('T')[0];
  }
  
  return undefined;
}

/**
 * Parse fiscal year from string or date
 */
function parseFiscalYear(value: string | undefined, paymentDate?: string): number | undefined {
  if (value) {
    // Extract 4-digit year
    const match = value.match(/\d{4}/);
    if (match) {
      return parseInt(match[0]);
    }
    // Try 2-digit year (FY24 -> 2024)
    const match2 = value.match(/\d{2}/);
    if (match2) {
      const year = parseInt(match2[0]);
      return year > 50 ? 1900 + year : 2000 + year;
    }
  }
  
  // Derive from payment date
  if (paymentDate) {
    const date = new Date(paymentDate);
    if (!isNaN(date.getTime())) {
      // NH fiscal year: July 1 - June 30
      // If month >= 7, it's the next FY
      const month = date.getMonth() + 1;
      const year = date.getFullYear();
      return month >= 7 ? year + 1 : year;
    }
  }
  
  return undefined;
}

/**
 * Check if text contains childcare-related keywords
 */
function isChildcareRelated(text: string): boolean {
  const lower = text.toLowerCase();
  return CHILDCARE_KEYWORDS.some(kw => lower.includes(kw));
}

/**
 * Check if text contains immigrant-related keywords
 */
function isImmigrantRelated(text: string): boolean {
  const lower = text.toLowerCase();
  return IMMIGRANT_KEYWORDS.some(kw => lower.includes(kw));
}

/**
 * Import CSV data into the database
 */
export async function importCSV(
  csvContent: string,
  options: {
    filterChildcare?: boolean;
    createProviders?: boolean;
  } = {}
): Promise<ImportResult> {
  const result: ImportResult = {
    success: false,
    totalRows: 0,
    importedRows: 0,
    skippedRows: 0,
    matchedProviders: 0,
    newExpenditures: 0,
    errors: [],
  };
  
  try {
    const { headers, rows } = parseCSV(csvContent);
    result.totalRows = rows.length;
    
    if (headers.length === 0) {
      result.errors.push('No headers found in CSV');
      return result;
    }
    
    const columnMap = normalizeHeaders(headers);
    
    // Check required columns
    if (!columnMap.has('vendor')) {
      result.errors.push('Required column "vendor" not found. Available: ' + headers.join(', '));
      return result;
    }
    if (!columnMap.has('amount')) {
      result.errors.push('Required column "amount" not found. Available: ' + headers.join(', '));
      return result;
    }
    
    await initializeDb();
    
    // Get existing providers for matching
    const providers = await query('SELECT id, name FROM providers');
    const providerMap = new Map<string, number>();
    for (const row of providers) {
      const name = (row.name as string).toLowerCase();
      providerMap.set(name, row.id as number);
    }
    
    // Process each row
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      
      try {
        const parsed: ParsedRow = {
          vendor: row[columnMap.get('vendor')!] || '',
          amount: parseAmount(row[columnMap.get('amount')!] || '0'),
          payment_date: columnMap.has('payment_date') 
            ? parseDate(row[columnMap.get('payment_date')!])
            : undefined,
          description: columnMap.has('description')
            ? row[columnMap.get('description')!]
            : undefined,
          agency: columnMap.has('agency')
            ? row[columnMap.get('agency')!]
            : undefined,
          fiscal_year: parseFiscalYear(
            columnMap.has('fiscal_year') ? row[columnMap.get('fiscal_year')!] : undefined,
            columnMap.has('payment_date') ? row[columnMap.get('payment_date')!] : undefined
          ),
          activity: columnMap.has('activity')
            ? row[columnMap.get('activity')!]
            : undefined,
          expense_class: columnMap.has('expense_class')
            ? row[columnMap.get('expense_class')!]
            : undefined,
        };
        
        // Skip empty vendors
        if (!parsed.vendor.trim()) {
          result.skippedRows++;
          continue;
        }
        
        // Filter for childcare if requested
        if (options.filterChildcare) {
          const searchText = `${parsed.vendor} ${parsed.description || ''} ${parsed.activity || ''}`;
          if (!isChildcareRelated(searchText)) {
            result.skippedRows++;
            continue;
          }
        }
        
        // Try to match to existing provider
        let providerId: number | null = null;
        const vendorLower = parsed.vendor.toLowerCase();
        
        for (const [providerName, id] of providerMap) {
          if (vendorLower.includes(providerName) || providerName.includes(vendorLower)) {
            providerId = id;
            result.matchedProviders++;
            break;
          }
        }
        
        // Create provider if option enabled and childcare-related
        if (!providerId && options.createProviders) {
          const searchText = `${parsed.vendor} ${parsed.description || ''}`;
          if (isChildcareRelated(searchText)) {
            const insertResult = await execute(`
              INSERT INTO providers (name, accepts_ccdf, is_immigrant_owned, notes)
              VALUES (?, 1, ?, 'Auto-created from CSV import')
            `, [
              parsed.vendor,
              isImmigrantRelated(searchText) ? 1 : 0,
            ]);
            
            providerId = insertResult.lastId || null;
            
            if (providerId) {
              providerMap.set(vendorLower, providerId);
            }
          }
        }
        
        // Insert expenditure
        await execute(`
          INSERT INTO expenditures (
            provider_id, fiscal_year, department, agency, activity,
            expense_class, vendor_name, amount, payment_date, description,
            source_url
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'TransparentNH CSV Import')
        `, [
          providerId,
          parsed.fiscal_year || null,
          parsed.agency || null,
          parsed.agency || null,
          parsed.activity || null,
          parsed.expense_class || null,
          parsed.vendor,
          parsed.amount,
          parsed.payment_date || null,
          parsed.description || null,
        ]);
        
        result.newExpenditures++;
        result.importedRows++;
        
      } catch (rowError) {
        result.errors.push(`Row ${i + 2}: ${rowError}`);
        result.skippedRows++;
      }
    }
    
    await saveDb();
    result.success = true;
    
  } catch (error) {
    result.errors.push(`Import failed: ${error}`);
  }
  
  return result;
}

/**
 * Get import statistics
 */
export async function getImportStats(): Promise<{
  totalExpenditures: number;
  totalAmount: number;
  byAgency: { agency: string; count: number; amount: number }[];
  byFiscalYear: { year: number; count: number; amount: number }[];
}> {
  await initializeDb();
  
  const total = await query('SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM expenditures');
  
  const byAgency = await query(`
    SELECT agency, COUNT(*) as count, COALESCE(SUM(amount), 0) as amount
    FROM expenditures
    WHERE agency IS NOT NULL
    GROUP BY agency
    ORDER BY amount DESC
    LIMIT 20
  `);
  
  const byYear = await query(`
    SELECT fiscal_year, COUNT(*) as count, COALESCE(SUM(amount), 0) as amount
    FROM expenditures
    WHERE fiscal_year IS NOT NULL
    GROUP BY fiscal_year
    ORDER BY fiscal_year DESC
  `);
  
  return {
    totalExpenditures: parseInt(String(total[0]?.count)) || 0,
    totalAmount: parseFloat(String(total[0]?.total)) || 0,
    byAgency: byAgency.map(row => ({
      agency: row.agency as string,
      count: parseInt(String(row.count)) || 0,
      amount: parseFloat(String(row.amount)) || 0,
    })),
    byFiscalYear: byYear.map(row => ({
      year: parseInt(String(row.fiscal_year)) || 0,
      count: parseInt(String(row.count)) || 0,
      amount: parseFloat(String(row.amount)) || 0,
    })),
  };
}

export default {
  importCSV,
  getImportStats,
  parseCSV,
  CHILDCARE_KEYWORDS,
  IMMIGRANT_KEYWORDS,
};
