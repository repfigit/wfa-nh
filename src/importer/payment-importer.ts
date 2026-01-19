/**
 * Payment Importer
 * Import CCDF scholarship payments and other payment data
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Column mappings for payment data
const PAYMENT_COLUMN_MAPPINGS: Record<string, string> = {
  // Provider identification
  'provider_name': 'provider_name',
  'facility_name': 'provider_name',
  'daycare_name': 'provider_name',
  'vendor_name': 'provider_name',
  'vendor': 'provider_name',
  'payee': 'provider_name',
  'name': 'provider_name',
  
  'provider_id': 'provider_id',
  'ccdf_provider_id': 'ccdf_provider_id',
  'ccdf_id': 'ccdf_provider_id',
  
  // Amount
  'amount': 'amount',
  'payment_amount': 'amount',
  'total_amount': 'amount',
  'payment': 'amount',
  'reimbursement': 'amount',
  
  // Date
  'payment_date': 'payment_date',
  'date': 'payment_date',
  'service_date': 'payment_date',
  'check_date': 'payment_date',
  
  // Fiscal period
  'fiscal_year': 'fiscal_year',
  'fy': 'fiscal_year',
  'year': 'fiscal_year',
  
  'fiscal_month': 'fiscal_month',
  'month': 'fiscal_month',
  'period': 'fiscal_month',
  
  // Service details
  'children_served': 'children_served',
  'child_count': 'children_served',
  'number_of_children': 'children_served',
  'children': 'children_served',
  
  'attendance_hours': 'attendance_hours',
  'hours': 'attendance_hours',
  'total_hours': 'attendance_hours',
  
  // Payment info
  'payment_type': 'payment_type',
  'type': 'payment_type',
  'category': 'payment_type',
  
  'funding_source': 'funding_source',
  'source': 'funding_source',
  'fund': 'funding_source',
  
  'check_number': 'check_number',
  'check_no': 'check_number',
  'reference': 'check_number',
  
  'description': 'description',
  'memo': 'description',
  'notes': 'description',
};

interface ParsedPayment {
  provider_name?: string;
  provider_id?: number;
  ccdf_provider_id?: string;
  amount: number;
  payment_date?: string;
  fiscal_year?: number;
  fiscal_month?: number;
  children_served?: number;
  attendance_hours?: number;
  payment_type?: string;
  funding_source?: string;
  check_number?: string;
  description?: string;
}

interface PaymentImportResult {
  success: boolean;
  totalRows: number;
  importedPayments: number;
  skippedRows: number;
  unmatchedProviders: string[];
  errors: string[];
  totalAmount: number;
}

function parseCSV(csvContent: string): { headers: string[]; rows: string[][] } {
  const lines = csvContent.split(/\r?\n/).filter(line => line.trim());
  if (lines.length === 0) {
    return { headers: [], rows: [] };
  }
  
  const headers = parseCSVLine(lines[0]).map(h => 
    h.toLowerCase().trim().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '')
  );
  
  const rows: string[][] = [];
  for (let i = 1; i < lines.length; i++) {
    const row = parseCSVLine(lines[i]);
    if (row.length > 0 && row.some(cell => cell.trim())) {
      rows.push(row);
    }
  }
  
  return { headers, rows };
}

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

function normalizeHeaders(headers: string[]): Map<string, number> {
  const normalized = new Map<string, number>();
  
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    const mappedName = PAYMENT_COLUMN_MAPPINGS[header];
    
    if (mappedName && !normalized.has(mappedName)) {
      normalized.set(mappedName, i);
    }
  }
  
  return normalized;
}

function parseAmount(amountStr: string): number {
  if (!amountStr) return 0;
  const cleaned = amountStr.replace(/[$,()]/g, '').trim();
  const amount = parseFloat(cleaned);
  if (amountStr.includes('(') && amountStr.includes(')')) {
    return -Math.abs(amount);
  }
  return isNaN(amount) ? 0 : amount;
}

function parseDate(dateStr: string): string | undefined {
  if (!dateStr) return undefined;
  
  const formats = [
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,
    /^(\d{4})-(\d{2})-(\d{2})$/,
    /^(\d{1,2})-(\d{1,2})-(\d{4})$/,
  ];
  
  for (const format of formats) {
    const match = dateStr.match(format);
    if (match) {
      if (format === formats[0] || format === formats[2]) {
        const [, month, day, year] = match;
        return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
      } else {
        return dateStr;
      }
    }
  }
  
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) {
    return date.toISOString().split('T')[0];
  }
  
  return undefined;
}

function parseFiscalYear(value: string | undefined, paymentDate?: string): number | undefined {
  if (value) {
    const match = value.match(/\d{4}/);
    if (match) return parseInt(match[0]);
    
    const match2 = value.match(/\d{2}/);
    if (match2) {
      const year = parseInt(match2[0]);
      return year > 50 ? 1900 + year : 2000 + year;
    }
  }
  
  if (paymentDate) {
    const date = new Date(paymentDate);
    if (!isNaN(date.getTime())) {
      const month = date.getMonth() + 1;
      const year = date.getFullYear();
      return month >= 7 ? year + 1 : year;
    }
  }
  
  return undefined;
}

function parseFiscalMonth(value: string | undefined, paymentDate?: string): number | undefined {
  if (value) {
    const num = parseInt(value);
    if (!isNaN(num) && num >= 1 && num <= 12) return num;
    
    // Try month name
    const months: Record<string, number> = {
      'jan': 1, 'january': 1,
      'feb': 2, 'february': 2,
      'mar': 3, 'march': 3,
      'apr': 4, 'april': 4,
      'may': 5,
      'jun': 6, 'june': 6,
      'jul': 7, 'july': 7,
      'aug': 8, 'august': 8,
      'sep': 9, 'sept': 9, 'september': 9,
      'oct': 10, 'october': 10,
      'nov': 11, 'november': 11,
      'dec': 12, 'december': 12,
    };
    const lower = value.toLowerCase().trim();
    if (months[lower]) return months[lower];
  }
  
  if (paymentDate) {
    const date = new Date(paymentDate);
    if (!isNaN(date.getTime())) {
      return date.getMonth() + 1;
    }
  }
  
  return undefined;
}

function parseInteger(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const num = parseInt(value.replace(/[^0-9-]/g, ''));
  return isNaN(num) ? undefined : num;
}

function parseFloat2(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const num = parseFloat(value.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? undefined : num;
}

/**
 * Import payments from CSV
 */
export async function importPayments(
  csvContent: string,
  options: {
    createMissingProviders?: boolean;
    defaultPaymentType?: string;
    defaultFundingSource?: string;
  } = {}
): Promise<PaymentImportResult> {
  const result: PaymentImportResult = {
    success: false,
    totalRows: 0,
    importedPayments: 0,
    skippedRows: 0,
    unmatchedProviders: [],
    errors: [],
    totalAmount: 0,
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
    if (!columnMap.has('amount')) {
      result.errors.push(`Required column "amount" not found. Available: ${headers.join(', ')}`);
      return result;
    }
    
    // Need some way to identify provider
    const hasProviderIdentifier = columnMap.has('provider_name') || 
                                   columnMap.has('provider_id') || 
                                   columnMap.has('ccdf_provider_id');
    if (!hasProviderIdentifier) {
      result.errors.push(`Need provider identifier. Include one of: provider_name, provider_id, ccdf_provider_id`);
      return result;
    }
    
    await initializeDb();
    
    // Load existing providers
    const providers = await query(`
      SELECT id, name, ccdf_provider_id FROM providers
    `);
    
    const byName = new Map<string, number>();
    const byCcdfId = new Map<string, number>();
    
    for (const p of providers) {
      if (p.name) byName.set((p.name as string).toLowerCase().trim(), p.id as number);
      if (p.ccdf_provider_id) byCcdfId.set((p.ccdf_provider_id as string).toLowerCase().trim(), p.id as number);
    }
    
    const unmatchedSet = new Set<string>();
    
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      
      try {
        const parsed: ParsedPayment = {
          provider_name: columnMap.has('provider_name') ? row[columnMap.get('provider_name')!]?.trim() : undefined,
          provider_id: columnMap.has('provider_id') ? parseInteger(row[columnMap.get('provider_id')!]) : undefined,
          ccdf_provider_id: columnMap.has('ccdf_provider_id') ? row[columnMap.get('ccdf_provider_id')!]?.trim() : undefined,
          amount: parseAmount(row[columnMap.get('amount')!] || '0'),
          payment_date: columnMap.has('payment_date') ? parseDate(row[columnMap.get('payment_date')!]) : undefined,
          fiscal_year: parseFiscalYear(
            columnMap.has('fiscal_year') ? row[columnMap.get('fiscal_year')!] : undefined,
            columnMap.has('payment_date') ? row[columnMap.get('payment_date')!] : undefined
          ),
          fiscal_month: parseFiscalMonth(
            columnMap.has('fiscal_month') ? row[columnMap.get('fiscal_month')!] : undefined,
            columnMap.has('payment_date') ? row[columnMap.get('payment_date')!] : undefined
          ),
          children_served: columnMap.has('children_served') ? parseInteger(row[columnMap.get('children_served')!]) : undefined,
          attendance_hours: columnMap.has('attendance_hours') ? parseFloat2(row[columnMap.get('attendance_hours')!]) : undefined,
          payment_type: columnMap.has('payment_type') ? row[columnMap.get('payment_type')!]?.trim() : options.defaultPaymentType,
          funding_source: columnMap.has('funding_source') ? row[columnMap.get('funding_source')!]?.trim() : options.defaultFundingSource,
          check_number: columnMap.has('check_number') ? row[columnMap.get('check_number')!]?.trim() : undefined,
          description: columnMap.has('description') ? row[columnMap.get('description')!]?.trim() : undefined,
        };
        
        // Skip zero amounts
        if (parsed.amount === 0) {
          result.skippedRows++;
          continue;
        }
        
        // Find provider
        let providerId: number | null = null;
        
        if (parsed.provider_id) {
          providerId = parsed.provider_id;
        } else if (parsed.ccdf_provider_id) {
          providerId = byCcdfId.get(parsed.ccdf_provider_id.toLowerCase()) || null;
        } else if (parsed.provider_name) {
          providerId = byName.get(parsed.provider_name.toLowerCase()) || null;
          
          // Try fuzzy match
          if (!providerId) {
            const searchKey = parsed.provider_name.toLowerCase();
            for (const [name, id] of byName) {
              if (name.includes(searchKey) || searchKey.includes(name)) {
                providerId = id;
                break;
              }
            }
          }
        }
        
        // Handle unmatched provider
        if (!providerId) {
          if (options.createMissingProviders && parsed.provider_name) {
            // Create new provider
            const insertResult = await execute(`
              INSERT INTO providers (name, accepts_ccdf, notes, created_at)
              VALUES (?, 1, 'Auto-created from payment import', datetime('now'))
            `, [parsed.provider_name]);
            
            providerId = insertResult.lastId || null;
            
            if (providerId) {
              byName.set(parsed.provider_name.toLowerCase(), providerId);
            }
          } else {
            unmatchedSet.add(parsed.provider_name || parsed.ccdf_provider_id || 'Unknown');
            result.skippedRows++;
            continue;
          }
        }
        
        // Insert payment
        await execute(`
          INSERT INTO payments (
            provider_id, amount, payment_date, fiscal_year, fiscal_month,
            children_served, attendance_hours, payment_type, funding_source,
            check_number, description, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        `, [
          providerId,
          parsed.amount,
          parsed.payment_date || null,
          parsed.fiscal_year || null,
          parsed.fiscal_month || null,
          parsed.children_served || null,
          parsed.attendance_hours || null,
          parsed.payment_type || 'CCDF Scholarship',
          parsed.funding_source || 'Federal CCDF',
          parsed.check_number || null,
          parsed.description || null,
        ]);
        
        result.importedPayments++;
        result.totalAmount += parsed.amount;
        
      } catch (rowError) {
        result.errors.push(`Row ${i + 2}: ${rowError}`);
        result.skippedRows++;
      }
    }
    
    await saveDb();
    result.unmatchedProviders = Array.from(unmatchedSet);
    result.success = true;
    
  } catch (error) {
    result.errors.push(`Import failed: ${error}`);
  }
  
  return result;
}

/**
 * Get a CSV template for payment imports
 */
export function getPaymentTemplate(): string {
  const headers = [
    'provider_name',
    'amount',
    'payment_date',
    'fiscal_year',
    'fiscal_month',
    'children_served',
    'payment_type',
    'funding_source',
    'check_number',
    'description',
  ];
  
  const exampleRow = [
    'ABC Childcare Center',
    '5000.00',
    '01/15/2025',
    '2025',
    '1',
    '25',
    'CCDF Scholarship',
    'Federal CCDF',
    'CHK-12345',
    'January 2025 scholarship payment',
  ];
  
  return [headers.join(','), exampleRow.join(',')].join('\n');
}

export default {
  importPayments,
  getPaymentTemplate,
};
