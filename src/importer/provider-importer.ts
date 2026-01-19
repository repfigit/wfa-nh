/**
 * Provider Importer
 * Import NH childcare provider lists from various sources
 * Supports: NH DHHS licensing data, CCDF provider registries, custom spreadsheets
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

// Column mappings for various provider data formats
const PROVIDER_COLUMN_MAPPINGS: Record<string, string> = {
  // Name variations
  'provider_name': 'name',
  'facility_name': 'name',
  'business_name': 'name',
  'daycare_name': 'name',
  'name': 'name',
  'legal_name': 'name',
  
  // DBA/Trade name
  'dba': 'dba_name',
  'dba_name': 'dba_name',
  'doing_business_as': 'dba_name',
  'trade_name': 'dba_name',
  
  // License info
  'license_number': 'license_number',
  'license_no': 'license_number',
  'license_id': 'license_number',
  'permit_number': 'license_number',
  
  'license_type': 'license_type',
  'facility_type': 'license_type',
  'provider_type': 'provider_type',
  'type': 'provider_type',
  'category': 'provider_type',
  
  'license_status': 'license_status',
  'status': 'license_status',
  
  // Address
  'address': 'address',
  'street_address': 'address',
  'street': 'address',
  'address_1': 'address',
  'address1': 'address',
  
  'city': 'city',
  'town': 'city',
  
  'state': 'state',
  'st': 'state',
  
  'zip': 'zip',
  'zip_code': 'zip',
  'zipcode': 'zip',
  'postal_code': 'zip',
  
  // Contact
  'phone': 'phone',
  'phone_number': 'phone',
  'telephone': 'phone',
  
  'email': 'email',
  'email_address': 'email',
  
  // Capacity
  'capacity': 'capacity',
  'licensed_capacity': 'capacity',
  'max_capacity': 'capacity',
  'total_capacity': 'capacity',
  
  'age_range': 'age_range',
  'ages_served': 'age_range',
  'age_group': 'age_range',
  
  // Owner info
  'owner': 'owner_name',
  'owner_name': 'owner_name',
  'director': 'owner_name',
  'contact_name': 'owner_name',
  
  // CCDF-specific
  'ccdf_provider_id': 'ccdf_provider_id',
  'ccdf_id': 'ccdf_provider_id',
  'scholarship_id': 'ccdf_provider_id',
  'provider_id': 'ccdf_provider_id',
  
  'accepts_ccdf': 'accepts_ccdf',
  'ccdf_enrolled': 'accepts_ccdf',
  'scholarship_provider': 'accepts_ccdf',
  'accepts_subsidy': 'accepts_ccdf',
  
  // Demographic flags
  'immigrant_owned': 'is_immigrant_owned',
  'is_immigrant_owned': 'is_immigrant_owned',
  
  'languages': 'language_services',
  'language_services': 'language_services',
  'languages_spoken': 'language_services',
};

interface ParsedProvider {
  name: string;
  dba_name?: string;
  license_number?: string;
  license_type?: string;
  license_status?: string;
  provider_type?: string;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  phone?: string;
  email?: string;
  capacity?: number;
  age_range?: string;
  owner_name?: string;
  ccdf_provider_id?: string;
  accepts_ccdf?: boolean;
  is_immigrant_owned?: boolean;
  language_services?: string;
}

interface ProviderImportResult {
  success: boolean;
  totalRows: number;
  newProviders: number;
  updatedProviders: number;
  skippedRows: number;
  errors: string[];
  importedProviders: { id: number; name: string; isNew: boolean }[];
}

/**
 * Parse CSV content into rows
 */
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

/**
 * Normalize headers to canonical field names
 */
function normalizeHeaders(headers: string[]): Map<string, number> {
  const normalized = new Map<string, number>();
  
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    const mappedName = PROVIDER_COLUMN_MAPPINGS[header];
    
    if (mappedName && !normalized.has(mappedName)) {
      normalized.set(mappedName, i);
    }
  }
  
  return normalized;
}

/**
 * Parse boolean values
 */
function parseBoolean(value: string | undefined): boolean | undefined {
  if (!value) return undefined;
  const lower = value.toLowerCase().trim();
  if (['yes', 'y', 'true', '1', 'x', 'enrolled', 'active'].includes(lower)) return true;
  if (['no', 'n', 'false', '0', '', 'not enrolled', 'inactive'].includes(lower)) return false;
  return undefined;
}

/**
 * Parse capacity as number
 */
function parseCapacity(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const num = parseInt(value.replace(/[^0-9]/g, ''));
  return isNaN(num) ? undefined : num;
}

/**
 * Normalize phone number
 */
function normalizePhone(phone: string | undefined): string | undefined {
  if (!phone) return undefined;
  const digits = phone.replace(/[^0-9]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7)}`;
  }
  return phone; // Return original if can't normalize
}

/**
 * Normalize state to 2-letter code
 */
function normalizeState(state: string | undefined): string {
  if (!state) return 'NH';
  const upper = state.toUpperCase().trim();
  if (upper === 'NEW HAMPSHIRE') return 'NH';
  if (upper.length === 2) return upper;
  return 'NH'; // Default to NH
}

/**
 * Generate a match key for deduplication
 */
function generateMatchKey(provider: ParsedProvider): string {
  const namePart = (provider.name || '').toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .slice(0, 20);
  const cityPart = (provider.city || '').toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .slice(0, 10);
  return `${namePart}_${cityPart}`;
}

/**
 * Import providers from CSV
 */
export async function importProviders(
  csvContent: string,
  options: {
    updateExisting?: boolean;  // Update existing providers if found
    matchBy?: 'name' | 'license' | 'ccdf_id';  // How to match existing
  } = {}
): Promise<ProviderImportResult> {
  const result: ProviderImportResult = {
    success: false,
    totalRows: 0,
    newProviders: 0,
    updatedProviders: 0,
    skippedRows: 0,
    errors: [],
    importedProviders: [],
  };
  
  try {
    const { headers, rows } = parseCSV(csvContent);
    result.totalRows = rows.length;
    
    if (headers.length === 0) {
      result.errors.push('No headers found in CSV');
      return result;
    }
    
    const columnMap = normalizeHeaders(headers);
    
    // Check for name column (required)
    if (!columnMap.has('name')) {
      result.errors.push(`Required column "name" not found. Available columns: ${headers.join(', ')}`);
      return result;
    }
    
    await initializeDb();
    
    // Load existing providers for matching
    const existingProviders = await query(`
      SELECT id, name, license_number, ccdf_provider_id, city 
      FROM providers
    `);
    
    // Build lookup maps
    const byName = new Map<string, any>();
    const byLicense = new Map<string, any>();
    const byCcdfId = new Map<string, any>();
    const byMatchKey = new Map<string, any>();
    
    for (const p of existingProviders) {
      if (p.name) {
        const key = (p.name as string).toLowerCase().trim();
        byName.set(key, p);
      }
      if (p.license_number) {
        byLicense.set((p.license_number as string).toLowerCase().trim(), p);
      }
      if (p.ccdf_provider_id) {
        byCcdfId.set((p.ccdf_provider_id as string).toLowerCase().trim(), p);
      }
      // Match key
      const matchKey = generateMatchKey({ 
        name: p.name as string, 
        city: p.city as string 
      });
      byMatchKey.set(matchKey, p);
    }
    
    // Process rows
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      
      try {
        const parsed: ParsedProvider = {
          name: columnMap.has('name') ? row[columnMap.get('name')!]?.trim() : '',
          dba_name: columnMap.has('dba_name') ? row[columnMap.get('dba_name')!]?.trim() : undefined,
          license_number: columnMap.has('license_number') ? row[columnMap.get('license_number')!]?.trim() : undefined,
          license_type: columnMap.has('license_type') ? row[columnMap.get('license_type')!]?.trim() : undefined,
          license_status: columnMap.has('license_status') ? row[columnMap.get('license_status')!]?.trim() : undefined,
          provider_type: columnMap.has('provider_type') ? row[columnMap.get('provider_type')!]?.trim() : undefined,
          address: columnMap.has('address') ? row[columnMap.get('address')!]?.trim() : undefined,
          city: columnMap.has('city') ? row[columnMap.get('city')!]?.trim() : undefined,
          state: normalizeState(columnMap.has('state') ? row[columnMap.get('state')!] : undefined),
          zip: columnMap.has('zip') ? row[columnMap.get('zip')!]?.trim() : undefined,
          phone: normalizePhone(columnMap.has('phone') ? row[columnMap.get('phone')!] : undefined),
          email: columnMap.has('email') ? row[columnMap.get('email')!]?.trim().toLowerCase() : undefined,
          capacity: parseCapacity(columnMap.has('capacity') ? row[columnMap.get('capacity')!] : undefined),
          age_range: columnMap.has('age_range') ? row[columnMap.get('age_range')!]?.trim() : undefined,
          owner_name: columnMap.has('owner_name') ? row[columnMap.get('owner_name')!]?.trim() : undefined,
          ccdf_provider_id: columnMap.has('ccdf_provider_id') ? row[columnMap.get('ccdf_provider_id')!]?.trim() : undefined,
          accepts_ccdf: parseBoolean(columnMap.has('accepts_ccdf') ? row[columnMap.get('accepts_ccdf')!] : undefined),
          is_immigrant_owned: parseBoolean(columnMap.has('is_immigrant_owned') ? row[columnMap.get('is_immigrant_owned')!] : undefined),
          language_services: columnMap.has('language_services') ? row[columnMap.get('language_services')!]?.trim() : undefined,
        };
        
        // Skip empty names
        if (!parsed.name) {
          result.skippedRows++;
          continue;
        }
        
        // Check for existing provider
        let existingProvider: any = null;
        const matchBy = options.matchBy || 'name';
        
        if (matchBy === 'license' && parsed.license_number) {
          existingProvider = byLicense.get(parsed.license_number.toLowerCase());
        } else if (matchBy === 'ccdf_id' && parsed.ccdf_provider_id) {
          existingProvider = byCcdfId.get(parsed.ccdf_provider_id.toLowerCase());
        } else {
          // Try match key first (more precise)
          const matchKey = generateMatchKey(parsed);
          existingProvider = byMatchKey.get(matchKey);
          
          // Fall back to name match
          if (!existingProvider) {
            existingProvider = byName.get(parsed.name.toLowerCase());
          }
        }
        
        if (existingProvider && options.updateExisting) {
          // Update existing provider
          await execute(`
            UPDATE providers SET
              dba_name = COALESCE(?, dba_name),
              license_number = COALESCE(?, license_number),
              license_type = COALESCE(?, license_type),
              license_status = COALESCE(?, license_status),
              provider_type = COALESCE(?, provider_type),
              address = COALESCE(?, address),
              city = COALESCE(?, city),
              state = COALESCE(?, state),
              zip = COALESCE(?, zip),
              phone = COALESCE(?, phone),
              email = COALESCE(?, email),
              capacity = COALESCE(?, capacity),
              age_range = COALESCE(?, age_range),
              owner_name = COALESCE(?, owner_name),
              ccdf_provider_id = COALESCE(?, ccdf_provider_id),
              accepts_ccdf = COALESCE(?, accepts_ccdf),
              is_immigrant_owned = COALESCE(?, is_immigrant_owned),
              language_services = COALESCE(?, language_services),
              updated_at = datetime('now')
            WHERE id = ?
          `, [
            parsed.dba_name || null,
            parsed.license_number || null,
            parsed.license_type || null,
            parsed.license_status || null,
            parsed.provider_type || null,
            parsed.address || null,
            parsed.city || null,
            parsed.state || null,
            parsed.zip || null,
            parsed.phone || null,
            parsed.email || null,
            parsed.capacity || null,
            parsed.age_range || null,
            parsed.owner_name || null,
            parsed.ccdf_provider_id || null,
            parsed.accepts_ccdf !== undefined ? (parsed.accepts_ccdf ? 1 : 0) : null,
            parsed.is_immigrant_owned !== undefined ? (parsed.is_immigrant_owned ? 1 : 0) : null,
            parsed.language_services || null,
            existingProvider.id,
          ]);
          
          result.updatedProviders++;
          result.importedProviders.push({
            id: existingProvider.id,
            name: parsed.name,
            isNew: false,
          });
          
        } else if (existingProvider) {
          // Skip duplicate
          result.skippedRows++;
          
        } else {
          // Insert new provider
          const insertResult = await execute(`
            INSERT INTO providers (
              name, dba_name, license_number, license_type, license_status,
              provider_type, address, city, state, zip, phone, email,
              capacity, age_range, owner_name, ccdf_provider_id,
              accepts_ccdf, is_immigrant_owned, language_services,
              notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Imported from CSV', datetime('now'), datetime('now'))
          `, [
            parsed.name,
            parsed.dba_name || null,
            parsed.license_number || null,
            parsed.license_type || null,
            parsed.license_status || null,
            parsed.provider_type || null,
            parsed.address || null,
            parsed.city || null,
            parsed.state || 'NH',
            parsed.zip || null,
            parsed.phone || null,
            parsed.email || null,
            parsed.capacity || null,
            parsed.age_range || null,
            parsed.owner_name || null,
            parsed.ccdf_provider_id || null,
            parsed.accepts_ccdf !== undefined ? (parsed.accepts_ccdf ? 1 : 0) : 0,
            parsed.is_immigrant_owned !== undefined ? (parsed.is_immigrant_owned ? 1 : 0) : 0,
            parsed.language_services || null,
          ]);
          
          const newId = insertResult.lastId || 0;
          result.newProviders++;
          result.importedProviders.push({
            id: newId,
            name: parsed.name,
            isNew: true,
          });
          
          // Add to lookup maps for subsequent rows
          byName.set(parsed.name.toLowerCase(), { id: newId, ...parsed });
          if (parsed.license_number) {
            byLicense.set(parsed.license_number.toLowerCase(), { id: newId, ...parsed });
          }
          if (parsed.ccdf_provider_id) {
            byCcdfId.set(parsed.ccdf_provider_id.toLowerCase(), { id: newId, ...parsed });
          }
          byMatchKey.set(generateMatchKey(parsed), { id: newId, ...parsed });
        }
        
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
 * Get a CSV template for provider imports
 */
export function getProviderTemplate(): string {
  const headers = [
    'name',
    'dba_name', 
    'license_number',
    'license_type',
    'license_status',
    'provider_type',
    'address',
    'city',
    'state',
    'zip',
    'phone',
    'email',
    'capacity',
    'age_range',
    'owner_name',
    'ccdf_provider_id',
    'accepts_ccdf',
    'is_immigrant_owned',
    'language_services',
  ];
  
  const exampleRow = [
    'ABC Childcare Center',
    'ABC Kids',
    'NH-12345',
    'Center',
    'Active',
    'Child Care Center',
    '123 Main St',
    'Manchester',
    'NH',
    '03101',
    '(603) 555-1234',
    'info@abcchildcare.com',
    '50',
    '6 weeks - 12 years',
    'Jane Smith',
    'CCDF-001',
    'Yes',
    'No',
    'English, Spanish',
  ];
  
  return [headers.join(','), exampleRow.join(',')].join('\n');
}

export default {
  importProviders,
  getProviderTemplate,
};
