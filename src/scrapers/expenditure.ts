/**
 * Scraper for NH TransparentNH Expenditure Data
 * 
 * Source: https://business.nh.gov/ExpenditureTransparency/
 * 
 * Note: The actual expenditure register may require form submissions
 * or specific search parameters. This provides utilities for parsing
 * expenditure data once obtained.
 */

import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { initializeDb, getDb, dbHelpers, saveDb } from '../db/database.js';

// Known immigrant-related vendors to search for
const TARGET_VENDORS = [
  'Ascentria',
  'International Institute',
  'IINE',
  'Lutheran Immigration',
  'Catholic Charities',
  'Refugee',
];

// Departments likely to have immigrant-related expenditures
const TARGET_DEPARTMENTS = [
  'Health and Human Services',
  'DHHS',
  'Education',
  'Employment Security',
];

interface ExpenditureRecord {
  fiscalYear: number;
  department: string;
  agency: string;
  activity: string;
  expenseClass: string;
  vendorName: string;
  amount: number;
  paymentDate: string | null;
  description: string | null;
}

/**
 * Parse expenditure data from HTML table
 */
function parseExpenditureTable(html: string): ExpenditureRecord[] {
  const $ = cheerio.load(html);
  const records: ExpenditureRecord[] = [];
  
  $('table tr').each((i, row) => {
    if (i === 0) return; // Skip header
    
    const cells = $(row).find('td');
    if (cells.length < 6) return;
    
    const record: ExpenditureRecord = {
      fiscalYear: parseInt($(cells[0]).text().trim()) || new Date().getFullYear(),
      department: $(cells[1]).text().trim(),
      agency: $(cells[2]).text().trim(),
      activity: $(cells[3]).text().trim(),
      expenseClass: $(cells[4]).text().trim(),
      vendorName: $(cells[5]).text().trim(),
      amount: parseFloat($(cells[6]).text().replace(/[$,]/g, '')) || 0,
      paymentDate: $(cells[7]).text().trim() || null,
      description: $(cells[8]).text().trim() || null,
    };
    
    records.push(record);
  });
  
  return records;
}

/**
 * Filter expenditures for immigrant-related vendors
 */
export function filterImmigrantRelated(records: ExpenditureRecord[]): ExpenditureRecord[] {
  return records.filter(record => {
    const vendorLower = record.vendorName.toLowerCase();
    const deptLower = record.department.toLowerCase();
    const activityLower = record.activity.toLowerCase();
    
    // Check vendor name
    for (const target of TARGET_VENDORS) {
      if (vendorLower.includes(target.toLowerCase())) {
        return true;
      }
    }
    
    // Check activity for refugee/immigrant keywords
    const keywords = ['refugee', 'immigrant', 'resettlement', 'translation', 'interpreter'];
    for (const kw of keywords) {
      if (activityLower.includes(kw)) {
        return true;
      }
    }
    
    return false;
  });
}

/**
 * Search TransparentNH for specific vendor expenditures
 * Note: This is a simplified version. The actual implementation would need
 * to handle the form submission and session management of the NH portal.
 */
export async function searchExpenditures(vendorName: string, fiscalYear?: number): Promise<ExpenditureRecord[]> {
  console.log(`Searching expenditures for: ${vendorName} (FY${fiscalYear || 'all'})`);
  
  // In a real implementation, you would:
  // 1. Navigate to https://business.nh.gov/ExpenditureTransparency/
  // 2. Submit the search form with vendor name
  // 3. Parse the results table
  
  // For now, return empty and note this requires manual data entry or API access
  console.log('Note: TransparentNH requires form submission. Use manual export or API.');
  
  return [];
}

/**
 * Import expenditure data from CSV export
 * Users can export data from TransparentNH and import it here
 */
export function parseExpenditureCSV(csvContent: string): ExpenditureRecord[] {
  const lines = csvContent.split('\n');
  const records: ExpenditureRecord[] = [];
  
  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    // Parse CSV (handling quoted fields)
    const fields = line.match(/(".*?"|[^,]+)/g)?.map(f => f.replace(/^"|"$/g, '').trim()) || [];
    
    if (fields.length >= 7) {
      records.push({
        fiscalYear: parseInt(fields[0]) || new Date().getFullYear(),
        department: fields[1] || '',
        agency: fields[2] || '',
        activity: fields[3] || '',
        expenseClass: fields[4] || '',
        vendorName: fields[5] || '',
        amount: parseFloat(fields[6].replace(/[$,]/g, '')) || 0,
        paymentDate: fields[7] || null,
        description: fields[8] || null,
      });
    }
  }
  
  return records;
}

/**
 * Save expenditure records to database
 */
export async function saveExpenditures(records: ExpenditureRecord[]): Promise<number> {
  console.log(`Saving ${records.length} expenditure records...`);
  
  let saved = 0;
  
  for (const record of records) {
    // Try to match vendor to known contractor
    const contractor = await dbHelpers.getContractorByName(record.vendorName) as { id: number } | null;
    
    try {
      await dbHelpers.insertExpenditure({
        contractor_id: contractor?.id || null,
        contract_id: null,
        fiscal_year: record.fiscalYear,
        department: record.department,
        agency: record.agency,
        activity: record.activity,
        expense_class: record.expenseClass,
        vendor_name: record.vendorName,
        amount: record.amount,
        payment_date: record.paymentDate,
        description: record.description,
        source_url: 'https://business.nh.gov/ExpenditureTransparency/',
      });
      saved++;
    } catch (error) {
      console.error(`Error saving expenditure for ${record.vendorName}:`, error);
    }
  }
  
  console.log(`Saved ${saved} of ${records.length} records`);
  return saved;
}

/**
 * Generate sample expenditure data for demonstration
 * In production, this would be replaced with actual scraped data
 */
export function generateSampleExpenditures(): ExpenditureRecord[] {
  return [
    {
      fiscalYear: 2024,
      department: 'Health and Human Services',
      agency: 'Office of Health Equity',
      activity: 'Refugee Resettlement Services',
      expenseClass: 'Contracts',
      vendorName: 'Ascentria Community Services, Inc.',
      amount: 125000,
      paymentDate: '2024-03-15',
      description: 'Q1 FY24 Refugee Support Services',
    },
    {
      fiscalYear: 2024,
      department: 'Health and Human Services',
      agency: 'Office of Health Equity',
      activity: 'Refugee Resettlement Services',
      expenseClass: 'Contracts',
      vendorName: 'Ascentria Community Services, Inc.',
      amount: 125000,
      paymentDate: '2024-06-15',
      description: 'Q2 FY24 Refugee Support Services',
    },
    {
      fiscalYear: 2024,
      department: 'Health and Human Services',
      agency: 'Office of Health Equity',
      activity: 'Refugee Social Services',
      expenseClass: 'Contracts',
      vendorName: 'International Institute of New England, Inc.',
      amount: 185000,
      paymentDate: '2024-02-28',
      description: 'Refugee Employment and ESL Programs',
    },
    {
      fiscalYear: 2024,
      department: 'Health and Human Services',
      agency: 'Office of Health Equity',
      activity: 'Language Access Services',
      expenseClass: 'Contracts',
      vendorName: 'Ascentria Community Services, Inc.',
      amount: 45000,
      paymentDate: '2024-04-30',
      description: 'Translation and Interpretation Services',
    },
    {
      fiscalYear: 2023,
      department: 'Health and Human Services',
      agency: 'Office of Health Equity',
      activity: 'Ukrainian Resettlement',
      expenseClass: 'Contracts',
      vendorName: 'Ascentria Community Services, Inc.',
      amount: 251910,
      paymentDate: '2023-10-15',
      description: 'Ukrainian Displaced Persons Support',
    },
    {
      fiscalYear: 2023,
      department: 'Health and Human Services',
      agency: 'Office of the Commissioner',
      activity: 'Refugee School Services',
      expenseClass: 'Contracts',
      vendorName: 'International Institute of New England, Inc.',
      amount: 211250,
      paymentDate: '2023-09-01',
      description: 'School Year Refugee Youth Services',
    },
  ];
}

// Main execution
async function main() {
  await initializeDb();
  
  console.log('=== NH Expenditure Scraper ===\n');
  
  // Generate and save sample data for demonstration
  console.log('Generating sample expenditure data...');
  const samples = generateSampleExpenditures();
  const filtered = filterImmigrantRelated(samples);
  
  console.log(`\nFiltered to ${filtered.length} immigrant-related expenditures:`);
  for (const record of filtered) {
    console.log(`  - ${record.vendorName}: $${record.amount.toLocaleString()} (${record.activity})`);
  }
  
  await saveExpenditures(filtered);
  
  console.log('\nExpenditure import complete!');
  console.log('\nNote: For real data, export from TransparentNH and use parseExpenditureCSV()');
}

// Check if running directly
const isMain = process.argv[1]?.includes('expenditure');
if (isMain) {
  main().catch(console.error);
}

export default { 
  searchExpenditures, 
  parseExpenditureCSV, 
  saveExpenditures, 
  generateSampleExpenditures,
  filterImmigrantRelated 
};
