/**
 * Census Bureau SAIPE (Small Area Income and Poverty Estimates) Scraper
 * Fetches county-level poverty and income data for New Hampshire
 *
 * Data source: https://api.census.gov/data/timeseries/poverty/saipe
 * Provides: Poverty rates, median income, children in poverty by county
 *
 * This data provides demographic context for childcare need analysis:
 * - High poverty areas may have more CCDF-eligible families
 * - Income data helps identify areas with childcare affordability issues
 */

import { initializeDb, saveDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';

const API_BASE = 'https://api.census.gov/data/timeseries/poverty/saipe';

// NH State FIPS code
const NH_STATE_FIPS = '33';

// NH County FIPS codes and names
const NH_COUNTIES: Record<string, string> = {
  '001': 'Belknap County',
  '003': 'Carroll County',
  '005': 'Cheshire County',
  '007': 'Coos County',
  '009': 'Grafton County',
  '011': 'Hillsborough County',
  '013': 'Merrimack County',
  '015': 'Rockingham County',
  '017': 'Strafford County',
  '019': 'Sullivan County',
};

// SAIPE variable definitions
const SAIPE_VARIABLES = {
  NAME: 'Geographic area name',
  SAEPOVALL_PT: 'All ages in poverty, estimate',
  SAEPOVRTALL_PT: 'All ages poverty rate, estimate',
  SAEPOV0_17_PT: 'Under 18 in poverty, estimate',
  SAEPOVRT0_17_PT: 'Under 18 poverty rate, estimate',
  SAEMHI_PT: 'Median household income, estimate',
  SAEPOV5_17R_PT: 'Ages 5-17 in families in poverty, estimate',
  SAEPOVRT5_17R_PT: 'Ages 5-17 in families poverty rate, estimate',
};

interface SAIPERecord {
  countyFips: string;
  countyName: string;
  year: number;
  povertyCount: number;
  povertyRate: number;
  childPovertyCount: number;
  childPovertyRate: number;
  medianHouseholdIncome: number;
  schoolAgePovertyCount: number;
  schoolAgePovertyRate: number;
}

interface ScrapeResult {
  success: boolean;
  source: string;
  year: number;
  countiesProcessed: number;
  recordsImported: number;
  stateStats: {
    totalPoverty: number;
    avgPovertyRate: number;
    totalChildPoverty: number;
    avgChildPovertyRate: number;
    avgMedianIncome: number;
  };
  counties: SAIPERecord[];
  error?: string;
}

/**
 * Fetch SAIPE data from Census API
 */
async function fetchSAIPEData(year: number): Promise<string[][] | null> {
  try {
    // Build variable list
    const variables = Object.keys(SAIPE_VARIABLES).join(',');

    // API endpoint for NH counties
    const url = `${API_BASE}?get=${variables}&for=county:*&in=state:${NH_STATE_FIPS}&YEAR=${year}`;
    console.log(`Fetching: ${url}`);

    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'NH-Childcare-Tracker/1.0',
      },
    });

    if (!response.ok) {
      console.error(`API error: ${response.status} ${response.statusText}`);
      return null;
    }

    const data = await response.json();
    return data as string[][];
  } catch (error) {
    console.error('Fetch error:', error);
    return null;
  }
}

/**
 * Parse API response into structured records
 */
function parseResponse(data: string[][], year: number): SAIPERecord[] {
  if (!data || data.length < 2) return [];

  const headers = data[0];
  const records: SAIPERecord[] = [];

  // Find column indices
  const getIndex = (name: string) => headers.indexOf(name);

  for (let i = 1; i < data.length; i++) {
    const row = data[i];

    const countyFips = row[getIndex('county')];
    const countyName = row[getIndex('NAME')] || NH_COUNTIES[countyFips] || `County ${countyFips}`;

    records.push({
      countyFips: `${NH_STATE_FIPS}${countyFips}`, // Full FIPS code
      countyName: countyName.replace(', New Hampshire', ''),
      year,
      povertyCount: parseInt(row[getIndex('SAEPOVALL_PT')]) || 0,
      povertyRate: parseFloat(row[getIndex('SAEPOVRTALL_PT')]) || 0,
      childPovertyCount: parseInt(row[getIndex('SAEPOV0_17_PT')]) || 0,
      childPovertyRate: parseFloat(row[getIndex('SAEPOVRT0_17_PT')]) || 0,
      medianHouseholdIncome: parseInt(row[getIndex('SAEMHI_PT')]) || 0,
      schoolAgePovertyCount: parseInt(row[getIndex('SAEPOV5_17R_PT')]) || 0,
      schoolAgePovertyRate: parseFloat(row[getIndex('SAEPOVRT5_17R_PT')]) || 0,
    });
  }

  return records;
}

/**
 * Save record to database
 */
async function saveRecord(record: SAIPERecord): Promise<boolean> {
  try {
    // Check for duplicate
    const existing = await query(`
      SELECT id FROM census_demographics
      WHERE county_fips = ? AND year = ?
      LIMIT 1
    `, [record.countyFips, record.year]);

    if (existing.length > 0) {
      // Update existing record
      await execute(`
        UPDATE census_demographics
        SET poverty_count = ?, poverty_rate = ?,
            child_poverty_count = ?, child_poverty_rate = ?,
            median_household_income = ?,
            school_age_poverty_count = ?, school_age_poverty_rate = ?,
            updated_at = datetime('now')
        WHERE county_fips = ? AND year = ?
      `, [
        record.povertyCount,
        record.povertyRate,
        record.childPovertyCount,
        record.childPovertyRate,
        record.medianHouseholdIncome,
        record.schoolAgePovertyCount,
        record.schoolAgePovertyRate,
        record.countyFips,
        record.year,
      ]);
      return false; // Updated, not new
    }

    // Insert new record
    await execute(`
      INSERT INTO census_demographics (
        county_fips, county_name, year,
        poverty_count, poverty_rate,
        child_poverty_count, child_poverty_rate,
        median_household_income,
        school_age_poverty_count, school_age_poverty_rate,
        source, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    `, [
      record.countyFips,
      record.countyName,
      record.year,
      record.povertyCount,
      record.povertyRate,
      record.childPovertyCount,
      record.childPovertyRate,
      record.medianHouseholdIncome,
      record.schoolAgePovertyCount,
      record.schoolAgePovertyRate,
      'census_saipe',
    ]);

    return true;
  } catch (error) {
    console.error('Error saving record:', error);
    return false;
  }
}

/**
 * Create census_demographics table if not exists
 */
async function ensureTable(): Promise<void> {
  await execute(`
    CREATE TABLE IF NOT EXISTS census_demographics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      county_fips TEXT NOT NULL,
      county_name TEXT NOT NULL,
      year INTEGER NOT NULL,
      poverty_count INTEGER,
      poverty_rate REAL,
      child_poverty_count INTEGER,
      child_poverty_rate REAL,
      median_household_income REAL,
      school_age_poverty_count INTEGER,
      school_age_poverty_rate REAL,
      children_under_5 INTEGER,
      source TEXT DEFAULT 'census_saipe',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(county_fips, year)
    )
  `, []);

  await execute(`CREATE INDEX IF NOT EXISTS idx_census_fips ON census_demographics(county_fips)`, []);
  await execute(`CREATE INDEX IF NOT EXISTS idx_census_year ON census_demographics(year)`, []);
}

/**
 * Main scrape function for single year
 */
export async function scrapeSAIPE(year: number): Promise<ScrapeResult> {
  const result: ScrapeResult = {
    success: false,
    source: 'Census Bureau SAIPE',
    year,
    countiesProcessed: 0,
    recordsImported: 0,
    stateStats: {
      totalPoverty: 0,
      avgPovertyRate: 0,
      totalChildPoverty: 0,
      avgChildPovertyRate: 0,
      avgMedianIncome: 0,
    },
    counties: [],
  };

  try {
    console.log(`\n=== Census SAIPE Scraper (${year}) ===`);

    await initializeDb();
    await ensureTable();

    // Fetch data
    const rawData = await fetchSAIPEData(year);
    if (!rawData) {
      result.error = 'Failed to fetch SAIPE data';
      return result;
    }

    // Parse response
    const records = parseResponse(rawData, year);
    result.countiesProcessed = records.length;
    console.log(`Parsed ${records.length} county records`);

    // Save each record
    for (const record of records) {
      const isNew = await saveRecord(record);
      if (isNew) {
        result.recordsImported++;
      }
      result.counties.push(record);
    }

    // Calculate state stats
    if (records.length > 0) {
      result.stateStats.totalPoverty = records.reduce((sum, r) => sum + r.povertyCount, 0);
      result.stateStats.avgPovertyRate = records.reduce((sum, r) => sum + r.povertyRate, 0) / records.length;
      result.stateStats.totalChildPoverty = records.reduce((sum, r) => sum + r.childPovertyCount, 0);
      result.stateStats.avgChildPovertyRate = records.reduce((sum, r) => sum + r.childPovertyRate, 0) / records.length;
      result.stateStats.avgMedianIncome = records.reduce((sum, r) => sum + r.medianHouseholdIncome, 0) / records.length;
    }

    await saveDb();
    result.success = true;

    console.log('\n=== Scrape Complete ===');
    console.log(`Counties: ${result.countiesProcessed}`);
    console.log(`New records: ${result.recordsImported}`);
    console.log(`State poverty count: ${result.stateStats.totalPoverty.toLocaleString()}`);
    console.log(`Avg poverty rate: ${result.stateStats.avgPovertyRate.toFixed(1)}%`);
    console.log(`Child poverty: ${result.stateStats.totalChildPoverty.toLocaleString()}`);
    console.log(`Avg median income: $${result.stateStats.avgMedianIncome.toLocaleString()}`);

  } catch (error) {
    result.error = error instanceof Error ? error.message : 'Unknown error';
    console.error('SAIPE scrape error:', error);
  }

  return result;
}

/**
 * Scrape multiple years
 */
export async function scrapeSAIPEMultipleYears(
  startYear: number = 2019,
  endYear?: number
): Promise<ScrapeResult[]> {
  const currentYear = endYear || new Date().getFullYear() - 1; // SAIPE data lags by ~1 year
  const results: ScrapeResult[] = [];

  for (let year = startYear; year <= currentYear; year++) {
    console.log(`\nProcessing year ${year}...`);
    const result = await scrapeSAIPE(year);
    results.push(result);

    // Rate limit between years
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  return results;
}

/**
 * Get high-poverty counties (for targeting analysis)
 */
export async function getHighPovertyCounties(
  year?: number,
  threshold: number = 10.0
): Promise<SAIPERecord[]> {
  await initializeDb();
  await ensureTable();

  const yearClause = year ? `AND year = ${year}` : '';

  const results = await query(`
    SELECT
      county_fips as countyFips,
      county_name as countyName,
      year,
      poverty_count as povertyCount,
      poverty_rate as povertyRate,
      child_poverty_count as childPovertyCount,
      child_poverty_rate as childPovertyRate,
      median_household_income as medianHouseholdIncome,
      school_age_poverty_count as schoolAgePovertyCount,
      school_age_poverty_rate as schoolAgePovertyRate
    FROM census_demographics
    WHERE child_poverty_rate >= ? ${yearClause}
    ORDER BY child_poverty_rate DESC
  `, [threshold]);

  return results.map((r: Record<string, unknown>) => ({
    countyFips: r.countyFips as string,
    countyName: r.countyName as string,
    year: r.year as number,
    povertyCount: r.povertyCount as number,
    povertyRate: r.povertyRate as number,
    childPovertyCount: r.childPovertyCount as number,
    childPovertyRate: r.childPovertyRate as number,
    medianHouseholdIncome: r.medianHouseholdIncome as number,
    schoolAgePovertyCount: r.schoolAgePovertyCount as number,
    schoolAgePovertyRate: r.schoolAgePovertyRate as number,
  }));
}

/**
 * Get summary stats across all years
 */
export async function getSAIPESummary(): Promise<{
  yearsAvailable: number[];
  latestYear: number;
  statewidePoverty: number;
  statewideChildPoverty: number;
  highestPovertyCounty: string;
  lowestIncomeCounty: string;
}> {
  await initializeDb();
  await ensureTable();

  const years = await query(`
    SELECT DISTINCT year FROM census_demographics ORDER BY year DESC
  `);

  const latestYear = years[0]?.year || 0;

  const statewide = await query(`
    SELECT
      SUM(poverty_count) as total_poverty,
      SUM(child_poverty_count) as total_child_poverty
    FROM census_demographics
    WHERE year = ?
  `, [latestYear]);

  const highestPoverty = await query(`
    SELECT county_name
    FROM census_demographics
    WHERE year = ?
    ORDER BY child_poverty_rate DESC
    LIMIT 1
  `, [latestYear]);

  const lowestIncome = await query(`
    SELECT county_name
    FROM census_demographics
    WHERE year = ?
    ORDER BY median_household_income ASC
    LIMIT 1
  `, [latestYear]);

  return {
    yearsAvailable: years.map((r: Record<string, unknown>) => r.year as number),
    latestYear,
    statewidePoverty: statewide[0]?.total_poverty || 0,
    statewideChildPoverty: statewide[0]?.total_child_poverty || 0,
    highestPovertyCounty: highestPoverty[0]?.county_name || 'Unknown',
    lowestIncomeCounty: lowestIncome[0]?.county_name || 'Unknown',
  };
}

export default {
  scrapeSAIPE,
  scrapeSAIPEMultipleYears,
  getHighPovertyCounties,
  getSAIPESummary,
  NH_COUNTIES,
};
