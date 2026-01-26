/**
 * Run all scrapers to collect NH contract data
 */

import { initializeDb } from '../db/database.js';
import { searchGCAgendas, saveScrapedData } from './governor-council.js';
import { generateSampleExpenditures, filterImmigrantRelated, saveExpenditures } from './expenditure.js';
import { scrapeDHHSContracts } from './dhhs-contracts.js';

async function runAllScrapers() {
  console.log('='.repeat(60));
  console.log('NH Immigrant Contracts Tracker - Data Collection');
  console.log('='.repeat(60));
  console.log();

  // Initialize database
  console.log('Initializing database...');
  await initializeDb();
  console.log();

  // Run Governor and Council scraper
  console.log('-'.repeat(60));
  console.log('Running Governor & Council Agenda Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const gcItems = await searchGCAgendas(2023, 2025);
    await saveScrapedData(gcItems);
    console.log(`Processed ${gcItems.length} G&C agenda items`);
  } catch (error) {
    console.error('Error in G&C scraper:', error);
  }
  
  console.log();

  // Run Expenditure scraper (sample data for demo)
  console.log('-'.repeat(60));
  console.log('Running Expenditure Scraper (Sample Data)...');
  console.log('-'.repeat(60));
  
  try {
    const expenditures = generateSampleExpenditures();
    const filtered = filterImmigrantRelated(expenditures);
    await saveExpenditures(filtered);
    console.log(`Processed ${filtered.length} expenditure records`);
  } catch (error) {
    console.error('Error in expenditure scraper:', error);
  }
  
  console.log();

  // Run DHHS Contracts scraper
  console.log('-'.repeat(60));
  console.log('Running DHHS Contracts Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const result = await scrapeDHHSContracts();
    console.log(`Processed ${result.stats.total} DHHS contracts`);
    console.log(`  - Immigrant-related: ${result.stats.immigrantRelated}`);
    console.log(`  - With fraud indicators: ${result.stats.withFraudIndicators}`);
  } catch (error) {
    console.error('Error in DHHS contracts scraper:', error);
  }
  
  console.log();
  console.log('='.repeat(60));
  console.log('Data collection complete!');
  console.log('='.repeat(60));
  console.log();
  console.log('Next steps:');
  console.log('  1. Run "npm run seed" to populate with known contract data');
  console.log('  2. Run "npm run dev" to start the web server');
  console.log('  3. Visit http://localhost:3000 to view the dashboard');
}

runAllScrapers().catch(console.error);
