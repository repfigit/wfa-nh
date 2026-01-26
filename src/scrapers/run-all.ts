/**
 * Run all scrapers to collect NH contract data
 */

import { initializeDb } from '../db/database.js';
import { searchGCAgendas, saveScrapedData } from './governor-council.js';
import { generateSampleExpenditures, filterImmigrantRelated, saveExpenditures } from './expenditure.js';
import { scrapeDHHSContracts } from './dhhs-contracts.js';
import { scrapeDASBids } from './das-bids.js';
import { scrapeSAMGov } from './sam-gov.js';
import { scrapeCharitableTrusts } from './charitable-trusts.js';
import { scrapeFederalAuditClearinghouse } from './federal-audit-clearinghouse.js';
import { scrapeHHSTAGGS } from './hhs-taggs.js';
import { bridgeDHHSContracts } from '../bridge/dhhs-contracts-bridge.js';
import { bridgeFAC } from '../bridge/fac-bridge.js';
import { bridgeHHSTAGGS } from '../bridge/hhs-taggs-bridge.js';
import { bridgeDASBids } from '../bridge/das-bids-bridge.js';
import { bridgeSAMGov } from '../bridge/sam-gov-bridge.js';
import { bridgeCharitableTrusts } from '../bridge/charitable-trusts-bridge.js';

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

  // Run DAS Bids scraper
  console.log('-'.repeat(60));
  console.log('Running DAS Bid Board Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const result = await scrapeDASBids();
    console.log(`Processed ${result.stats.total} DAS bids`);
    console.log(`  - Immigrant-related: ${result.stats.immigrantRelated}`);
    console.log(`  - With fraud indicators: ${result.stats.withFraudIndicators}`);
  } catch (error) {
    console.error('Error in DAS bids scraper:', error);
  }
  
  console.log();

  // Run SAM.gov Federal Contracts scraper
  console.log('-'.repeat(60));
  console.log('Running SAM.gov Federal Contracts Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const result = await scrapeSAMGov();
    console.log(`Processed ${result.stats.total} federal awards`);
    console.log(`  - Total federal amount: $${result.stats.totalFederalAmount.toLocaleString()}`);
    console.log(`  - With fraud indicators: ${result.stats.withFraudIndicators}`);
  } catch (error) {
    console.error('Error in SAM.gov scraper:', error);
  }
  
  console.log();

  // Run Charitable Trusts scraper
  console.log('-'.repeat(60));
  console.log('Running NH Charitable Trusts Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const result = await scrapeCharitableTrusts();
    console.log(`Processed ${result.stats.total} nonprofit profiles`);
    console.log(`  - Combined revenue: $${result.stats.totalRevenue.toLocaleString()}`);
    console.log(`  - With fraud indicators: ${result.stats.withFraudIndicators}`);
  } catch (error) {
    console.error('Error in Charitable Trusts scraper:', error);
  }
  
  console.log();

  // Run Federal Audit Clearinghouse scraper
  console.log('-'.repeat(60));
  console.log('Running Federal Audit Clearinghouse Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const result = await scrapeFederalAuditClearinghouse();
    console.log(`Processed ${result.stats.total} audit reports`);
    console.log(`  - With findings: ${result.stats.withFindings}`);
    console.log(`  - Total questioned costs: $${result.stats.totalQuestionedCosts.toLocaleString()}`);
    console.log(`  - With fraud indicators: ${result.stats.withFraudIndicators}`);
  } catch (error) {
    console.error('Error in FAC scraper:', error);
  }
  
  console.log();

  // Run HHS TAGGS scraper
  console.log('-'.repeat(60));
  console.log('Running HHS TAGGS Scraper...');
  console.log('-'.repeat(60));
  
  try {
    const result = await scrapeHHSTAGGS();
    console.log(`Processed ${result.stats.total} HHS awards`);
    console.log(`  - Refugee-related: ${result.stats.refugeeAwards}`);
    console.log(`  - Childcare-related: ${result.stats.childcareAwards}`);
    console.log(`  - Total amount: $${result.stats.totalAmount.toLocaleString()}`);
    console.log(`  - With fraud indicators: ${result.stats.withFraudIndicators}`);
  } catch (error) {
    console.error('Error in HHS TAGGS scraper:', error);
  }
  
  console.log();

  // Bridge scraped data into master tables
  console.log('='.repeat(60));
  console.log('Bridging Data to Master Tables');
  console.log('='.repeat(60));
  console.log();

  // Bridge DHHS Contracts
  console.log('-'.repeat(60));
  console.log('Bridging DHHS Contracts...');
  console.log('-'.repeat(60));
  try {
    const dhhsResult = await bridgeDHHSContracts();
    console.log(`  Contractors: ${dhhsResult.contractorsImported} imported, ${dhhsResult.contractorsUpdated} updated`);
    console.log(`  Contracts: ${dhhsResult.contractsImported} imported`);
    console.log(`  Fraud Indicators: ${dhhsResult.fraudIndicatorsCreated} created`);
  } catch (error) {
    console.error('Error bridging DHHS contracts:', error);
  }
  console.log();

  // Bridge DAS Bids
  console.log('-'.repeat(60));
  console.log('Bridging DAS Bids...');
  console.log('-'.repeat(60));
  try {
    const dasResult = await bridgeDASBids();
    console.log(`  Contractors: ${dasResult.contractorsImported} imported, ${dasResult.contractorsUpdated} updated`);
    console.log(`  Contracts: ${dasResult.contractsImported} imported`);
    console.log(`  Fraud Indicators: ${dasResult.fraudIndicatorsCreated} created`);
  } catch (error) {
    console.error('Error bridging DAS bids:', error);
  }
  console.log();

  // Bridge SAM.gov
  console.log('-'.repeat(60));
  console.log('Bridging SAM.gov Federal Awards...');
  console.log('-'.repeat(60));
  try {
    const samResult = await bridgeSAMGov();
    console.log(`  Contractors: ${samResult.contractorsImported} imported, ${samResult.contractorsUpdated} updated`);
    console.log(`  Expenditures: ${samResult.expendituresImported} imported`);
    console.log(`  Fraud Indicators: ${samResult.fraudIndicatorsCreated} created`);
  } catch (error) {
    console.error('Error bridging SAM.gov data:', error);
  }
  console.log();

  // Bridge Charitable Trusts
  console.log('-'.repeat(60));
  console.log('Bridging Charitable Trusts / Form 990 Data...');
  console.log('-'.repeat(60));
  try {
    const ctResult = await bridgeCharitableTrusts();
    console.log(`  Contractors: ${ctResult.contractorsImported} imported, ${ctResult.contractorsUpdated} updated`);
    console.log(`  Fraud Indicators: ${ctResult.fraudIndicatorsCreated} created`);
  } catch (error) {
    console.error('Error bridging charitable trusts data:', error);
  }
  console.log();

  // Bridge Federal Audit Clearinghouse
  console.log('-'.repeat(60));
  console.log('Bridging Federal Audit Clearinghouse...');
  console.log('-'.repeat(60));
  try {
    const facResult = await bridgeFAC();
    console.log(`  Contractors: ${facResult.contractorsImported} imported, ${facResult.contractorsUpdated} updated`);
    console.log(`  Fraud Indicators: ${facResult.fraudIndicatorsCreated} created`);
  } catch (error) {
    console.error('Error bridging FAC data:', error);
  }
  console.log();

  // Bridge HHS TAGGS
  console.log('-'.repeat(60));
  console.log('Bridging HHS TAGGS...');
  console.log('-'.repeat(60));
  try {
    const taggsResult = await bridgeHHSTAGGS();
    console.log(`  Contractors: ${taggsResult.contractorsImported} imported, ${taggsResult.contractorsUpdated} updated`);
    console.log(`  Expenditures: ${taggsResult.expendituresImported} imported`);
    console.log(`  Fraud Indicators: ${taggsResult.fraudIndicatorsCreated} created`);
  } catch (error) {
    console.error('Error bridging HHS TAGGS data:', error);
  }
  console.log();

  console.log('='.repeat(60));
  console.log('Data collection and bridging complete!');
  console.log('='.repeat(60));
  console.log();
  console.log('Next steps:');
  console.log('  1. Run "npm run seed" to populate with known contract data');
  console.log('  2. Run "npm run dev" to start the web server');
  console.log('  3. Visit http://localhost:3000 to view the dashboard');
}

runAllScrapers().catch(console.error);
