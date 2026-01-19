/**
 * Scheduled Data Ingestion Tasks
 * Automated data collection from various sources on defined schedules
 *
 * Schedule Overview:
 * - Daily 6 AM UTC: USAspending.gov federal awards
 * - Weekly Monday 4 AM UTC: ACF CCDF statistics
 * - Weekly Tuesday 4 AM UTC: ProPublica 990 nonprofit data
 * - Weekly Wednesday 4 AM UTC: Census SAIPE demographics
 * - Weekly Thursday 4 AM UTC: Data.gov CCDF bulk data
 * - Weekly Monday 8 AM UTC: Fraud analysis
 * - Weekly Sunday 2 AM UTC: Full data refresh orchestration
 * - Monthly 1st 3 AM UTC: Data quality check
 */

import { schedules, logger } from "@trigger.dev/sdk";
import { scrapeUSASpending } from "../scraper/usaspending-scraper.js";
import { scrapeACFData, scrapeACFMultipleYears } from "../scraper/acf-ccdf-scraper.js";
import { scrapeProPublica990 } from "../scraper/propublica-990-scraper.js";
import { scrapeSAIPE, scrapeSAIPEMultipleYears } from "../scraper/census-saipe-scraper.js";
import { scrapeDataGovCCDF, scrapeAllYears as scrapeAllCCDFYears } from "../scraper/datagov-ccdf-scraper.js";
import { runFullFraudAnalysis } from "../analyzer/fraud-detector.js";
import { initializeDb } from "../db/database.js";
import { execute, query } from "../db/db-adapter.js";

// Helper to get current federal fiscal year (Oct 1 - Sep 30)
function getCurrentFiscalYear(): number {
  const now = new Date();
  return now.getMonth() >= 9 ? now.getFullYear() + 1 : now.getFullYear();
}

// Helper to log ingestion runs
async function logIngestionRun(
  source: string,
  status: 'started' | 'completed' | 'failed',
  details: Record<string, unknown> = {}
): Promise<number | null> {
  try {
    await initializeDb();

    if (status === 'started') {
      const result = await execute(`
        INSERT INTO ingestion_runs (source, status, started_at, details)
        VALUES (?, ?, datetime('now'), ?)
      `, [source, status, JSON.stringify(details)]);
      return result.lastId ?? null;
    } else {
      // Update existing run
      if (details.runId) {
        await execute(`
          UPDATE ingestion_runs
          SET status = ?, completed_at = datetime('now'),
              records_processed = ?, records_imported = ?,
              details = ?, error_message = ?
          WHERE id = ?
        `, [
          status,
          details.recordsProcessed || 0,
          details.recordsImported || 0,
          JSON.stringify(details),
          details.error || null,
          details.runId
        ]);
      }
    }
  } catch (error) {
    logger.error("Failed to log ingestion run", { error, source, status });
  }
  return null;
}

/**
 * Daily USAspending.gov Scraper
 * Fetches federal CCDF awards for current fiscal year
 * Runs daily at 6 AM UTC to catch new awards
 */
export const dailyUSASpendingIngest = schedules.task({
  id: "daily-usaspending-ingest",
  cron: "0 6 * * *", // Daily at 6 AM UTC
  run: async (payload) => {
    const fiscalYear = getCurrentFiscalYear();
    logger.info("Daily USAspending.gov ingestion started", {
      fiscalYear,
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('usaspending', 'started', { fiscalYear });

    try {
      const result = await scrapeUSASpending(fiscalYear);

      await logIngestionRun('usaspending', result.success ? 'completed' : 'failed', {
        runId,
        fiscalYear,
        recordsProcessed: result.totalAwards,
        recordsImported: result.importedRecords,
        totalAmount: result.totalAmount,
        error: result.error
      });

      logger.info("Daily USAspending.gov ingestion complete", {
        success: result.success,
        totalAwards: result.totalAwards,
        importedRecords: result.importedRecords,
        totalAmount: result.totalAmount
      });

      return {
        success: result.success,
        source: 'usaspending',
        fiscalYear,
        totalAwards: result.totalAwards,
        importedRecords: result.importedRecords,
        totalAmount: result.totalAmount,
        error: result.error
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await logIngestionRun('usaspending', 'failed', { runId, error: errorMessage });
      logger.error("Daily USAspending.gov ingestion failed", { error: errorMessage });
      throw error;
    }
  },
});

/**
 * Weekly ACF CCDF Data Scraper
 * Fetches official HHS CCDF expenditure statistics
 * Runs Monday at 4 AM UTC
 */
export const weeklyACFCCDFIngest = schedules.task({
  id: "weekly-acf-ccdf-ingest",
  cron: "0 4 * * 1", // Monday at 4 AM UTC
  run: async (payload) => {
    logger.info("Weekly ACF CCDF ingestion started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('acf-ccdf', 'started', {});

    try {
      // Scrape current and previous fiscal year
      const currentFY = getCurrentFiscalYear();
      const results = await scrapeACFMultipleYears();

      const totalImported = results.reduce((sum, r) => sum + r.importedRecords, 0);
      const totalExpenditure = results.reduce((sum, r) => sum + (r.nhStats?.totalExpenditure || 0), 0);
      const allSuccessful = results.every(r => r.success);

      await logIngestionRun('acf-ccdf', allSuccessful ? 'completed' : 'failed', {
        runId,
        recordsProcessed: results.length,
        recordsImported: totalImported,
        totalExpenditure,
        years: results.map(r => r.fiscalYear)
      });

      logger.info("Weekly ACF CCDF ingestion complete", {
        success: allSuccessful,
        yearsProcessed: results.length,
        totalImported,
        totalExpenditure
      });

      return {
        success: allSuccessful,
        source: 'acf-ccdf',
        yearsProcessed: results.length,
        totalImported,
        totalExpenditure,
        results: results.map(r => ({
          fiscalYear: r.fiscalYear,
          success: r.success,
          importedRecords: r.importedRecords,
          nhStats: r.nhStats
        }))
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await logIngestionRun('acf-ccdf', 'failed', { runId, error: errorMessage });
      logger.error("Weekly ACF CCDF ingestion failed", { error: errorMessage });
      throw error;
    }
  },
});

/**
 * Weekly Fraud Analysis
 * Runs comprehensive fraud detection on all ingested data
 * Runs Monday at 8 AM UTC (after data ingestion completes)
 */
export const weeklyFraudAnalysis = schedules.task({
  id: "weekly-fraud-analysis",
  cron: "0 8 * * 1", // Monday at 8 AM UTC
  run: async (payload) => {
    logger.info("Weekly fraud analysis started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('fraud-analysis', 'started', {});

    try {
      const result = await runFullFraudAnalysis();

      await logIngestionRun('fraud-analysis', 'completed', {
        runId,
        recordsProcessed: result.structuring.length + result.duplicates.length,
        recordsImported: result.savedIndicators,
        structuringFlags: result.structuring.length,
        duplicateFlags: result.duplicates.length,
        topVendors: result.vendorConcentration.slice(0, 5)
      });

      logger.info("Weekly fraud analysis complete", {
        structuringFlags: result.structuring.length,
        duplicateFlags: result.duplicates.length,
        savedIndicators: result.savedIndicators
      });

      return {
        success: true,
        source: 'fraud-analysis',
        structuringFlags: result.structuring.length,
        duplicateFlags: result.duplicates.length,
        savedIndicators: result.savedIndicators,
        topVendors: result.vendorConcentration.slice(0, 5).map(v => ({
          vendor: v.vendor,
          totalAmount: v.total_amount,
          transactionCount: v.transaction_count
        }))
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await logIngestionRun('fraud-analysis', 'failed', { runId, error: errorMessage });
      logger.error("Weekly fraud analysis failed", { error: errorMessage });
      throw error;
    }
  },
});

/**
 * Weekly Full Data Refresh Orchestration
 * Master task that coordinates all data ingestion in sequence
 * Runs Sunday at 2 AM UTC for comprehensive weekly refresh
 */
export const weeklyFullRefresh = schedules.task({
  id: "weekly-full-refresh",
  cron: "0 2 * * 0", // Sunday at 2 AM UTC
  maxDuration: 600, // 10 minutes for full orchestration
  run: async (payload) => {
    logger.info("Weekly full data refresh started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('full-refresh', 'started', {});
    const results: Record<string, Record<string, unknown>> = {};
    const errors: string[] = [];

    // 1. USAspending.gov - Federal awards
    try {
      logger.info("Step 1/4: Scraping USAspending.gov");
      const fiscalYear = getCurrentFiscalYear();
      const usaResult = await scrapeUSASpending(fiscalYear);
      results.usaspending = {
        success: usaResult.success,
        totalAwards: usaResult.totalAwards,
        importedRecords: usaResult.importedRecords,
        totalAmount: usaResult.totalAmount
      };
      logger.info("USAspending.gov complete", results.usaspending);
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      errors.push(`USAspending: ${msg}`);
      results.usaspending = { success: false, error: msg };
      logger.error("USAspending.gov failed", { error: msg });
    }

    // Small delay between sources
    await new Promise(resolve => setTimeout(resolve, 2000));

    // 2. ACF CCDF - HHS statistics
    try {
      logger.info("Step 2/4: Scraping ACF CCDF data");
      const acfResults = await scrapeACFMultipleYears();
      const totalImported = acfResults.reduce((sum, r) => sum + r.importedRecords, 0);
      results.acfCcdf = {
        success: acfResults.every(r => r.success),
        yearsProcessed: acfResults.length,
        totalImported,
        totalExpenditure: acfResults.reduce((sum, r) => sum + (r.nhStats?.totalExpenditure || 0), 0)
      };
      logger.info("ACF CCDF complete", results.acfCcdf);
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      errors.push(`ACF CCDF: ${msg}`);
      results.acfCcdf = { success: false, error: msg };
      logger.error("ACF CCDF failed", { error: msg });
    }

    // Small delay
    await new Promise(resolve => setTimeout(resolve, 2000));

    // 3. Get ingestion summary stats
    try {
      logger.info("Step 3/4: Gathering data summary");
      await initializeDb();

      const providerCount = await query('SELECT COUNT(*) as count FROM providers');
      const expenditureCount = await query('SELECT COUNT(*) as count FROM expenditures');
      const totalAmount = await query('SELECT SUM(amount) as total FROM expenditures');

      results.summary = {
        providers: providerCount[0]?.count || 0,
        expenditures: expenditureCount[0]?.count || 0,
        totalAmount: totalAmount[0]?.total || 0
      };
      logger.info("Data summary complete", results.summary);
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      logger.error("Summary stats failed", { error: msg });
      results.summary = { error: msg };
    }

    // 4. Run fraud analysis on refreshed data
    try {
      logger.info("Step 4/4: Running fraud analysis");
      const fraudResult = await runFullFraudAnalysis();
      results.fraudAnalysis = {
        success: true,
        structuringFlags: fraudResult.structuring.length,
        duplicateFlags: fraudResult.duplicates.length,
        savedIndicators: fraudResult.savedIndicators
      };
      logger.info("Fraud analysis complete", results.fraudAnalysis);
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      errors.push(`Fraud analysis: ${msg}`);
      results.fraudAnalysis = { success: false, error: msg };
      logger.error("Fraud analysis failed", { error: msg });
    }

    const overallSuccess = errors.length === 0;

    await logIngestionRun('full-refresh', overallSuccess ? 'completed' : 'failed', {
      runId,
      recordsProcessed: 4,
      recordsImported: (results.usaspending as any)?.importedRecords || 0,
      results,
      errors: errors.length > 0 ? errors : undefined
    });

    logger.info("Weekly full data refresh complete", {
      success: overallSuccess,
      errors: errors.length,
      results
    });

    return {
      success: overallSuccess,
      source: 'full-refresh',
      completedAt: new Date().toISOString(),
      results,
      errors: errors.length > 0 ? errors : undefined
    };
  },
});

/**
 * Monthly Data Quality Check
 * Validates data integrity and identifies stale records
 * Runs 1st of each month at 3 AM UTC
 */
export const monthlyDataQualityCheck = schedules.task({
  id: "monthly-data-quality-check",
  cron: "0 3 1 * *", // 1st of month at 3 AM UTC
  run: async (payload) => {
    logger.info("Monthly data quality check started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    await initializeDb();
    const issues: string[] = [];
    const stats: Record<string, unknown> = {};

    try {
      // Check for duplicate providers
      const duplicateProviders = await query(`
        SELECT name, COUNT(*) as count
        FROM providers
        GROUP BY LOWER(name)
        HAVING count > 1
      `);
      if (duplicateProviders.length > 0) {
        issues.push(`Found ${duplicateProviders.length} potential duplicate providers`);
      }
      stats.duplicateProviders = duplicateProviders.length;

      // Check for orphaned expenditures (no provider match)
      const orphanedExpenditures = await query(`
        SELECT COUNT(*) as count
        FROM expenditures
        WHERE provider_id IS NULL
      `);
      stats.orphanedExpenditures = orphanedExpenditures[0]?.count || 0;

      // Check for stale data (no updates in 30 days)
      const recentIngestions = await query(`
        SELECT source, MAX(completed_at) as last_run
        FROM ingestion_runs
        WHERE status = 'completed'
        GROUP BY source
      `);
      stats.lastIngestions = recentIngestions;

      // Check fraud indicators needing review
      const openIndicators = await query(`
        SELECT severity, COUNT(*) as count
        FROM fraud_indicators
        WHERE status = 'open'
        GROUP BY severity
      `);
      stats.openFraudIndicators = openIndicators;

      // Check data coverage by fiscal year
      const yearCoverage = await query(`
        SELECT fiscal_year, COUNT(*) as count, SUM(amount) as total
        FROM expenditures
        GROUP BY fiscal_year
        ORDER BY fiscal_year DESC
      `);
      stats.fiscalYearCoverage = yearCoverage;

      logger.info("Monthly data quality check complete", {
        issues: issues.length,
        stats
      });

      return {
        success: true,
        source: 'data-quality-check',
        issues,
        stats,
        checkedAt: new Date().toISOString()
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      logger.error("Monthly data quality check failed", { error: errorMessage });
      return {
        success: false,
        source: 'data-quality-check',
        error: errorMessage
      };
    }
  },
});

/**
 * Weekly ProPublica 990 Nonprofit Scraper
 * Fetches IRS 990 filings for NH childcare nonprofits
 * Runs Tuesday at 4 AM UTC
 */
export const weeklyProPublica990Ingest = schedules.task({
  id: "weekly-propublica-990-ingest",
  cron: "0 4 * * 2", // Tuesday at 4 AM UTC
  run: async (payload) => {
    logger.info("Weekly ProPublica 990 ingestion started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('propublica-990', 'started', {});

    try {
      const result = await scrapeProPublica990({
        maxOrgs: 75,
        recentYearsOnly: true,
      });

      await logIngestionRun('propublica-990', result.success ? 'completed' : 'failed', {
        runId,
        recordsProcessed: result.filingsProcessed,
        recordsImported: result.filingsImported,
        organizationsFound: result.organizationsFound,
        totalRevenue: result.totalRevenue,
        error: result.error
      });

      logger.info("Weekly ProPublica 990 ingestion complete", {
        success: result.success,
        organizationsFound: result.organizationsFound,
        filingsImported: result.filingsImported,
        totalRevenue: result.totalRevenue
      });

      return {
        success: result.success,
        source: 'propublica-990',
        organizationsFound: result.organizationsFound,
        filingsProcessed: result.filingsProcessed,
        filingsImported: result.filingsImported,
        totalRevenue: result.totalRevenue,
        error: result.error
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await logIngestionRun('propublica-990', 'failed', { runId, error: errorMessage });
      logger.error("Weekly ProPublica 990 ingestion failed", { error: errorMessage });
      throw error;
    }
  },
});

/**
 * Weekly Census SAIPE Demographics Scraper
 * Fetches county-level poverty and income data for NH
 * Runs Wednesday at 4 AM UTC
 */
export const weeklyCensusSAIPEIngest = schedules.task({
  id: "weekly-census-saipe-ingest",
  cron: "0 4 * * 3", // Wednesday at 4 AM UTC
  run: async (payload) => {
    logger.info("Weekly Census SAIPE ingestion started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('census-saipe', 'started', {});

    try {
      // Scrape last 3 years
      const currentYear = new Date().getFullYear() - 1;
      const results = await scrapeSAIPEMultipleYears(currentYear - 2, currentYear);

      const totalCounties = results.reduce((sum, r) => sum + r.countiesProcessed, 0);
      const totalImported = results.reduce((sum, r) => sum + r.recordsImported, 0);
      const allSuccess = results.every(r => r.success);

      // Get stats from latest successful result
      const latestResult = results.find(r => r.success) || results[0];

      await logIngestionRun('census-saipe', allSuccess ? 'completed' : 'failed', {
        runId,
        recordsProcessed: totalCounties,
        recordsImported: totalImported,
        yearsProcessed: results.length,
        avgChildPovertyRate: latestResult?.stateStats.avgChildPovertyRate,
        error: results.find(r => r.error)?.error
      });

      logger.info("Weekly Census SAIPE ingestion complete", {
        success: allSuccess,
        yearsProcessed: results.length,
        countiesProcessed: totalCounties,
        recordsImported: totalImported
      });

      return {
        success: allSuccess,
        source: 'census-saipe',
        yearsProcessed: results.length,
        countiesProcessed: totalCounties,
        recordsImported: totalImported,
        stateStats: latestResult?.stateStats,
        error: results.find(r => r.error)?.error
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await logIngestionRun('census-saipe', 'failed', { runId, error: errorMessage });
      logger.error("Weekly Census SAIPE ingestion failed", { error: errorMessage });
      throw error;
    }
  },
});

/**
 * Weekly Data.gov CCDF Bulk Scraper
 * Fetches official CCDF administrative statistics
 * Runs Thursday at 4 AM UTC
 */
export const weeklyDataGovCCDFIngest = schedules.task({
  id: "weekly-datagov-ccdf-ingest",
  cron: "0 4 * * 4", // Thursday at 4 AM UTC
  run: async (payload) => {
    logger.info("Weekly Data.gov CCDF ingestion started", {
      scheduledTime: payload.timestamp,
      timezone: payload.timezone
    });

    const runId = await logIngestionRun('datagov-ccdf', 'started', {});

    try {
      // Scrape all available years
      const results = await scrapeAllCCDFYears();

      const totalImported = results.reduce((sum, r) => sum + r.recordsImported, 0);
      const allSuccess = results.every(r => r.success);
      const latestResult = results.find(r => r.success && r.nhData);

      await logIngestionRun('datagov-ccdf', allSuccess ? 'completed' : 'failed', {
        runId,
        recordsProcessed: results.length,
        recordsImported: totalImported,
        yearsProcessed: results.length,
        nhData: latestResult?.nhData,
        error: results.find(r => r.error)?.error
      });

      logger.info("Weekly Data.gov CCDF ingestion complete", {
        success: allSuccess,
        yearsProcessed: results.length,
        recordsImported: totalImported,
        childrenServed: latestResult?.nhData?.childrenServed,
        totalExpenditure: latestResult?.nhData?.totalExpenditure
      });

      return {
        success: allSuccess,
        source: 'datagov-ccdf',
        yearsProcessed: results.length,
        recordsImported: totalImported,
        nhData: latestResult?.nhData,
        error: results.find(r => r.error)?.error
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await logIngestionRun('datagov-ccdf', 'failed', { runId, error: errorMessage });
      logger.error("Weekly Data.gov CCDF ingestion failed", { error: errorMessage });
      throw error;
    }
  },
});

// Export all scheduled tasks
export default {
  dailyUSASpendingIngest,
  weeklyACFCCDFIngest,
  weeklyProPublica990Ingest,
  weeklyCensusSAIPEIngest,
  weeklyDataGovCCDFIngest,
  weeklyFraudAnalysis,
  weeklyFullRefresh,
  monthlyDataQualityCheck,
};
