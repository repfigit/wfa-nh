import { task, schedules } from "@trigger.dev/sdk";
import transparentNhMonthlyScraper from "../scraper/transparent-nh-monthly-scraper.js";
import transparentNhBridge from "../bridge/transparent-nh-bridge.js";

/**
 * One-time historical load of all fiscal years (2010-2025)
 * Run this once to populate historical data
 */
export const scrapeHistoricalTask = task({
  id: "transparent-nh-historical-load",
  retry: {
    maxAttempts: 3,
    factor: 2,
    minTimeoutInMs: 10000,
    maxTimeoutInMs: 300000,
  },
  run: async (payload?: { bridge?: boolean }) => {
    console.log("Starting historical data load...");
    const result = await transparentNhMonthlyScraper.scrapeHistorical();
    
    let bridgeResult = null;
    if (payload?.bridge !== false) {
      console.log("Bridging data to master tables...");
      bridgeResult = await transparentNhBridge.bridgeAll();
    }
    
    return {
      success: result.success,
      yearsScraped: result.yearsScraped,
      totalRows: result.totalRows,
      bridge: bridgeResult,
      message: `Loaded ${result.yearsScraped} fiscal years with ${result.totalRows} total expenditure records`
    };
  },
});

/**
 * Scrape a specific fiscal year
 */
export const scrapeFiscalYearTask = task({
  id: "transparent-nh-scrape-fiscal-year",
  retry: {
    maxAttempts: 3,
    factor: 2,
    minTimeoutInMs: 5000,
    maxTimeoutInMs: 60000,
  },
  run: async (payload: { fiscalYear: number }) => {
    console.log(`Scraping fiscal year ${payload.fiscalYear}...`);
    const result = await transparentNhMonthlyScraper.scrapeFiscalYear(payload.fiscalYear);
    
    return {
      success: result.success,
      fiscalYear: payload.fiscalYear,
      monthsScraped: result.monthsScraped,
      totalRows: result.totalRows,
      errors: result.errors
    };
  },
});

/**
 * Check for and scrape new months in current FY (2026)
 * This is the task that runs on a schedule
 */
export const scrapeNewMonthsTask = task({
  id: "transparent-nh-scrape-new-months",
  retry: {
    maxAttempts: 3,
    factor: 2,
    minTimeoutInMs: 5000,
    maxTimeoutInMs: 60000,
  },
  run: async (payload?: { bridge?: boolean }) => {
    console.log("Checking for new expenditure data...");
    const result = await transparentNhMonthlyScraper.scrapeNewMonths();
    
    let bridgeResult = null;
    if (result.newMonths.length > 0 && payload?.bridge !== false) {
      console.log("Bridging new data to master tables...");
      bridgeResult = await transparentNhBridge.bridgeFiscalYear(2026);
    }
    
    return {
      success: result.success,
      newMonths: result.newMonths,
      totalRows: result.totalRows,
      bridge: bridgeResult,
      message: result.newMonths.length > 0 
        ? `Found ${result.newMonths.length} new months: ${result.newMonths.join(', ')}`
        : 'No new data available'
    };
  },
});

/**
 * Scheduled task - runs weekly to check for new expenditure data
 * NH typically releases monthly data with a 1-2 month lag
 */
export const weeklyExpenditureCheck = schedules.task({
  id: "transparent-nh-weekly-check",
  cron: "0 6 * * 1", // Every Monday at 6 AM UTC
  run: async (payload) => {
    console.log(`Weekly expenditure check - ${payload.timestamp.toISOString()}`);
    
    const result = await transparentNhMonthlyScraper.scrapeNewMonths();
    
    let bridgeResult = null;
    if (result.newMonths.length > 0) {
      console.log("Bridging new data to master tables...");
      bridgeResult = await transparentNhBridge.bridgeFiscalYear(2026);
    }
    
    return {
      scheduledAt: payload.timestamp,
      timezone: payload.timezone,
      success: result.success,
      newMonths: result.newMonths,
      totalRows: result.totalRows,
      bridge: bridgeResult,
      message: result.newMonths.length > 0 
        ? `Found ${result.newMonths.length} new months of expenditure data`
        : 'No new expenditure data this week'
    };
  },
});

/**
 * Full refresh of current fiscal year
 * Re-downloads all available months (useful if data was corrected)
 */
export const refreshCurrentFYTask = task({
  id: "transparent-nh-refresh-current-fy",
  retry: {
    maxAttempts: 2,
    factor: 2,
    minTimeoutInMs: 10000,
    maxTimeoutInMs: 120000,
  },
  run: async () => {
    console.log("Full refresh of current fiscal year...");
    const result = await transparentNhMonthlyScraper.scrapeCurrentFiscalYear();
    
    return {
      success: result.success,
      monthsScraped: result.monthsScraped,
      totalRows: result.totalRows,
      message: `Refreshed FY2026: ${result.monthsScraped} months, ${result.totalRows} records`
    };
  },
});
