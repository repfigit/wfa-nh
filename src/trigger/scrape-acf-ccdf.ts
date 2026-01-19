/**
 * ACF CCDF Data Scraper Task
 * Fetches official CCDF expenditure data from ACF.hhs.gov
 * Runs in Trigger.dev for reliable execution
 */

import { task, logger } from "@trigger.dev/sdk";
import { 
  scrapeACFData, 
  scrapeACFMultipleYears,
  getAvailableFiscalYears,
  getNHCCDFStats 
} from "../scraper/acf-ccdf-scraper.js";

export interface ACFCCDFPayload {
  fiscalYear?: number;
  allYears?: boolean;
}

export interface ACFCCDFResult {
  success: boolean;
  source: string;
  results: Array<{
    fiscalYear: number;
    success: boolean;
    nhStats: {
      totalExpenditure: number;
      federalExpenditure: number;
      stateExpenditure: number;
      qualityInvestment: number;
      childrenServed: number;
      familiesServed: number;
      avgMonthlySubsidy: number;
    } | null;
    importedRecords: number;
    error?: string;
  }>;
  summary: {
    yearsProcessed: number;
    totalImported: number;
    totalExpenditure: number;
  };
  availableYears: number[];
}

export const scrapeACFCCDFTask = task({
  id: "scrape-acf-ccdf",
  maxDuration: 120, // 2 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: ACFCCDFPayload): Promise<ACFCCDFResult> => {
    logger.info("ACF CCDF scraper started", { payload });

    const availableYears = getAvailableFiscalYears();
    
    let scrapeResults;
    if (payload.allYears) {
      logger.info("Scraping all available fiscal years", { years: availableYears });
      scrapeResults = await scrapeACFMultipleYears();
    } else {
      const year = payload.fiscalYear || availableYears[0];
      logger.info("Scraping single fiscal year", { year });
      scrapeResults = [await scrapeACFData(year)];
    }

    const results = scrapeResults.map(r => ({
      fiscalYear: r.fiscalYear,
      success: r.success,
      nhStats: r.nhStats,
      importedRecords: r.importedRecords,
      error: r.error,
    }));

    const summary = {
      yearsProcessed: results.length,
      totalImported: results.reduce((sum, r) => sum + r.importedRecords, 0),
      totalExpenditure: results.reduce((sum, r) => sum + (r.nhStats?.totalExpenditure || 0), 0),
    };

    logger.info("ACF CCDF scraper complete", summary);

    return {
      success: results.every(r => r.success),
      source: 'ACF.hhs.gov',
      results,
      summary,
      availableYears,
    };
  },
});
