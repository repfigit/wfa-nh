/**
 * Data.gov CCDF Bulk Data Scraper Task
 * Fetches official CCDF administrative data
 * Runs in Trigger.dev for reliable execution
 */

import { task, logger } from "@trigger.dev/sdk";
import {
  scrapeDataGovCCDF,
  scrapeAllYears,
  getNHCCDFTrend,
  getAvailableFiscalYears,
} from "../scraper/datagov-ccdf-scraper.js";

export interface DataGovCCDFPayload {
  fiscalYear?: number;
  allYears?: boolean;
  includeTrend?: boolean;
}

export interface DataGovCCDFResult {
  success: boolean;
  source: string;
  fiscalYear?: number;
  yearsProcessed?: number;
  recordsImported: number;
  nhData: {
    childrenServed: number;
    familiesServed: number;
    totalExpenditure: number;
    federalExpenditure: number;
    stateExpenditure: number;
    avgMonthlySubsidy: number;
    providersParticipating: number;
  } | null;
  trend?: Array<{
    fiscalYear: number;
    childrenServed: number;
    totalExpenditure: number;
    avgMonthlySubsidy: number;
  }>;
  availableYears: number[];
  error?: string;
}

export const scrapeDataGovCCDFTask = task({
  id: "scrape-datagov-ccdf",
  maxDuration: 120, // 2 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: DataGovCCDFPayload): Promise<DataGovCCDFResult> => {
    logger.info("Data.gov CCDF scraper started", { payload });

    let totalImported = 0;
    let nhData: DataGovCCDFResult['nhData'] = null;
    let error: string | undefined;
    let yearsProcessed: number | undefined;
    let targetYear: number | undefined;

    if (payload.allYears) {
      // Scrape all available years
      logger.info("Scraping all available years");
      const results = await scrapeAllYears();
      yearsProcessed = results.length;
      totalImported = results.reduce((sum, r) => sum + r.recordsImported, 0);

      // Get latest year's data
      const latestResult = results.find(r => r.success && r.nhData);
      if (latestResult?.nhData) {
        nhData = {
          childrenServed: latestResult.nhData.childrenServed,
          familiesServed: latestResult.nhData.familiesServed,
          totalExpenditure: latestResult.nhData.totalExpenditure,
          federalExpenditure: latestResult.nhData.federalExpenditure,
          stateExpenditure: latestResult.nhData.stateExpenditure,
          avgMonthlySubsidy: latestResult.nhData.avgMonthlySubsidy,
          providersParticipating: latestResult.nhData.providersParticipating,
        };
        targetYear = latestResult.fiscalYear;
      }

      if (!results.every(r => r.success)) {
        error = results.find(r => r.error)?.error;
      }
    } else {
      // Scrape single year
      const year = payload.fiscalYear || getAvailableFiscalYears()[0];
      targetYear = year;
      logger.info("Scraping single year", { year });

      const result = await scrapeDataGovCCDF(year);
      totalImported = result.recordsImported;
      error = result.error;

      if (result.nhData) {
        nhData = {
          childrenServed: result.nhData.childrenServed,
          familiesServed: result.nhData.familiesServed,
          totalExpenditure: result.nhData.totalExpenditure,
          federalExpenditure: result.nhData.federalExpenditure,
          stateExpenditure: result.nhData.stateExpenditure,
          avgMonthlySubsidy: result.nhData.avgMonthlySubsidy,
          providersParticipating: result.nhData.providersParticipating,
        };
      }
    }

    // Get trend data if requested
    let trend: DataGovCCDFResult['trend'];
    if (payload.includeTrend) {
      try {
        const trendData = await getNHCCDFTrend();
        trend = trendData.map(t => ({
          fiscalYear: t.fiscalYear,
          childrenServed: t.childrenServed,
          totalExpenditure: t.totalExpenditure,
          avgMonthlySubsidy: t.avgMonthlySubsidy,
        }));
      } catch (e) {
        logger.warn("Could not get trend data", { error: e });
      }
    }

    const success = nhData !== null && !error;

    logger.info("Data.gov CCDF scraper complete", {
      success,
      fiscalYear: targetYear,
      recordsImported: totalImported,
      childrenServed: nhData?.childrenServed,
      totalExpenditure: nhData?.totalExpenditure,
    });

    return {
      success,
      source: 'Data.gov CCDF / ACF Statistics',
      fiscalYear: targetYear,
      yearsProcessed,
      recordsImported: totalImported,
      nhData,
      trend,
      availableYears: getAvailableFiscalYears(),
      error,
    };
  },
});
