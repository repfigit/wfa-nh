/**
 * Census SAIPE Demographics Scraper Task
 * Fetches county-level poverty and income data for New Hampshire
 * Runs in Trigger.dev for reliable execution
 */

import { task, logger } from "@trigger.dev/sdk";
import {
  scrapeSAIPE,
  scrapeSAIPEMultipleYears,
  getHighPovertyCounties,
  getSAIPESummary
} from "../scraper/census-saipe-scraper.js";

export interface CensusSAIPEPayload {
  year?: number;
  startYear?: number;
  endYear?: number;
  multipleYears?: boolean;
  includeSummary?: boolean;
}

export interface CensusSAIPEResult {
  success: boolean;
  source: string;
  year?: number;
  yearsProcessed?: number;
  countiesProcessed: number;
  recordsImported: number;
  stateStats: {
    totalPoverty: number;
    avgPovertyRate: number;
    totalChildPoverty: number;
    avgChildPovertyRate: number;
    avgMedianIncome: number;
  };
  highPovertyCounties?: Array<{
    countyName: string;
    childPovertyRate: number;
    medianIncome: number;
  }>;
  summary?: {
    yearsAvailable: number[];
    latestYear: number;
    statewidePoverty: number;
    statewideChildPoverty: number;
    highestPovertyCounty: string;
    lowestIncomeCounty: string;
  };
  error?: string;
}

export const scrapeCensusSAIPETask = task({
  id: "scrape-census-saipe",
  maxDuration: 180, // 3 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: CensusSAIPEPayload): Promise<CensusSAIPEResult> => {
    logger.info("Census SAIPE scraper started", { payload });

    let result: CensusSAIPEResult;

    if (payload.multipleYears) {
      // Scrape multiple years
      const startYear = payload.startYear || 2019;
      const endYear = payload.endYear || new Date().getFullYear() - 1;

      logger.info("Scraping multiple years", { startYear, endYear });

      const results = await scrapeSAIPEMultipleYears(startYear, endYear);
      const allSuccess = results.every(r => r.success);

      // Aggregate stats from latest year
      const latestResult = results.find(r => r.success) || results[0];

      result = {
        success: allSuccess,
        source: 'Census Bureau SAIPE',
        yearsProcessed: results.length,
        countiesProcessed: results.reduce((sum, r) => sum + r.countiesProcessed, 0),
        recordsImported: results.reduce((sum, r) => sum + r.recordsImported, 0),
        stateStats: latestResult?.stateStats || {
          totalPoverty: 0,
          avgPovertyRate: 0,
          totalChildPoverty: 0,
          avgChildPovertyRate: 0,
          avgMedianIncome: 0,
        },
        error: allSuccess ? undefined : results.find(r => r.error)?.error,
      };
    } else {
      // Scrape single year
      const year = payload.year || new Date().getFullYear() - 1;
      logger.info("Scraping single year", { year });

      const singleResult = await scrapeSAIPE(year);

      result = {
        success: singleResult.success,
        source: 'Census Bureau SAIPE',
        year: singleResult.year,
        countiesProcessed: singleResult.countiesProcessed,
        recordsImported: singleResult.recordsImported,
        stateStats: singleResult.stateStats,
        error: singleResult.error,
      };
    }

    // Get high poverty counties if successful
    if (result.success) {
      try {
        const highPoverty = await getHighPovertyCounties(payload.year, 8.0);
        result.highPovertyCounties = highPoverty.slice(0, 5).map(c => ({
          countyName: c.countyName,
          childPovertyRate: c.childPovertyRate,
          medianIncome: c.medianHouseholdIncome,
        }));
      } catch (e) {
        logger.warn("Could not get high poverty counties", { error: e });
      }
    }

    // Include summary if requested
    if (payload.includeSummary) {
      try {
        result.summary = await getSAIPESummary();
      } catch (e) {
        logger.warn("Could not get summary", { error: e });
      }
    }

    logger.info("Census SAIPE scraper complete", {
      success: result.success,
      countiesProcessed: result.countiesProcessed,
      recordsImported: result.recordsImported,
      avgChildPovertyRate: result.stateStats.avgChildPovertyRate,
    });

    return result;
  },
});
