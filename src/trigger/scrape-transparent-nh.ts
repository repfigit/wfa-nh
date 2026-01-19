/**
 * TransparentNH Scraper Task
 * Downloads fiscal year expenditure ZIP files, extracts CSV, filters childcare records
 * Runs in Trigger.dev to avoid Vercel timeout limitations
 */

import { task, logger } from "@trigger.dev/sdk";
import { 
  scrapeFiscalYear, 
  scrapeRecentYears, 
  getAvailableFiscalYears 
} from "../scraper/transparent-nh-scraper.js";

export interface TransparentNHPayload {
  fiscalYear?: number;
  recentYears?: boolean;
}

export interface TransparentNHResult {
  type: "single" | "recent";
  fiscalYear?: number;
  success: boolean;
  totalRecords: number;
  childcareRecords: number;
  importedRecords: number;
  totalAmount: number;
  error?: string;
  results?: Array<{
    success: boolean;
    fiscalYear: number;
    totalRecords: number;
    childcareRecords: number;
    importedRecords: number;
    totalAmount: number;
    error?: string;
  }>;
  summary?: {
    yearsScraped: number;
    totalRecords: number;
    childcareRecords: number;
    importedRecords: number;
  };
}

export const scrapeTransparentNH = task({
  id: "scrape-transparent-nh",
  maxDuration: 300, // 5 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: TransparentNHPayload): Promise<TransparentNHResult> => {
    logger.info("TransparentNH scraper started", { payload });

    if (payload.recentYears) {
      logger.info("Scraping recent fiscal years (last 3)");
      
      const results = await scrapeRecentYears();
      
      const summary = {
        yearsScraped: results.length,
        totalRecords: results.reduce((sum, r) => sum + r.totalRecords, 0),
        childcareRecords: results.reduce((sum, r) => sum + r.childcareRecords, 0),
        importedRecords: results.reduce((sum, r) => sum + r.importedRecords, 0),
      };

      logger.info("Recent years scrape complete", summary);

      return {
        type: "recent",
        success: results.every(r => r.success),
        totalRecords: summary.totalRecords,
        childcareRecords: summary.childcareRecords,
        importedRecords: summary.importedRecords,
        totalAmount: results.reduce((sum, r) => sum + r.totalAmount, 0),
        results,
        summary,
      };
    } else {
      // Single fiscal year scrape
      const availableYears = getAvailableFiscalYears();
      const fiscalYear = payload.fiscalYear || availableYears[0] || new Date().getFullYear();

      logger.info("Scraping single fiscal year", { fiscalYear, availableYears });

      const result = await scrapeFiscalYear(fiscalYear);

      logger.info("Single year scrape complete", {
        fiscalYear,
        success: result.success,
        totalRecords: result.totalRecords,
        childcareRecords: result.childcareRecords,
        importedRecords: result.importedRecords,
        totalAmount: result.totalAmount,
      });

      return {
        type: "single",
        fiscalYear: result.fiscalYear,
        success: result.success,
        totalRecords: result.totalRecords,
        childcareRecords: result.childcareRecords,
        importedRecords: result.importedRecords,
        totalAmount: result.totalAmount,
        error: result.error,
      };
    }
  },
});
