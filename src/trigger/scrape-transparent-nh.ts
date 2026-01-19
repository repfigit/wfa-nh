/**
 * TransparentNH Scraper Task
 * Downloads fiscal year expenditure ZIP files, extracts CSV, filters childcare records
 * Runs in Trigger.dev to avoid Vercel timeout limitations
 * 
 * NOTE: NH.gov has aggressive bot protection that blocks automated downloads.
 * This scraper will attempt to download but may fail. Use USAspending.gov or
 * ACF CCDF scrapers as alternatives for federal childcare funding data.
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
  blocked?: boolean;
  alternativeDataSources?: string[];
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

// Check if error indicates bot blocking
function isBlockedError(error?: string): boolean {
  if (!error) return false;
  return error.includes('403') || 
         error.includes('404') || 
         error.includes('Forbidden') ||
         error.includes('blocking') ||
         error.includes('bot protection');
}

export const scrapeTransparentNH = task({
  id: "scrape-transparent-nh",
  maxDuration: 300, // 5 minutes
  retry: {
    maxAttempts: 1, // Reduced retries since site blocks automated requests
  },
  run: async (payload: TransparentNHPayload): Promise<TransparentNHResult> => {
    logger.info("TransparentNH scraper started", { payload });
    logger.warn("Note: NH.gov may block automated requests. Consider using USAspending.gov or ACF CCDF scrapers instead.");

    const alternativeDataSources = [
      "USAspending.gov - Federal CCDF awards (scrape-usaspending task)",
      "ACF CCDF Statistics - Official HHS data (scrape-acf-ccdf task)",
      "Manual CSV upload - Download from TransparentNH manually and upload via UI"
    ];

    if (payload.recentYears) {
      logger.info("Scraping recent fiscal years (last 3)");
      
      const results = await scrapeRecentYears();
      
      const summary = {
        yearsScraped: results.length,
        totalRecords: results.reduce((sum, r) => sum + r.totalRecords, 0),
        childcareRecords: results.reduce((sum, r) => sum + r.childcareRecords, 0),
        importedRecords: results.reduce((sum, r) => sum + r.importedRecords, 0),
      };

      const allBlocked = results.every(r => isBlockedError(r.error));
      const anySuccess = results.some(r => r.success);

      if (allBlocked) {
        logger.error("All requests were blocked by NH.gov bot protection");
      }

      logger.info("Recent years scrape complete", { ...summary, allBlocked });

      return {
        type: "recent",
        success: anySuccess,
        blocked: allBlocked,
        alternativeDataSources: allBlocked ? alternativeDataSources : undefined,
        totalRecords: summary.totalRecords,
        childcareRecords: summary.childcareRecords,
        importedRecords: summary.importedRecords,
        totalAmount: results.reduce((sum, r) => sum + r.totalAmount, 0),
        error: allBlocked ? "NH.gov is blocking automated requests. Please use alternative data sources." : undefined,
        results,
        summary,
      };
    } else {
      // Single fiscal year scrape
      const availableYears = getAvailableFiscalYears();
      const fiscalYear = payload.fiscalYear || availableYears[0] || new Date().getFullYear();

      logger.info("Scraping single fiscal year", { fiscalYear, availableYears });

      const result = await scrapeFiscalYear(fiscalYear);
      const blocked = isBlockedError(result.error);

      if (blocked) {
        logger.error("Request was blocked by NH.gov bot protection", { fiscalYear, error: result.error });
      }

      logger.info("Single year scrape complete", {
        fiscalYear,
        success: result.success,
        blocked,
        totalRecords: result.totalRecords,
        childcareRecords: result.childcareRecords,
        importedRecords: result.importedRecords,
        totalAmount: result.totalAmount,
      });

      return {
        type: "single",
        fiscalYear: result.fiscalYear,
        success: result.success,
        blocked,
        alternativeDataSources: blocked ? alternativeDataSources : undefined,
        totalRecords: result.totalRecords,
        childcareRecords: result.childcareRecords,
        importedRecords: result.importedRecords,
        totalAmount: result.totalAmount,
        error: blocked 
          ? `NH.gov blocked the request (${result.error}). Please use alternative data sources.`
          : result.error,
      };
    }
  },
});
