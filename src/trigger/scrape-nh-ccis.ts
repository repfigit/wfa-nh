/**
 * NH CCIS (Child Care Information System) Scraper Task
 * Downloads provider directory from NH Child Care Search portal
 * Runs in Trigger.dev with Puppeteer for browser automation
 *
 * Data source: https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch
 */

import { task, logger } from "@trigger.dev/sdk";
import { scrapeCCIS, importFromCSV } from "../scraper/nh-ccis-scraper.js";

export interface NHCCISPayload {
  /** Path to a pre-downloaded CSV file to import instead of scraping */
  csvPath?: string;
}

export interface NHCCISResult {
  success: boolean;
  totalFound: number;
  imported: number;
  updated: number;
  downloadPath?: string;
  error?: string;
}

export const scrapeNHCCIS = task({
  id: "scrape-nh-ccis",
  maxDuration: 600, // 10 minutes - Puppeteer scraping can be slow
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: NHCCISPayload): Promise<NHCCISResult> => {
    logger.info("NH CCIS scraper started", { payload });

    try {
      let result;

      if (payload.csvPath) {
        // Import from pre-downloaded CSV
        logger.info("Importing from CSV file", { csvPath: payload.csvPath });
        result = await importFromCSV(payload.csvPath);
      } else {
        // Scrape using Puppeteer
        logger.info("Scraping NH CCIS portal with Puppeteer");
        result = await scrapeCCIS();
      }

      logger.info("NH CCIS scrape complete", {
        success: result.success,
        totalFound: result.totalFound,
        imported: result.imported,
        updated: result.updated,
        error: result.error,
      });

      if (result.success) {
        return {
          success: result.success,
          totalFound: result.totalFound,
          imported: result.imported,
          updated: result.updated,
          downloadPath: result.downloadPath,
          error: result.error,
        };
      } else {
        // If scraper returned success: false, throw an error to mark the run as failed in Trigger.dev
        throw new Error(result.error || "Scraper failed without specific error message");
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      logger.error("NH CCIS scraper failed", { error: errorMessage });
      
      // Re-throw to ensure Trigger.dev marks the run as failed
      throw error;
    }
  },
});
