/**
 * NH Licensing Scraper Task
 * Scrapes NH DHHS child care licensing data
 * Runs in Trigger.dev to avoid Vercel timeout limitations
 */

import { task, logger } from "@trigger.dev/sdk";
import { scrapeLicensing } from "../scraper/nh-licensing-scraper.js";

export interface NHLicensingPayload {
  // Currently no options, but placeholder for future enhancements
  forceRefresh?: boolean;
}

export interface NHLicensingResult {
  success: boolean;
  providersFound: number;
  providersImported: number;
  error?: string;
}

export const scrapeNHLicensing = task({
  id: "scrape-nh-licensing",
  maxDuration: 300, // 5 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: NHLicensingPayload): Promise<NHLicensingResult> => {
    logger.info("NH Licensing scraper started", { payload });

    try {
      const result = await scrapeLicensing();

      logger.info("Licensing scrape complete", {
        success: result.success,
        totalFound: result.totalFound,
        imported: result.imported,
        error: result.error,
      });

      return {
        success: result.success,
        providersFound: result.totalFound,
        providersImported: result.imported,
        error: result.error,
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      logger.error("NH Licensing scraper failed", { error: errorMessage });

      return {
        success: false,
        providersFound: 0,
        providersImported: 0,
        error: errorMessage,
      };
    }
  },
});
