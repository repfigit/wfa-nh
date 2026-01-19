/**
 * NH DAS Contracts Scraper Task
 * Searches NH DAS contracts database for childcare-related contracts
 * Runs in Trigger.dev to avoid Vercel timeout limitations
 */

import { task, logger } from "@trigger.dev/sdk";
import { 
  searchContracts, 
  scrapeAllChildcareContracts, 
  saveScrapedContracts,
  CHILDCARE_KEYWORDS 
} from "../scraper/nh-das-scraper.js";

export interface NHDASContractsPayload {
  keyword?: string;
  fullScrape?: boolean;
}

export interface NHDASContractsResult {
  type: "search" | "full";
  success: boolean;
  keyword?: string;
  contractsFound: number;
  contractsSaved: number;
  errors?: string[];
  error?: string;
}

export const scrapeNHDASContracts = task({
  id: "scrape-nh-das-contracts",
  maxDuration: 300, // 5 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: NHDASContractsPayload): Promise<NHDASContractsResult> => {
    logger.info("NH DAS Contracts scraper started", { payload });

    if (payload.fullScrape) {
      logger.info("Running full childcare contracts scrape", { 
        keywords: CHILDCARE_KEYWORDS 
      });

      const result = await scrapeAllChildcareContracts();

      logger.info("Full scrape complete", {
        total: result.total,
        saved: result.saved,
        errorCount: result.errors.length,
      });

      return {
        type: "full",
        success: result.errors.length === 0,
        contractsFound: result.total,
        contractsSaved: result.saved,
        errors: result.errors.length > 0 ? result.errors : undefined,
      };
    } else {
      // Single keyword search
      const keyword = payload.keyword || "daycare";
      
      logger.info("Searching contracts", { keyword });

      const result = await searchContracts(keyword);

      let savedCount = 0;
      if (result.success && result.contracts.length > 0) {
        logger.info("Saving contracts to database", { 
          count: result.contracts.length 
        });
        savedCount = await saveScrapedContracts(result.contracts);
      }

      logger.info("Search complete", {
        keyword,
        success: result.success,
        found: result.contracts.length,
        saved: savedCount,
      });

      return {
        type: "search",
        success: result.success,
        keyword,
        contractsFound: result.contracts.length,
        contractsSaved: savedCount,
        error: result.error,
      };
    }
  },
});
