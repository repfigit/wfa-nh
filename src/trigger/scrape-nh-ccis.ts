import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeCCIS } from "../scraper/nh-ccis-scraper.js";
import { bridgeCCIS } from "../bridge/ccis-bridge.js";

export const scrapeNhCcis = task({
  id: "scrape-nh-ccis",
  run: async (payload: any) => {
    logger.info("Starting NH CCIS Pipeline");
    
    const scrapeResult = await scrapeCCIS();
    
    if (!scrapeResult.success) {
      const errorMsg = scrapeResult.error || "Scrape failed for unknown reason";
      logger.error(`CCIS scrape failed: ${errorMsg}`);
      throw new Error(`CCIS scrape failed: ${errorMsg}`);
    }
    
    if (!scrapeResult.documentId) {
      logger.error("Scrape succeeded but did not produce document ID");
      throw new Error("Scrape failed to produce document ID");
    }
    
    const bridgeResult = await bridgeCCIS(scrapeResult.documentId);
    
    return { ...scrapeResult, ...bridgeResult };
  },
});
