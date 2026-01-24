import { task, logger, metadata } from "@trigger.dev/sdk/v3";
import { scrapeCCIS } from "../scraper/nh-ccis-scraper.js";
import { bridgeCCIS } from "../bridge/ccis-bridge.js";

export const scrapeNhCcis = task({
  id: "scrape-nh-ccis",
  run: async (payload: any) => {
    logger.info("Starting NH CCIS Pipeline");

    // Stage 1: Scraping
    metadata.set("stage", "scraping");
    metadata.set("status", "Connecting to CCIS portal...");
    metadata.set("progress", 10);

    const scrapeResult = await scrapeCCIS();

    if (!scrapeResult.success) {
      const errorMsg = scrapeResult.error || "Scrape failed for unknown reason";
      metadata.set("stage", "error");
      metadata.set("status", `Failed: ${errorMsg}`);
      logger.error(`CCIS scrape failed: ${errorMsg}`);
      throw new Error(`CCIS scrape failed: ${errorMsg}`);
    }

    if (!scrapeResult.documentId) {
      metadata.set("stage", "error");
      metadata.set("status", "Scrape succeeded but no document ID");
      logger.error("Scrape succeeded but did not produce document ID");
      throw new Error("Scrape failed to produce document ID");
    }

    // Stage 2: Bridging
    metadata.set("stage", "bridging");
    metadata.set("status", `Scraped ${scrapeResult.totalFound} providers. Updating database...`);
    metadata.set("progress", 60);
    metadata.set("providersFound", scrapeResult.totalFound);

    const bridgeResult = await bridgeCCIS(scrapeResult.documentId);

    // Stage 3: Complete
    metadata.set("stage", "complete");
    metadata.set("status", `Done! ${bridgeResult.updated || scrapeResult.totalFound} providers updated`);
    metadata.set("progress", 100);

    return { ...scrapeResult, ...bridgeResult };
  },
});
