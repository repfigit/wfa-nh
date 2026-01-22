import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeFiscalYear } from "../scraper/transparent-nh-scraper.js";
import { bridgeTransparentNH } from "../bridge/transparent-nh-bridge.js";

export const scrapeTransparentNH = task({
  id: "scrape-transparent-nh",
  run: async (payload: { year?: number }) => {
    const year = payload.year || 2026;
    logger.info(`Starting Transparent NH Pipeline for FY${year}`);
    
    // 1. EXTRACT & LOAD
    const scrapeResult = await scrapeFiscalYear(year);
    logger.info(`Extraction complete into table: ${scrapeResult.tableName}`);

    // 2. TRANSFORM & BRIDGE
    const bridgeResult = await bridgeTransparentNH(year);
    logger.info(`Bridging complete`, bridgeResult);
    
    return {
      year,
      ...scrapeResult,
      ...bridgeResult
    };
  },
});
