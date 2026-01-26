import { task, logger } from "@trigger.dev/sdk/v3";
import transparentNhMonthlyScraper from "../scraper/transparent-nh-monthly-scraper.js";
import transparentNhBridge from "../bridge/transparent-nh-bridge.js";

export const scrapeTransparentNH = task({
  id: "scrape-transparent-nh",
  run: async (payload: { year?: number; bridge?: boolean }) => {
    const year = payload.year || 2026;
    logger.info(`Starting Transparent NH Pipeline for FY${year}`);
    
    // 1. EXTRACT & LOAD
    const scrapeResult = await transparentNhMonthlyScraper.scrapeFiscalYear(year);
    logger.info(`Extraction complete: ${scrapeResult.monthsScraped} months, ${scrapeResult.totalRows} rows`);

    // 2. TRANSFORM & BRIDGE (optional)
    let bridgeResult = null;
    if (payload.bridge !== false) {
      bridgeResult = await transparentNhBridge.bridgeFiscalYear(year);
      logger.info(`Bridging complete`, bridgeResult);
    }
    
    return {
      year,
      monthsScraped: scrapeResult.monthsScraped,
      totalRows: scrapeResult.totalRows,
      errors: scrapeResult.errors,
      bridge: bridgeResult
    };
  },
});
