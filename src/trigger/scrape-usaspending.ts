import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeUSASpending } from "../scraper/usaspending-scraper.js";
import { bridgeUSASpending } from "../bridge/usaspending-bridge.js";

export const scrapeUsaspending = task({
  id: "scrape-usaspending",
  run: async (payload: { fiscalYear?: number }) => {
    logger.info("Starting USAspending Pipeline");
    
    const scrapeResult = await scrapeUSASpending(payload.fiscalYear);
    const bridgeResult = await bridgeUSASpending(scrapeResult.documentId!);
    
    return { ...scrapeResult, ...bridgeResult };
  },
});
