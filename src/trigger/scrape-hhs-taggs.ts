import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeHHSTAGGS } from "../scrapers/hhs-taggs.js";

export const scrapeHhsTaggs = task({
  id: "scrape-hhs-taggs",
  run: async (): Promise<Awaited<ReturnType<typeof scrapeHHSTAGGS>>> => {
    logger.info("Starting HHS TAGGS scraper");
    const result = await scrapeHHSTAGGS();
    logger.info(`Scraped ${result.stats.total} HHS awards`);
    return result;
  },
});
