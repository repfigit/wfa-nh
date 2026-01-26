import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeDASBids } from "../scrapers/das-bids.js";

export const scrapeDasBids = task({
  id: "scrape-das-bids",
  run: async (): Promise<Awaited<ReturnType<typeof scrapeDASBids>>> => {
    logger.info("Starting DAS Bids scraper");
    const result = await scrapeDASBids();
    logger.info(`Scraped ${result.stats.total} DAS bids`);
    return result;
  },
});
