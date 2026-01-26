import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeCharitableTrusts } from "../scrapers/charitable-trusts.js";

export const scrapeCharitableTrustsTask = task({
  id: "scrape-charitable-trusts",
  run: async () => {
    logger.info("Starting Charitable Trusts / Form 990 scraper");
    const result = await scrapeCharitableTrusts();
    logger.info(`Scraped ${result.stats.total} nonprofit profiles`);
    return result.stats;
  },
});
