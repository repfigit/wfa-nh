import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeSAMGov } from "../scrapers/sam-gov.js";

export const scrapeSamGov = task({
  id: "scrape-sam-gov",
  run: async (): Promise<Awaited<ReturnType<typeof scrapeSAMGov>>> => {
    logger.info("Starting SAM.gov scraper");
    const apiKey = process.env.SAM_GOV_API_KEY;
    const result = await scrapeSAMGov(apiKey);
    logger.info(`Scraped ${result.stats.total} SAM.gov awards`);
    return result;
  },
});
