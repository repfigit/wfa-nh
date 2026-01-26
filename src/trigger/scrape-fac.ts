import { task, logger } from "@trigger.dev/sdk/v3";
import { scrapeFederalAuditClearinghouse } from "../scrapers/federal-audit-clearinghouse.js";

export const scrapeFac = task({
  id: "scrape-fac",
  run: async (): Promise<Awaited<ReturnType<typeof scrapeFederalAuditClearinghouse>>> => {
    logger.info("Starting Federal Audit Clearinghouse scraper");
    const result = await scrapeFederalAuditClearinghouse();
    logger.info(`Scraped ${result.stats.total} audit reports`);
    return result;
  },
});
