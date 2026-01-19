/**
 * ProPublica 990 Nonprofit Scraper Task
 * Fetches IRS 990 filing data for NH childcare nonprofits
 * Runs in Trigger.dev for reliable execution
 */

import { task, logger } from "@trigger.dev/sdk";
import { scrapeProPublica990, getNH990Summary } from "../scraper/propublica-990-scraper.js";

export interface ProPublica990Payload {
  maxOrgs?: number;
  recentYearsOnly?: boolean;
  includeSummary?: boolean;
}

export interface ProPublica990Result {
  success: boolean;
  source: string;
  organizationsFound: number;
  filingsProcessed: number;
  filingsImported: number;
  totalRevenue: number;
  organizations: Array<{
    ein: string;
    name: string;
    city: string;
    nteeCode: string;
    latestRevenue: number;
    latestExpenses: number;
    latestAssets: number;
  }>;
  summary?: {
    totalOrgs: number;
    totalRevenue: number;
    totalAssets: number;
    avgRevenue: number;
  };
  error?: string;
}

export const scrapeProPublica990Task = task({
  id: "scrape-propublica-990",
  maxDuration: 300, // 5 minutes - API calls can be slow
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: ProPublica990Payload): Promise<ProPublica990Result> => {
    logger.info("ProPublica 990 scraper started", { payload });

    const result = await scrapeProPublica990({
      maxOrgs: payload.maxOrgs || 50,
      recentYearsOnly: payload.recentYearsOnly !== false,
    });

    // Include summary if requested
    let summary: ProPublica990Result['summary'];
    if (payload.includeSummary) {
      const summaryData = await getNH990Summary();
      summary = {
        totalOrgs: summaryData.totalOrgs,
        totalRevenue: summaryData.totalRevenue,
        totalAssets: summaryData.totalAssets,
        avgRevenue: summaryData.avgRevenue,
      };
    }

    logger.info("ProPublica 990 scraper complete", {
      success: result.success,
      organizationsFound: result.organizationsFound,
      filingsProcessed: result.filingsProcessed,
      filingsImported: result.filingsImported,
      totalRevenue: result.totalRevenue,
    });

    return {
      ...result,
      summary,
    };
  },
});
