/**
 * USAspending.gov Scraper Task
 * Fetches federal CCDF grant data for New Hampshire
 * Runs in Trigger.dev for reliable execution
 */

import { task, logger } from "@trigger.dev/sdk";
import { scrapeUSASpending, getCFDASpendingSummary } from "../scraper/usaspending-scraper.js";

export interface USASpendingPayload {
  fiscalYear?: number;
  includeSummary?: boolean;
}

export interface USASpendingResult {
  success: boolean;
  source: string;
  totalAwards: number;
  totalAmount: number;
  importedRecords: number;
  fiscalYears: number[];
  error?: string;
  awards?: Array<{
    awardId: string;
    recipient: string;
    amount: number;
    cfda: string;
    fiscalYear: number;
    description: string;
  }>;
  cfdaSummary?: Array<{
    cfda: string;
    name: string;
    amount: number;
  }>;
}

export const scrapeUSASpendingTask = task({
  id: "scrape-usaspending",
  maxDuration: 180, // 3 minutes
  retry: {
    maxAttempts: 3,
  },
  run: async (payload: USASpendingPayload): Promise<USASpendingResult> => {
    logger.info("USAspending.gov scraper started", { payload });

    const result = await scrapeUSASpending(payload.fiscalYear);

    // Optionally include CFDA spending summary
    if (payload.includeSummary) {
      const summary = await getCFDASpendingSummary(payload.fiscalYear);
      (result as USASpendingResult).cfdaSummary = summary;
    }

    logger.info("USAspending.gov scraper complete", {
      success: result.success,
      totalAwards: result.totalAwards,
      totalAmount: result.totalAmount,
      importedRecords: result.importedRecords,
      fiscalYears: result.fiscalYears,
    });

    return result as USASpendingResult;
  },
});
