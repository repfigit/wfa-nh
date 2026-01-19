/**
 * Fraud Analysis Task
 * Runs fraud detection analysis on payment/expenditure data
 * Runs in Trigger.dev to avoid Vercel timeout limitations
 */

import { task, logger } from "@trigger.dev/sdk";
import {
  detectStructuring,
  detectDuplicates,
  analyzeVendorConcentration,
  saveStructuringFlags,
  saveDuplicateFlags,
  runFullFraudAnalysis,
} from "../analyzer/fraud-detector.js";

export interface FraudAnalysisPayload {
  analysisType?: "full" | "structuring" | "duplicates" | "concentration";
  structuringThreshold?: number;
  topVendors?: number;
}

export interface FraudAnalysisResult {
  success: boolean;
  analysisType: string;
  structuringFlags?: number;
  duplicateFlags?: number;
  topVendors?: {
    vendor: string;
    total_amount: number;
    transaction_count: number;
  }[];
  savedIndicators?: number;
  error?: string;
}

export const runFraudAnalysis = task({
  id: "run-fraud-analysis",
  maxDuration: 300, // 5 minutes
  retry: {
    maxAttempts: 2,
  },
  run: async (payload: FraudAnalysisPayload): Promise<FraudAnalysisResult> => {
    const analysisType = payload.analysisType || "full";
    logger.info("Fraud analysis started", { analysisType, payload });

    try {
      if (analysisType === "full") {
        logger.info("Running full fraud analysis");
        const result = await runFullFraudAnalysis();

        logger.info("Full analysis complete", {
          structuringFlags: result.structuring.length,
          duplicateFlags: result.duplicates.length,
          savedIndicators: result.savedIndicators,
        });

        return {
          success: true,
          analysisType: "full",
          structuringFlags: result.structuring.length,
          duplicateFlags: result.duplicates.length,
          topVendors: result.vendorConcentration.slice(0, 5).map((v) => ({
            vendor: v.vendor,
            total_amount: v.total_amount,
            transaction_count: v.transaction_count,
          })),
          savedIndicators: result.savedIndicators,
        };
      }

      if (analysisType === "structuring") {
        const threshold = payload.structuringThreshold || 10000;
        logger.info("Running structuring detection", { threshold });

        const flags = await detectStructuring(threshold);
        const saved = await saveStructuringFlags(flags);

        logger.info("Structuring analysis complete", {
          flagsFound: flags.length,
          saved,
        });

        return {
          success: true,
          analysisType: "structuring",
          structuringFlags: flags.length,
          savedIndicators: saved,
        };
      }

      if (analysisType === "duplicates") {
        logger.info("Running duplicate detection");

        const flags = await detectDuplicates();
        const saved = await saveDuplicateFlags(flags);

        logger.info("Duplicate analysis complete", {
          flagsFound: flags.length,
          saved,
        });

        return {
          success: true,
          analysisType: "duplicates",
          duplicateFlags: flags.length,
          savedIndicators: saved,
        };
      }

      if (analysisType === "concentration") {
        const topN = payload.topVendors || 10;
        logger.info("Running vendor concentration analysis", { topN });

        const vendors = await analyzeVendorConcentration(topN);

        logger.info("Concentration analysis complete", {
          vendorsAnalyzed: vendors.length,
        });

        return {
          success: true,
          analysisType: "concentration",
          topVendors: vendors.map((v) => ({
            vendor: v.vendor,
            total_amount: v.total_amount,
            transaction_count: v.transaction_count,
          })),
        };
      }

      return {
        success: false,
        analysisType,
        error: `Unknown analysis type: ${analysisType}`,
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      logger.error("Fraud analysis failed", { error: errorMessage });

      return {
        success: false,
        analysisType,
        error: errorMessage,
      };
    }
  },
});
