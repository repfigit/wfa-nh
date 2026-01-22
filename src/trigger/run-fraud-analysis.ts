import { task, logger } from "@trigger.dev/sdk/v3";
import { runFullFraudAnalysis } from "../analyzer/fraud-detector.js";

export const runFraudAnalysis = task({
  id: "run-fraud-analysis",
  run: async (payload: any) => {
    logger.info("Starting Fraud Analysis");
    const result = await runFullFraudAnalysis();
    logger.info("Fraud analysis complete", result);
    return result;
  },
});
