import { defineConfig } from "@trigger.dev/sdk";
import { playwright } from "@trigger.dev/build/extensions/playwright";

export default defineConfig({
  project: "proj_jgarnbajxtqeftxmdqxa",
  dirs: ["./src/trigger"],
  retries: {
    enabledInDev: false,
    default: {
      maxAttempts: 2,
      minTimeoutInMs: 1000,
      maxTimeoutInMs: 30000,
      factor: 2,
      randomize: true,
    },
  },
  maxDuration: 300, // 5 minutes max per task
  build: {
    // Mark these as external so they get installed via npm in the container
    external: [
      "sql.js",
    ],
    // Extensions for platform-specific modules
    extensions: [
      {
        name: "libsql-native",
        onBuildComplete: async (context) => {
          // Tell Trigger to install native dependencies during container build
          if (context.target === "deploy") {
            context.addLayer({
              id: "libsql-native-deps",
              commands: [
                "npm install @libsql/linux-x64-gnu --no-save",
              ],
            });
          }
        },
      },
      playwright({
        browsers: ["chromium"],
        headless: true,
      }),
    ],
  },
});
