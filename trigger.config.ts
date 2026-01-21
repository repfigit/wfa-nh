import { defineConfig } from "@trigger.dev/sdk";

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
      {
        name: "puppeteer-browsers",
        onBuildComplete: async (context) => {
          if (context.target === "deploy" || context.target === "dev") {
            context.addLayer({
              id: "puppeteer-chrome",
              image: {
                instructions: [
                  "RUN apt-get update && apt-get install -y \\",
                  "    fonts-liberation \\",
                  "    gconf-service \\",
                  "    libappindicator1 \\",
                  "    libasound2 \\",
                  "    libatk1.0-0 \\",
                  "    libc6 \\",
                  "    libcairo2 \\",
                  "    libcups2 \\",
                  "    libdbus-1-3 \\",
                  "    libexpat1 \\",
                  "    libfontconfig1 \\",
                  "    libgbm1 \\",
                  "    libgcc1 \\",
                  "    libgconf-2-4 \\",
                  "    libgdk-pixbuf2.0-0 \\",
                  "    libglib2.0-0 \\",
                  "    libgtk-3-0 \\",
                  "    libnspr4 \\",
                  "    libnss3 \\",
                  "    libpango-1.0-0 \\",
                  "    libpangocairo-1.0-0 \\",
                  "    libstdc++6 \\",
                  "    libx11-6 \\",
                  "    libx11-xcb1 \\",
                  "    libxcb1 \\",
                  "    libxcomposite1 \\",
                  "    libxcursor1 \\",
                  "    libxdamage1 \\",
                  "    libxext6 \\",
                  "    libxfixes3 \\",
                  "    libxi6 \\",
                  "    libxrandr2 \\",
                  "    libxrender1 \\",
                  "    libxss1 \\",
                  "    libxtst6 \\",
                  "    ca-certificates \\",
                  "    fonts-liberation \\",
                  "    libappindicator1 \\",
                  "    libnss3 \\",
                  "    lsb-release \\",
                  "    xdg-utils \\",
                  "    wget \\",
                  "    && rm -rf /var/lib/apt/lists/*"
                ],
              },
            });
          }
        },
      }
    ],
  },
});
