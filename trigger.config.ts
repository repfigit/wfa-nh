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
                  "RUN apt-get update && apt-get install -y wget gnupg ca-certificates",
                  "RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -",
                  "RUN sh -c 'echo \"deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main\" >> /etc/apt/sources.list.d/google.list'",
                  "RUN apt-get update && apt-get install -y google-chrome-stable fonts-ipafont-gothic fonts-wqy-zenhei fonts-thai-tlwg fonts-kacst fonts-freefont-ttf libxss1 --no-install-recommends",
                  "RUN rm -rf /var/lib/apt/lists/*"
                ],
              },
            });
          }
        },
      }
    ],
  },
});
