# Running CCIS Scraper via Trigger.dev

## Prerequisites

1. **Install Trigger.dev CLI** (if not already installed):
   ```bash
   npm install -D @trigger.dev/cli@latest
   ```

2. **Set up environment variables** in your `.env` file:
   ```bash
   TRIGGER_SECRET_KEY=your_secret_key_here
   TURSO_DATABASE_URL=your_database_url
   TURSO_AUTH_TOKEN=your_auth_token
   ```

   Get your `TRIGGER_SECRET_KEY` from:
   - Trigger.dev dashboard: https://cloud.trigger.dev
   - Go to your project → Settings → API Keys

## Method 1: Using Trigger.dev Dev Server (Recommended for Local Testing)

### Step 1: Start the Dev Server

In one terminal, run:
```bash
npx trigger.dev@latest dev
```

Wait for it to show:
```
✓ Ready! Triggering is enabled!
```

The dev server will:
- Build your tasks
- Start a local worker
- Enable task triggering

### Step 2: Trigger the Task

In another terminal (or use the Trigger.dev dashboard), trigger the task:

**Option A: Using the script**
```bash
npx tsx trigger-ccis.ts
```

**Option B: Using Trigger.dev CLI**
```bash
npx trigger.dev@latest run scrape-nh-ccis
```

**Option C: Using the API endpoint** (if your server is running)
```bash
curl -X POST http://localhost:3000/api/trigger/ccis \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ADMIN_API_KEY" \
  -d '{}'
```

## Method 2: Deploy and Run in Production

### Step 1: Deploy to Trigger.dev

```bash
npx trigger.dev@latest deploy
```

### Step 2: Trigger via Dashboard

1. Go to https://cloud.trigger.dev
2. Navigate to your project
3. Go to "Tasks" → "scrape-nh-ccis"
4. Click "Trigger" button
5. Monitor the run in real-time

### Step 3: Trigger via API

```bash
npx tsx trigger-ccis.ts
```

Or use the Trigger.dev API directly.

## Method 3: Direct Execution (Without Trigger.dev)

For quick testing without Trigger.dev:

```bash
npx tsx run-ccis-direct.ts
```

This runs the scraper directly without going through Trigger.dev's infrastructure.

## Troubleshooting

### "Version mismatch" error
Make sure your `@trigger.dev/sdk` version matches the CLI version:
```bash
npm install @trigger.dev/sdk@latest
```

### "TRIGGER_SECRET_KEY not set"
Add your secret key to `.env` file or export it:
```bash
export TRIGGER_SECRET_KEY=your_key_here
```

### Dev server won't start
- Check that port 3000 (or configured port) is available
- Ensure all dependencies are installed: `npm install`
- Check that `trigger.config.ts` is properly configured

### Task not found
- Make sure the task ID in `src/trigger/scrape-nh-ccis.ts` matches: `"scrape-nh-ccis"`
- Verify the task is exported correctly
- Rebuild: `npm run build`

## Monitoring Runs

View run status:
```bash
npx trigger.dev@latest runs list
```

View specific run:
```bash
npx trigger.dev@latest runs show <run-id>
```

Or check the Trigger.dev dashboard for real-time logs and status.
