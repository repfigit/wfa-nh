# How to Test the scrape-nh-ccis Task Using CLI

## Method 1: Using Trigger.dev Dev Server (Recommended)

### Step 1: Start the Dev Server

In one terminal, start the dev server:

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
- Show real-time logs

### Step 2: Trigger the Task

In **another terminal**, trigger the task using one of these methods:

#### Option A: Using the Trigger Script (Easiest)

```bash
npx tsx trigger-ccis.ts
```

This will:
- Trigger the `scrape-nh-ccis` task
- Show the run ID
- Display status

#### Option B: Using the SDK Directly

Create a test script:

```typescript
// test-ccis.ts
import { tasks } from '@trigger.dev/sdk/v3';

async function test() {
  const handle = await tasks.trigger('scrape-nh-ccis', {});
  console.log('Run ID:', handle.id);
  console.log('Status:', handle.status);
}

test();
```

Run it:
```bash
npx tsx test-ccis.ts
```

#### Option C: Using the API Endpoint

If your Express server is running:

```bash
curl -X POST http://localhost:3000/api/trigger/ccis \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ADMIN_API_KEY" \
  -d '{}'
```

### Step 3: Monitor the Run

Watch the dev server terminal for real-time logs, or check run status:

```bash
# List recent runs
npx trigger.dev@latest runs list

# View specific run details
npx trigger.dev@latest runs show <run-id>
```

## Method 2: Direct Execution (Without Trigger.dev)

For quick testing without the dev server:

```bash
npx tsx run-ccis-direct.ts
```

This runs the scraper directly, bypassing Trigger.dev infrastructure.

## Method 3: Using Trigger.dev Dashboard

1. **Deploy your tasks** (if not already deployed):
   ```bash
   npx trigger.dev@latest deploy
   ```

2. **Go to the dashboard**:
   - Visit https://cloud.trigger.dev
   - Navigate to your project
   - Go to "Tasks" → "scrape-nh-ccis"

3. **Trigger the task**:
   - Click the "Trigger" button
   - Monitor the run in real-time
   - View logs and results

## Monitoring and Debugging

### View Run Logs

```bash
# List all runs
npx trigger.dev@latest runs list

# Filter by task
npx trigger.dev@latest runs list --task scrape-nh-ccis

# View specific run
npx trigger.dev@latest runs show <run-id>

# Follow logs in real-time (if supported)
npx trigger.dev@latest runs logs <run-id>
```

### Check Task Status

```bash
# See what tasks are available
npx trigger.dev@latest whoami
```

### View Dev Server Logs

When running `npx trigger.dev@latest dev`, you'll see:
- Task build progress
- Task execution logs
- Errors and warnings
- Real-time output from your task

## Troubleshooting

### "Task not found" error

Make sure:
1. The dev server is running
2. The task ID matches: `"scrape-nh-ccis"`
3. The task is exported from `src/trigger/scrape-nh-ccis.ts`
4. You've run `npm run build` to compile TypeScript

### "Connection refused" or "Cannot connect"

- Ensure the dev server is running
- Check that port 3000 (or your configured port) is available
- Verify your `.env` file has correct configuration

### Task runs but fails

Check the dev server logs for:
- Error messages
- Stack traces
- Database connection issues
- Missing environment variables

### Version mismatch

If you see version errors:
```bash
npm install @trigger.dev/sdk@latest
```

## Quick Test Command

Here's a one-liner to test the task (requires dev server running):

```bash
npx tsx trigger-ccis.ts
```

Or test directly without Trigger.dev:

```bash
npx tsx run-ccis-direct.ts
```

## Example Output

When running successfully, you should see:

```
Triggering CCIS scraper task...
✅ Task triggered successfully!
Run ID: run_abc123xyz
Status: QUEUED

Task is running. Check the Trigger.dev dashboard for progress.
```

Then in the dev server terminal, you'll see:

```
[scrape-nh-ccis] Starting NH CCIS Pipeline
[scrape-nh-ccis] Starting NH CCIS scraper...
[scrape-nh-ccis] Target URL: https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch
...
```
