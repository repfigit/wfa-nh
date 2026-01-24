# How to Update Trigger.dev CLI

## Current Setup

- **SDK Version**: 4.3.3 (in `package.json`)
- **CLI**: Using `npx trigger.dev@latest` (not installed locally)

## Update Methods

### Method 1: Update SDK (Recommended)

The SDK and CLI should match versions. Update the SDK:

```bash
npm install @trigger.dev/sdk@latest
```

This will update `package.json` and `package-lock.json` to the latest version.

### Method 2: Install/Update CLI as Dev Dependency

If you want to install the CLI locally (optional):

```bash
npm install -D @trigger.dev/cli@latest
```

Then you can use:
```bash
npx trigger.dev dev
```

Instead of:
```bash
npx trigger.dev@latest dev
```

### Method 3: Use npx (Always Latest)

If you use `npx trigger.dev@latest`, you'll always get the latest version without needing to update anything:

```bash
npx trigger.dev@latest dev
npx trigger.dev@latest deploy
npx trigger.dev@latest runs list
```

This is the recommended approach - no local installation needed!

## Version Matching

⚠️ **Important**: The CLI and SDK versions should match to avoid errors.

When you see version mismatch errors:
```
ERROR: Version mismatch detected
CLI version: 4.3.3
Current package versions that don't match:
  - @trigger.dev/sdk@4.3.2
```

**Fix it by updating the SDK:**
```bash
npm install @trigger.dev/sdk@4.3.3
```

Or update both to latest:
```bash
npm install @trigger.dev/sdk@latest
```

## Check Current Versions

```bash
# Check SDK version in package.json
npm list @trigger.dev/sdk

# Check latest available SDK version
npm view @trigger.dev/sdk version

# Check CLI version (when using npx)
npx trigger.dev@latest --version
```

## Recommended Workflow

1. **Use npx for CLI** (always latest, no updates needed):
   ```bash
   npx trigger.dev@latest dev
   ```

2. **Keep SDK updated**:
   ```bash
   npm install @trigger.dev/sdk@latest
   ```

3. **If versions mismatch**, update SDK to match CLI:
   ```bash
   # Check CLI version
   npx trigger.dev@latest --version
   
   # Update SDK to match (or use @latest)
   npm install @trigger.dev/sdk@<version>
   ```

## Quick Update Command

To update everything to latest:

```bash
npm install @trigger.dev/sdk@latest
```

That's it! Since you're using `npx trigger.dev@latest`, the CLI is always up-to-date.
