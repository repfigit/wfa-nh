import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import { dbHelpers, initializeDb } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';
import { importCSV, getImportStats } from '../importer/csv-importer.js';
import { 
  searchContracts, 
  scrapeAllChildcareContracts, 
  saveScrapedContracts,
  CHILDCARE_KEYWORDS 
} from '../scraper/nh-das-scraper.js';
import {
  detectStructuring,
  detectDuplicates,
  analyzeVendorConcentration,
  runFullFraudAnalysis,
} from '../analyzer/fraud-detector.js';
import {
  scrapeFiscalYear,
  scrapeRecentYears,
  getAvailableFiscalYears,
  scrapeCurrentFiscalYear,
  scrapeAllHistoricalYears,
} from '../scraper/transparent-nh-scraper.js';
import { scrapeCCIS } from '../scraper/nh-ccis-scraper.js';
import { tasks, runs, configure } from '@trigger.dev/sdk/v3';
import { scrapeUSASpending, getNHStateOverview } from '../scraper/usaspending-scraper.js';

// Configure Trigger.dev
if (process.env.TRIGGER_SECRET_KEY) {
  configure({ secretKey: process.env.TRIGGER_SECRET_KEY });
}

import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const app = express();

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb', type: 'text/csv' }));

// Serve static files from the public directory
app.use(express.static(path.join(__dirname, '../../public')));

// Helper
const asyncHandler = (fn: (req: Request, res: Response, next: NextFunction) => Promise<any>) => 
  (req: Request, res: Response, next: NextFunction) => Promise.resolve(fn(req, res, next)).catch(next);

// Auth Middleware (Hardened)
const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  const apiKey = req.headers['x-api-key'] as string;
  const expectedKey = process.env.ADMIN_API_KEY;
  
  if (!expectedKey) {
    console.error('CRITICAL: ADMIN_API_KEY is not set. Access denied to protected endpoint.');
    return res.status(500).json({ 
      error: 'Internal Server Error', 
      message: 'Server security configuration is missing. Please contact administrator.' 
    });
  }
  
  if (!apiKey || apiKey !== expectedKey) {
    return res.status(401).json({ 
      error: 'Unauthorized', 
      message: 'Valid x-api-key header is required' 
    });
  }
  
  next();
};

// Database Initialization Helper
let initialized = false;
async function ensureInitialized() {
  if (!initialized) {
    await initializeDb();
    initialized = true;
  }
}

// --- Public Routes ---

app.get('/api/dashboard', asyncHandler(async (req, res) => {
  await ensureInitialized();
  res.json(await dbHelpers.getDashboardStats());
}));

app.get('/api/providers', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const q = req.query.q;
  if (typeof q === 'string') {
    return res.json(await dbHelpers.searchProviders(q));
  }
  const immigrantOwnedOnly = req.query.immigrant_owned === 'true';
  res.json(await dbHelpers.getAllProviders(immigrantOwnedOnly));
}));

app.get('/api/providers/:id', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const idStr = req.params.id as string;
  const id = parseInt(idStr, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid ID' });
  const p = await dbHelpers.getProviderWithPayments(id);
  p ? res.json(p) : res.status(404).json({ error: 'Provider not found' });
}));

app.get('/api/payments', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const providerId = typeof req.query.provider_id === 'string' ? parseInt(req.query.provider_id, 10) : undefined;
  const fiscalYear = typeof req.query.fiscal_year === 'string' ? parseInt(req.query.fiscal_year, 10) : undefined;
  const limit = typeof req.query.limit === 'string' ? parseInt(req.query.limit, 10) : 100;
  
  res.json(await dbHelpers.getAllPayments({
    provider_id: providerId,
    fiscal_year: fiscalYear,
    limit: limit,
  }));
}));

app.get('/api/fraud-indicators', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const status = typeof req.query.status === 'string' ? req.query.status : undefined;
  const severity = typeof req.query.severity === 'string' ? req.query.severity : undefined;
  res.json(await dbHelpers.getFraudIndicators({ status, severity }));
}));

app.get('/api/data-sources', asyncHandler(async (req, res) => {
  await ensureInitialized();
  res.json(await dbHelpers.getDataSources());
}));

app.get('/api/federal/summary', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const summary = await query('SELECT fiscal_year, COUNT(*) as award_count, SUM(amount) as total_amount FROM expenditures WHERE source_url LIKE "USAspending:%" GROUP BY fiscal_year ORDER BY fiscal_year DESC');
  res.json({ 
    totalFederalAmount: summary.reduce((s: any, r: any) => s + (r.total_amount || 0), 0), 
    totalAwards: summary.reduce((s: any, r: any) => s + (r.award_count || 0), 0), 
    byFiscalYear: summary 
  });
}));

app.get('/api/federal/awards', asyncHandler(async (req, res) => {
  await ensureInitialized();
  if (req.query.refresh === 'true') {
    // Refresh requires auth
    const expectedKey = process.env.ADMIN_API_KEY;
    const apiKey = req.headers['x-api-key'];
    if (!expectedKey || apiKey !== expectedKey) {
      return res.status(401).json({ error: 'Unauthorized', message: 'Valid API key required for refresh' });
    }
    const result = await scrapeUSASpending();
    return res.json(result);
  }
  const awards = await query('SELECT * FROM expenditures WHERE source_url LIKE "USAspending:%" LIMIT 50');
  res.json({ awards });
}));

// --- Protected Routes ---

app.patch('/api/fraud-indicators/:id', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const { status, notes } = req.body;
  const idStr = req.params.id as string;
  const id = parseInt(idStr, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid ID' });
  
  const updateParts: string[] = [];
  const params: any[] = [];
  
  if (status) {
    updateParts.push('status = ?');
    params.push(status);
  }
  if (notes !== undefined) {
    updateParts.push('notes = ?');
    params.push(notes);
  }
  
  if (updateParts.length === 0) return res.status(400).json({ error: 'No updates provided' });
  
  updateParts.push("updated_at = datetime('now')");
  params.push(id);
  
  await execute(`UPDATE fraud_indicators SET ${updateParts.join(', ')} WHERE id = ?`, params);
  const results = await query('SELECT * FROM fraud_indicators WHERE id = ?', [id]);
  results.length > 0 ? res.json(results[0]) : res.status(404).json({ error: 'Not found' });
}));

app.post('/api/trigger/transparent-nh', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const handle = await tasks.trigger('scrape-transparent-nh', req.body);
  res.json({ success: true, runId: handle.id });
}));

app.get('/api/trigger/runs', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const list = await runs.list({ limit: 10 });
  res.json({ runs: list.data });
}));

app.get('/api/ingestion/runs', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const history = await query('SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 20');
  res.json({ success: true, runs: history });
}));

app.get('/api/admin/db-info', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const { IS_TURSO, IS_LOCAL } = await import('../db/db-adapter.js');
  res.json({
    isTurso: IS_TURSO(),
    isLocal: IS_LOCAL(),
    databaseUrl: process.env.TURSO_DATABASE_URL ? 'SET' : 'NOT SET',
    nodeEnv: process.env.NODE_ENV,
  });
}));

app.post('/api/admin/init-db', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  await initializeDb();
  res.json({ message: 'Database initialized' });
}));

app.post('/api/admin/seed-sample-data', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const { seedDatabase } = await import('../db/seed.js');
  await seedDatabase();
  res.json({ message: 'Sample data seeded' });
}));

app.get('/api/scraper/transparent-nh/years', asyncHandler(async (req, res) => {
  res.json({ years: getAvailableFiscalYears() });
}));

// --- Scraper Trigger Endpoints ---

// Trigger CCIS Provider Directory scraper
app.post('/api/trigger/ccis', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();

  // Trigger the task in the background via Trigger.dev
  const handle = await tasks.trigger('scrape-nh-ccis', req.body);
  
  res.json({ 
    success: true, 
    message: 'CCIS scraper task triggered',
    runId: handle.id 
  });
}));

// Trigger TransparentNH Historical Ingestion (all years)
app.post('/api/trigger/transparent-nh-historical', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();

  const startTime = Date.now();

  try {
    const result = await scrapeAllHistoricalYears();
    const duration = Date.now() - startTime;

    res.json({
      success: true,
      message: 'TransparentNH historical ingestion completed',
      result,
      durationMs: duration,
    });
  } catch (error: any) {
    res.status(500).json({
      success: false,
      error: error.message || 'TransparentNH historical ingestion failed',
    });
  }
}));

// Trigger TransparentNH Current Fiscal Year scraper
app.post('/api/trigger/transparent-nh-current', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();

  const startTime = Date.now();

  try {
    const result = await scrapeCurrentFiscalYear();
    const duration = Date.now() - startTime;

    res.json({
      success: true,
      message: 'TransparentNH current year scraper completed',
      result,
      durationMs: duration,
    });
  } catch (error: any) {
    res.status(500).json({
      success: false,
      error: error.message || 'TransparentNH current year scraper failed',
    });
  }
}));

// Catch-all
app.use('/api', (req, res) => {
  res.status(404).json({ error: 'Not Found', message: `API route not found: ${req.method} ${req.originalUrl}` });
});

// Error Handling
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error('API Error:', err);
  res.status(500).json({ error: err.message || 'Internal server error' });
});
