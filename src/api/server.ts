import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { initializeDb, dbHelpers } from '../db/database.js';
import { query, execute } from '../db/db-adapter.js';
import { importCSV } from '../importer/csv-importer.js';
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
} from '../scraper/transparent-nh-scraper.js';
import { tasks, runs, configure } from '@trigger.dev/sdk/v3';
import { scrapeUSASpending, getNHStateOverview } from '../scraper/usaspending-scraper.js';
import { scrapeACFData, getNHCCDFStats, getAvailableFiscalYears as getACFFiscalYears } from '../scraper/acf-ccdf-scraper.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3001;

// Configure Trigger.dev
configure({ secretKey: process.env.TRIGGER_SECRET_KEY });

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb', type: 'text/csv' }));
app.use(express.static(path.join(__dirname, '../../public')));

const asyncHandler = (fn: (req: Request, res: Response, next: NextFunction) => Promise<any>) => 
  (req: Request, res: Response, next: NextFunction) => Promise.resolve(fn(req, res, next)).catch(next);

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  const apiKey = req.headers['x-api-key'] as string;
  const expectedKey = process.env.ADMIN_API_KEY;
  if (!expectedKey) return next();
  if (apiKey !== expectedKey) return res.status(401).json({ error: 'Unauthorized', message: 'Invalid API key' });
  next();
};

// API Routes
app.get('/api/dashboard', asyncHandler(async (req, res) => res.json(await dbHelpers.getDashboardStats())));

app.get('/api/providers', asyncHandler(async (req, res) => {
  const q = req.query.q;
  if (typeof q === 'string') {
    return res.json(await dbHelpers.searchProviders(q));
  }
  const immigrantOwnedOnly = req.query.immigrant_owned === 'true';
  res.json(await dbHelpers.getAllProviders(immigrantOwnedOnly));
}));

app.get('/api/providers/:id', asyncHandler(async (req, res) => {
  const p = await dbHelpers.getProviderWithPayments(parseInt(req.params.id));
  p ? res.json(p) : res.status(404).json({ error: 'Not found' });
}));

app.get('/api/payments', asyncHandler(async (req, res) => {
  res.json(await dbHelpers.getAllPayments({
    provider_id: req.query.provider_id ? parseInt(req.query.provider_id as string) : undefined,
    fiscal_year: req.query.fiscal_year ? parseInt(req.query.fiscal_year as string) : undefined,
    limit: req.query.limit ? parseInt(req.query.limit as string) : 100,
  }));
}));

app.get('/api/fraud-indicators', asyncHandler(async (req, res) => {
  res.json(await dbHelpers.getFraudIndicators({
    status: req.query.status as string,
    severity: req.query.severity as string,
  }));
}));

app.get('/api/data-sources', asyncHandler(async (req, res) => res.json(await dbHelpers.getDataSources())));

// Admin & Processing
app.get('/api/admin/db-info', requireAuth, asyncHandler(async (req, res) => {
  const { IS_TURSO, IS_LOCAL, initDb } = await import('../db/db-adapter.js');
  await initDb();
  res.json({
    isTurso: IS_TURSO(),
    isLocal: IS_LOCAL(),
    databaseUrl: process.env.TURSO_DATABASE_URL ? 'SET' : 'NOT SET',
    nodeEnv: process.env.NODE_ENV,
  });
}));

app.post('/api/trigger/transparent-nh', requireAuth, asyncHandler(async (req, res) => {
  const handle = await tasks.trigger('scrape-transparent-nh', req.body);
  res.json({ success: true, runId: handle.id });
}));

app.get('/api/trigger/runs', requireAuth, asyncHandler(async (req, res) => {
  const list = await runs.list({ limit: 10 });
  res.json({ runs: list.data });
}));

app.get('/api/ingestion/runs', requireAuth, asyncHandler(async (req, res) => {
  const history = await query('SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 20');
  res.json({ success: true, runs: history });
}));

app.get('/api/federal/summary', asyncHandler(async (req, res) => {
  const summary = await query('SELECT fiscal_year, COUNT(*) as award_count, SUM(amount) as total_amount FROM expenditures WHERE source_url LIKE "USAspending:%" GROUP BY fiscal_year ORDER BY fiscal_year DESC');
  res.json({ totalFederalAmount: summary.reduce((s:any, r:any) => s + r.total_amount, 0), totalAwards: summary.reduce((s:any, r:any) => s + r.award_count, 0), byFiscalYear: summary });
}));

app.get('/api/federal/awards', asyncHandler(async (req, res) => {
  if (req.query.refresh === 'true') {
    const result = await scrapeUSASpending();
    return res.json(result);
  }
  const awards = await query('SELECT * FROM expenditures WHERE source_url LIKE "USAspending:%" LIMIT 50');
  res.json({ awards });
}));

app.post('/api/admin/init-db', requireAuth, asyncHandler(async (req, res) => {
  await initializeDb();
  res.json({ message: 'Database initialized' });
}));

app.post('/api/admin/seed-sample-data', requireAuth, asyncHandler(async (req, res) => {
  const { seedDatabase } = await import('../db/seed.js');
  await seedDatabase();
  res.json({ message: 'Sample data seeded' });
}));

app.get('/api/scraper/transparent-nh/years', asyncHandler(async (req, res) => res.json({ years: getAvailableFiscalYears() })));

app.get('*', (req, res) => res.sendFile(path.join(__dirname, '../../public/index.html')));

export async function startServer() {
  await initializeDb();
  app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
}

export default app;
