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
  runFullFraudAnalysis,
} from '../analyzer/fraud-detector.js';
import {
  scrapeFiscalYear,
  getAvailableFiscalYears,
  scrapeCurrentFiscalYear,
  scrapeAllHistoricalYears,
} from '../scraper/transparent-nh-scraper.js';
import { scrapeCCIS } from '../scraper/nh-ccis-scraper.js';
import { tasks, runs, configure } from '@trigger.dev/sdk/v3';
import { scrapeUSASpending, getNHStateOverview } from '../scraper/usaspending-scraper.js';
import { scrapeDHHSContracts, getDHHSContracts, linkContractToProvider } from '../scrapers/dhhs-contracts.js';

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
  res.json({ sources: await dbHelpers.getDataSources() });
}));

app.get('/api/federal/summary', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const summary = await query('SELECT fiscal_year, COUNT(*) as award_count, SUM(amount) as total_amount FROM expenditures WHERE source_url LIKE ? GROUP BY fiscal_year ORDER BY fiscal_year DESC', ['%usaspending%']);
  res.json({ 
    totalFederalAmount: summary.reduce((s: any, r: any) => s + (r.total_amount || 0), 0), 
    totalAwards: summary.reduce((s: any, r: any) => s + (r.award_count || 0), 0), 
    byFiscalYear: summary 
  });
}));

app.get('/api/federal/awards', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const awards = await query('SELECT * FROM expenditures WHERE source_url LIKE ? LIMIT 50', ['%usaspending%']);
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

// Get status of recent Trigger.dev runs
app.get('/api/trigger/runs', requireAuth, asyncHandler(async (req, res) => {
  try {
    const recentRuns = await runs.list({ limit: 10 });
    res.json({
      runs: recentRuns.data.map(run => ({
        id: run.id,
        taskIdentifier: run.taskIdentifier,
        status: run.status,
        createdAt: run.createdAt,
        updatedAt: run.updatedAt,
        finishedAt: run.finishedAt,
        metadata: run.metadata,
      }))
    });
  } catch (error: any) {
    res.json({ runs: [], error: error.message });
  }
}));

// Get details of a specific run including metadata
app.get('/api/trigger/runs/:runId', requireAuth, asyncHandler(async (req, res) => {
  try {
    const runId = req.params.runId as string;
    const run = await runs.retrieve(runId);
    res.json({
      id: run.id,
      taskIdentifier: run.taskIdentifier,
      status: run.status,
      createdAt: run.createdAt,
      updatedAt: run.updatedAt,
      finishedAt: run.finishedAt,
      metadata: run.metadata,
      output: run.output,
    });
  } catch (error: any) {
    res.status(404).json({ error: error.message });
  }
}));

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

app.get('/api/admin/sources', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const tables = await query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'source_%'");
  res.json({ tables: tables.map(t => t.name) });
}));

app.get('/api/admin/sources/:table', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const table = req.params.table as string;
  if (!table.startsWith('source_')) return res.status(403).json({ error: 'Access denied' });
  
  const page = parseInt(req.query.page as string || '1', 10);
  const limit = parseInt(req.query.limit as string || '100', 10);
  const offset = (page - 1) * limit;
  
  // Dynamic filtering
  const filters: Record<string, string> = {};
  Object.keys(req.query).forEach(key => {
    if (key.startsWith('filter_')) {
      const col = key.replace('filter_', '');
      filters[col] = req.query[key] as string;
    }
  });

  let whereClause = '';
  const params: any[] = [];
  
  const filterKeys = Object.keys(filters);
  if (filterKeys.length > 0) {
    whereClause = ' WHERE ' + filterKeys.map(col => {
      params.push(`%${filters[col]}%`);
      return `"${col}" LIKE ?`;
    }).join(' AND ');
  }

  // Get total count for pagination
  const countResult = await query(`SELECT COUNT(*) as count FROM "${table}"${whereClause}`, params);
  const totalCount = countResult[0]?.count || 0;

  // Get data
  const rows = await query(`SELECT * FROM "${table}"${whereClause} LIMIT ? OFFSET ?`, [...params, limit, offset]);
  
  res.json({ 
    table, 
    rows,
    pagination: {
      totalCount,
      totalPages: Math.ceil(totalCount / limit),
      currentPage: page,
      limit
    }
  });
}));

// --- DHHS Contracts Routes ---

// Get all DHHS contracts (public)
app.get('/api/dhhs-contracts', asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  const filters = {
    immigrantRelatedOnly: req.query.immigrant_related === 'true',
    withFraudIndicatorsOnly: req.query.with_fraud_indicators === 'true',
    vendor: typeof req.query.vendor === 'string' ? req.query.vendor : undefined,
  };
  
  const contracts = await getDHHSContracts(filters);
  
  // Summary stats
  const stats = {
    total: contracts.length,
    immigrantRelated: contracts.filter(c => c.isImmigrantRelated).length,
    withFraudIndicators: contracts.filter(c => c.fraudIndicators && c.fraudIndicators.length > 0).length,
    totalValue: contracts.reduce((sum, c) => sum + (c.awardedValue || 0), 0),
    byVendor: {} as Record<string, { count: number; totalValue: number }>,
    bySolicitationType: {} as Record<string, number>,
  };
  
  // Aggregate by vendor
  for (const contract of contracts) {
    if (contract.awardedVendor) {
      if (!stats.byVendor[contract.awardedVendor]) {
        stats.byVendor[contract.awardedVendor] = { count: 0, totalValue: 0 };
      }
      stats.byVendor[contract.awardedVendor].count++;
      stats.byVendor[contract.awardedVendor].totalValue += contract.awardedValue || 0;
    }
    
    const solType = contract.solicitationType || 'unknown';
    stats.bySolicitationType[solType] = (stats.bySolicitationType[solType] || 0) + 1;
  }
  
  res.json({ contracts, stats });
}));

// Get single DHHS contract by RFP number
app.get('/api/dhhs-contracts/:rfpNumber', asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  const rfpNumber = req.params.rfpNumber;
  const contracts = await getDHHSContracts();
  const contract = contracts.find(c => c.rfpNumber === rfpNumber);
  
  if (!contract) {
    return res.status(404).json({ error: 'Contract not found' });
  }
  
  // Get related data from other sources
  const relatedGCDocs = await query(
    "SELECT * FROM scraped_documents WHERE source_key = 'governor_council' AND (title LIKE ? OR raw_content LIKE ?)",
    [`%${rfpNumber}%`, `%${rfpNumber}%`]
  );
  
  const relatedExpenditures = contract.awardedVendor 
    ? await query("SELECT * FROM expenditures WHERE vendor_name LIKE ?", [`%${contract.awardedVendor}%`])
    : [];
  
  res.json({
    contract,
    relatedData: {
      gcAgendaItems: relatedGCDocs,
      expenditures: relatedExpenditures,
    },
  });
}));

// Trigger DHHS contracts scraper (protected)
app.post('/api/trigger/dhhs-contracts', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  const startTime = Date.now();
  
  try {
    const result = await scrapeDHHSContracts();
    const duration = Date.now() - startTime;
    
    res.json({
      success: true,
      message: 'DHHS contracts scraper completed',
      stats: result.stats,
      durationMs: duration,
    });
  } catch (error: any) {
    res.status(500).json({
      success: false,
      error: error.message || 'DHHS contracts scraper failed',
    });
  }
}));

// Link a DHHS contract to a provider (protected)
app.post('/api/dhhs-contracts/:rfpNumber/link-provider', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  const { providerId } = req.body;
  const rfpNumber = req.params.rfpNumber as string;
  
  if (!providerId) {
    return res.status(400).json({ error: 'providerId is required' });
  }
  
  try {
    await linkContractToProvider(rfpNumber, providerId);
    res.json({ success: true, message: `Linked ${rfpNumber} to provider ${providerId}` });
  } catch (error: any) {
    res.status(400).json({ error: error.message });
  }
}));

// Cross-reference analysis endpoint (protected)
app.get('/api/analysis/cross-reference', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  // Get all data sources
  const dhhsContracts = await getDHHSContracts();
  const gcDocs = await query("SELECT * FROM scraped_documents WHERE source_key = 'governor_council'");
  const expenditures = await query("SELECT * FROM expenditures ORDER BY fiscal_year DESC LIMIT 1000");
  
  // Build cross-reference report
  const vendorSummary: Record<string, {
    dhhsContracts: number;
    dhhsContractValue: number;
    gcAgendaItems: number;
    expenditures: number;
    expenditureTotal: number;
    fraudIndicators: string[];
    discrepancies: string[];
  }> = {};
  
  // Process DHHS contracts
  for (const contract of dhhsContracts) {
    if (!contract.awardedVendor) continue;
    
    const vendor = contract.awardedVendor;
    if (!vendorSummary[vendor]) {
      vendorSummary[vendor] = {
        dhhsContracts: 0,
        dhhsContractValue: 0,
        gcAgendaItems: 0,
        expenditures: 0,
        expenditureTotal: 0,
        fraudIndicators: [],
        discrepancies: [],
      };
    }
    
    vendorSummary[vendor].dhhsContracts++;
    vendorSummary[vendor].dhhsContractValue += contract.awardedValue || 0;
    
    for (const indicator of contract.fraudIndicators || []) {
      vendorSummary[vendor].fraudIndicators.push(`[${indicator.severity}] ${indicator.type}: ${indicator.description}`);
    }
  }
  
  // Process expenditures
  for (const exp of expenditures) {
    // Try to match to known vendors
    for (const [vendor, summary] of Object.entries(vendorSummary)) {
      if (exp.vendor_name && exp.vendor_name.toLowerCase().includes(vendor.toLowerCase().split(' ')[0])) {
        summary.expenditures++;
        summary.expenditureTotal += exp.amount || 0;
      }
    }
  }
  
  // Identify discrepancies
  for (const [vendor, summary] of Object.entries(vendorSummary)) {
    if (summary.dhhsContractValue > 0 && summary.expenditureTotal > summary.dhhsContractValue * 1.1) {
      summary.discrepancies.push(
        `Expenditures ($${summary.expenditureTotal.toLocaleString()}) exceed contract value ($${summary.dhhsContractValue.toLocaleString()})`
      );
    }
    
    if (summary.dhhsContracts > 0 && summary.expenditures === 0) {
      summary.discrepancies.push('Has contracts but no matching expenditure records found');
    }
  }
  
  res.json({
    vendors: vendorSummary,
    summary: {
      totalVendors: Object.keys(vendorSummary).length,
      totalContractValue: Object.values(vendorSummary).reduce((s, v) => s + v.dhhsContractValue, 0),
      totalExpenditures: Object.values(vendorSummary).reduce((s, v) => s + v.expenditureTotal, 0),
      vendorsWithDiscrepancies: Object.values(vendorSummary).filter(v => v.discrepancies.length > 0).length,
      vendorsWithFraudIndicators: Object.values(vendorSummary).filter(v => v.fraudIndicators.length > 0).length,
    },
  });
}));

// Catch-all
app.use('/api', (req, res) => {
  res.status(404).json({ error: 'Not Found', message: `API route not found: ${req.method} ${req.originalUrl}` });
});


// Error Handling
let lastError: { message: string; timestamp: number } | null = null;
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  const errorMessage = err.message || 'Internal server error';
  const now = Date.now();
  
  // Suppress duplicate errors within 2 seconds
  if (lastError && lastError.message === errorMessage && (now - lastError.timestamp) < 2000) {
    // Skip logging duplicate errors
  } else {
    console.error(`API Error [${req.method} ${req.path}]:`, errorMessage);
    lastError = { message: errorMessage, timestamp: now };
  }
  
  res.status(500).json({ error: errorMessage });
});
