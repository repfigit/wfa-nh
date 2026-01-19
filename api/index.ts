import type { VercelRequest, VercelResponse } from '@vercel/node';
import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import { initializeDb, dbHelpers } from '../src/db/database.js';
import { query, execute } from '../src/db/db-adapter.js';
import { importCSV, getImportStats } from '../src/importer/csv-importer.js';
import { 
  searchContracts, 
  scrapeAllChildcareContracts, 
  saveScrapedContracts,
  CHILDCARE_KEYWORDS 
} from '../src/scraper/nh-das-scraper.js';
import {
  detectStructuring,
  detectDuplicates,
  analyzeVendorConcentration,
  runFullFraudAnalysis,
} from '../src/analyzer/fraud-detector.js';
import {
  scrapeFiscalYear,
  scrapeRecentYears,
  getAvailableFiscalYears,
} from '../src/scraper/transparent-nh-scraper.js';
import { seedDatabase } from '../src/db/seed.js';

const app = express();

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb', type: 'text/csv' }));

// Error handler
const asyncHandler = (fn: (req: Request, res: Response, next: NextFunction) => Promise<any>) => 
  (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };

// Initialize database on cold start
let initialized = false;
async function ensureInitialized() {
  if (!initialized) {
    await initializeDb();
    // Note: We migrated existing data to Turso, so we don't seed fresh data
    // await seedDatabase();
    initialized = true;
  }
}

// API Routes

// Dashboard stats
app.get('/api/dashboard', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const stats = await dbHelpers.getDashboardStats();
  res.json(stats);
}));

// Providers (Daycares)
app.get('/api/providers', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const immigrantOwnedOnly = req.query.immigrant_owned === 'true';
  const query = req.query.q as string | undefined;
  
  if (query) {
    const providers = await dbHelpers.searchProviders(query);
    res.json(providers);
  } else {
    const providers = await dbHelpers.getAllProviders(immigrantOwnedOnly);
    res.json(providers);
  }
}));

app.get('/api/providers/:id', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const id = parseInt(req.params.id as string);
  const provider = await dbHelpers.getProviderWithPayments(id);
  if (!provider) {
    return res.status(404).json({ error: 'Provider not found' });
  }
  res.json(provider);
}));

app.get('/api/providers/:id/payments', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const id = parseInt(req.params.id as string);
  const payments = await dbHelpers.getAllPayments({ provider_id: id });
  res.json(payments);
}));

// Payments
app.get('/api/payments', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const filters = {
    provider_id: req.query.provider_id ? parseInt(req.query.provider_id as string) : undefined,
    fiscal_year: req.query.fiscal_year ? parseInt(req.query.fiscal_year as string) : undefined,
    limit: req.query.limit ? parseInt(req.query.limit as string) : 100,
  };
  const payments = await dbHelpers.getAllPayments(filters);
  res.json(payments);
}));

// Fraud Indicators
app.get('/api/fraud-indicators', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const filters = {
    status: req.query.status as string | undefined,
    severity: req.query.severity as string | undefined,
    provider_id: req.query.provider_id ? parseInt(req.query.provider_id as string) : undefined,
  };
  const indicators = await dbHelpers.getFraudIndicators(filters);
  res.json(indicators);
}));

app.patch('/api/fraud-indicators/:id', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const { status, notes } = req.body;
  const id = parseInt(req.params.id as string);
  
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
  
  if (updateParts.length === 0) {
    return res.status(400).json({ error: 'No updates provided' });
  }
  
  updateParts.push("updated_at = datetime('now')");
  params.push(id);
  
  const sqlQuery = `UPDATE fraud_indicators SET ${updateParts.join(', ')} WHERE id = ?`;
  await execute(sqlQuery, params);
  
  const results = await query('SELECT * FROM fraud_indicators WHERE id = ?', [id]);
  
  if (results.length === 0) {
    return res.status(404).json({ error: 'Fraud indicator not found' });
  }
  
  res.json(results[0]);
}));

// CSV Import
app.post('/api/import/csv', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const csvContent = req.body.csv || req.body;
  const filterChildcare = req.query.filter_childcare === 'true';
  const createProviders = req.query.create_providers === 'true';
  
  if (!csvContent || typeof csvContent !== 'string') {
    return res.status(400).json({ error: 'CSV content required in request body' });
  }
  
  const result = await importCSV(csvContent, {
    filterChildcare,
    createProviders,
  });
  
  res.json(result);
}));

// Get import statistics
app.get('/api/import/stats', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const stats = await getImportStats();
  res.json(stats);
}));

// Get expenditures
app.get('/api/expenditures', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
  const fiscal_year = req.query.fiscal_year ? parseInt(req.query.fiscal_year as string) : undefined;
  
  let sqlQuery = `
    SELECT e.*, p.name as provider_name
    FROM expenditures e
    LEFT JOIN providers p ON e.provider_id = p.id
    WHERE 1=1
  `;
  const params: any[] = [];
  
  if (fiscal_year) {
    sqlQuery += ' AND e.fiscal_year = ?';
    params.push(fiscal_year);
  }
  
  sqlQuery += ' ORDER BY e.amount DESC LIMIT ?';
  params.push(limit);
  
  const results = await query(sqlQuery, params);
  res.json(results);
}));

// NH DAS Scraper
app.post('/api/scraper/search', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const keyword = req.body.keyword || 'daycare';
  
  const result = await searchContracts(keyword);
  
  if (result.success && result.contracts.length > 0) {
    const saved = await saveScrapedContracts(result.contracts);
    res.json({ ...result, saved });
  } else {
    res.json(result);
  }
}));

app.post('/api/scraper/full', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const result = await scrapeAllChildcareContracts();
  
  res.json({
    success: result.errors.length === 0,
    keywords_searched: CHILDCARE_KEYWORDS.length,
    ...result,
  });
}));

// TransparentNH Scraper
app.get('/api/scraper/transparent-nh/years', asyncHandler(async (req, res) => {
  const years = getAvailableFiscalYears();
  res.json({ years });
}));

app.post('/api/scraper/transparent-nh', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  
  if (!fiscalYear) {
    return res.status(400).json({ error: 'fiscalYear is required' });
  }
  
  const result = await scrapeFiscalYear(fiscalYear);
  res.json(result);
}));

app.post('/api/scraper/transparent-nh/recent', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const results = await scrapeRecentYears();
  
  const summary = {
    success: results.every(r => r.success),
    years_scraped: results.length,
    total_records: results.reduce((sum, r) => sum + r.totalRecords, 0),
    childcare_records: results.reduce((sum, r) => sum + r.childcareRecords, 0),
    imported_records: results.reduce((sum, r) => sum + r.importedRecords, 0),
    total_amount: results.reduce((sum, r) => sum + r.totalAmount, 0),
    results,
  };
  
  res.json(summary);
}));

// Get scraped contracts
app.get('/api/contracts', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 50;
  
  const results = await query(`
    SELECT c.*, co.name as contractor_name
    FROM contracts c
    LEFT JOIN contractors co ON c.contractor_id = co.id
    ORDER BY c.created_at DESC
    LIMIT ?
  `, [limit]);
  
  res.json(results);
}));

// Fraud Analysis
app.post('/api/analyze/fraud', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const result = await runFullFraudAnalysis();
  res.json({ success: true, ...result });
}));

app.get('/api/analyze/structuring', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const threshold = req.query.threshold ? parseInt(req.query.threshold as string) : 10000;
  const windowDays = req.query.window_days ? parseInt(req.query.window_days as string) : 7;
  
  const flags = await detectStructuring(threshold, windowDays);
  
  res.json({
    threshold,
    window_days: windowDays,
    flags,
    count: flags.length,
  });
}));

app.get('/api/analyze/duplicates', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const duplicates = await detectDuplicates();
  res.json({ duplicates, count: duplicates.length });
}));

app.get('/api/analyze/concentration', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const topN = req.query.top_n ? parseInt(req.query.top_n as string) : 10;
  const concentration = await analyzeVendorConcentration(topN);
  res.json({ top_n: topN, vendors: concentration });
}));

// Reports
app.get('/api/reports/summary', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const report = await dbHelpers.getReportSummary();
  res.json(report);
}));

// Data Sources
app.get('/api/data-sources', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const sources = await dbHelpers.getDataSources();
  res.json(sources);
}));

// Clean up duplicates (admin endpoint)
app.post('/api/admin/cleanup-duplicates', asyncHandler(async (req, res) => {
  await ensureInitialized();
  await dbHelpers.cleanupDuplicateDataSources();
  const sources = await dbHelpers.getDataSources();
  res.json({ 
    message: 'Duplicates cleaned up', 
    remaining: sources.length,
    sources 
  });
}));

// Force schema creation and seeding (admin endpoint)
app.post('/api/admin/init-db', asyncHandler(async (req, res) => {
  console.log('Manually initializing database...');
  const { createClient } = await import('@libsql/client');
  
  try {
    // Connect directly to Turso
    const client = createClient({
      url: process.env.TURSO_DATABASE_URL!,
      authToken: process.env.TURSO_AUTH_TOKEN,
    });
    
    const results: string[] = [];
    
    // Create data_sources table directly
    try {
      await client.execute(`
        CREATE TABLE IF NOT EXISTS data_sources (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          url TEXT,
          type TEXT,
          last_scraped TEXT,
          notes TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      `);
      results.push('data_sources table: CREATED');
    } catch (err: any) {
      results.push(`data_sources table: ERROR - ${err.message}`);
    }
    
    // Create keywords table
    try {
      await client.execute(`
        CREATE TABLE IF NOT EXISTS keywords (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          keyword TEXT NOT NULL UNIQUE,
          category TEXT,
          weight REAL DEFAULT 1.0
        )
      `);
      results.push('keywords table: CREATED');
    } catch (err: any) {
      results.push(`keywords table: ERROR - ${err.message}`);
    }
    
    // Create other missing tables
    const otherTables = [
      `CREATE TABLE IF NOT EXISTS contractors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        dba_name TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        is_immigrant_owned INTEGER DEFAULT 0,
        owner_background TEXT,
        vendor_id TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS contracts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contractor_id INTEGER,
        contract_number TEXT,
        description TEXT,
        department TEXT,
        agency TEXT,
        start_date TEXT,
        end_date TEXT,
        original_amount REAL,
        current_amount REAL,
        status TEXT,
        contract_type TEXT,
        source_url TEXT,
        approval_date TEXT,
        gc_item_number TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS expenditures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_id INTEGER,
        contractor_id INTEGER,
        contract_id INTEGER,
        fiscal_year INTEGER,
        department TEXT,
        agency TEXT,
        activity TEXT,
        expense_class TEXT,
        vendor_name TEXT,
        amount REAL,
        payment_date TEXT,
        check_number TEXT,
        description TEXT,
        source_url TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS scrape_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_source_id INTEGER,
        started_at TEXT DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT,
        records_found INTEGER DEFAULT 0,
        records_imported INTEGER DEFAULT 0,
        status TEXT DEFAULT 'running',
        error_message TEXT
      )`
    ];
    
    for (const sql of otherTables) {
      try {
        await client.execute(sql);
        results.push('Table created: OK');
      } catch (err: any) {
        results.push(`Table error: ${err.message}`);
      }
    }
    
    // Check tables now
    const tables = await client.execute("SELECT name FROM sqlite_master WHERE type='table'");
    
    // Seed data sources
    const { dataSources } = await import('../src/db/data-sources.js');
    
    let dsCount = 0;
    try {
      const countResult = await client.execute('SELECT COUNT(*) as count FROM data_sources');
      dsCount = Number(countResult.rows[0]?.count) || 0;
    } catch (e) {
      // Ignore
    }
    
    if (dsCount === 0) {
      console.log('Seeding data sources...');
      for (const ds of dataSources) {
        try {
          await client.execute({
            sql: 'INSERT OR IGNORE INTO data_sources (name, url, type, notes) VALUES (?, ?, ?, ?)',
            args: [ds.name, ds.url, ds.type, ds.notes]
          });
        } catch (err: any) {
          results.push(`Seed error: ${err.message}`);
        }
      }
      results.push(`Seeded ${dataSources.length} data sources`);
    } else {
      results.push(`Data sources already seeded: ${dsCount}`);
    }
    
    // Final check
    const finalCount = await client.execute('SELECT COUNT(*) as count FROM data_sources');
    const sources = await client.execute('SELECT * FROM data_sources');
    
    res.json({ 
      message: 'Database initialized',
      executionResults: results,
      tables: tables.rows.map((r: any) => r.name),
      dataSourcesCount: Number(finalCount.rows[0]?.count),
      dataSources: sources.rows
    });
  } catch (error: any) {
    console.error('Init error:', error);
    res.status(500).json({ error: error.message, stack: error.stack });
  }
}));

// Force schema creation (admin endpoint)
app.post('/api/admin/create-schema', asyncHandler(async (req, res) => {
  console.log('Manually creating schema...');
  const { executeRaw } = await import('../src/db/db-adapter.js');
  const { postgresSchema } = await import('../src/db/schema-postgres.js');
  
  try {
    await executeRaw(postgresSchema);
    res.json({ message: 'Schema created successfully' });
  } catch (error) {
    console.error('Schema creation error:', error);
    res.status(500).json({ error: error.message });
  }
}));

// Debug database info
app.get('/api/admin/db-info', asyncHandler(async (req, res) => {
  const { IS_TURSO, IS_LOCAL, initDb, query } = await import('../src/db/db-adapter.js');
  
  // Initialize DB first
  await initDb();
  
  const env: any = {
    TURSO_DATABASE_URL: process.env.TURSO_DATABASE_URL ? 'SET' : 'NOT SET',
    TURSO_AUTH_TOKEN: process.env.TURSO_AUTH_TOKEN ? 'SET' : 'NOT SET',
    isTurso: IS_TURSO(),
    isLocal: IS_LOCAL(),
    nodeEnv: process.env.NODE_ENV
  };
  
  // Test basic query
  try {
    const testResult = await query('SELECT 1 as test');
    env.testQuery = testResult;
  } catch (error: any) {
    env.testQueryError = error.message;
  }
  
  // Check tables
  try {
    const tables = await query("SELECT name FROM sqlite_master WHERE type='table'");
    env.tables = tables.map((t: any) => t.name);
  } catch (error: any) {
    env.tablesError = error.message;
  }
  
  // Check data_sources count
  try {
    const dsCount = await query('SELECT COUNT(*) as count FROM data_sources');
    env.dataSourcesCount = dsCount[0]?.count;
  } catch (error: any) {
    env.dataSourcesError = error.message;
  }
  
  res.json(env);
}));

// ============================================
// SCRAPER JOB MANAGEMENT
// ============================================

// Create scrape_jobs table if not exists
async function ensureScrapeJobsTable() {
  await execute(`
    CREATE TABLE IF NOT EXISTS scrape_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scraper_name TEXT NOT NULL,
      status TEXT DEFAULT 'pending',
      progress INTEGER DEFAULT 0,
      total_steps INTEGER DEFAULT 100,
      current_step TEXT,
      records_found INTEGER DEFAULT 0,
      records_imported INTEGER DEFAULT 0,
      errors TEXT,
      started_at TEXT DEFAULT CURRENT_TIMESTAMP,
      completed_at TEXT,
      result_summary TEXT
    )
  `);
}

// Create a new scrape job
async function createScrapeJob(scraperName: string, totalSteps: number = 100): Promise<number> {
  await ensureScrapeJobsTable();
  const result = await execute(
    `INSERT INTO scrape_jobs (scraper_name, status, progress, total_steps, current_step) 
     VALUES (?, 'running', 0, ?, 'Initializing...')`,
    [scraperName, totalSteps]
  );
  return result.lastId || 0;
}

// Update scrape job progress
async function updateScrapeJob(jobId: number, updates: {
  progress?: number;
  current_step?: string;
  records_found?: number;
  records_imported?: number;
  status?: string;
  errors?: string;
  result_summary?: string;
}) {
  const setParts: string[] = [];
  const params: any[] = [];
  
  if (updates.progress !== undefined) { setParts.push('progress = ?'); params.push(updates.progress); }
  if (updates.current_step !== undefined) { setParts.push('current_step = ?'); params.push(updates.current_step); }
  if (updates.records_found !== undefined) { setParts.push('records_found = ?'); params.push(updates.records_found); }
  if (updates.records_imported !== undefined) { setParts.push('records_imported = ?'); params.push(updates.records_imported); }
  if (updates.status !== undefined) { setParts.push('status = ?'); params.push(updates.status); }
  if (updates.errors !== undefined) { setParts.push('errors = ?'); params.push(updates.errors); }
  if (updates.result_summary !== undefined) { setParts.push('result_summary = ?'); params.push(updates.result_summary); }
  
  if (updates.status === 'completed' || updates.status === 'failed') {
    setParts.push("completed_at = datetime('now')");
  }
  
  if (setParts.length > 0) {
    params.push(jobId);
    await execute(`UPDATE scrape_jobs SET ${setParts.join(', ')} WHERE id = ?`, params);
  }
}

// Get all scrape jobs
app.get('/api/scraper/jobs', asyncHandler(async (req, res) => {
  await ensureInitialized();
  await ensureScrapeJobsTable();
  const jobs = await query('SELECT * FROM scrape_jobs ORDER BY started_at DESC LIMIT 50');
  res.json(jobs);
}));

// Get specific job status
app.get('/api/scraper/jobs/:id', asyncHandler(async (req, res) => {
  await ensureInitialized();
  await ensureScrapeJobsTable();
  const jobId = parseInt(req.params.id as string);
  const jobs = await query('SELECT * FROM scrape_jobs WHERE id = ?', [jobId]);
  if (jobs.length === 0) {
    return res.status(404).json({ error: 'Job not found' });
  }
  res.json(jobs[0]);
}));

// Get active/running jobs
app.get('/api/scraper/jobs/active', asyncHandler(async (req, res) => {
  await ensureInitialized();
  await ensureScrapeJobsTable();
  const jobs = await query("SELECT * FROM scrape_jobs WHERE status = 'running' ORDER BY started_at DESC");
  res.json(jobs);
}));

// ============================================
// INDIVIDUAL SCRAPER ENDPOINTS
// ============================================

// TransparentNH Scraper with job tracking
app.post('/api/scraper/transparent-nh/start', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year || new Date().getFullYear();
  
  // Create job
  const jobId = await createScrapeJob('transparent-nh', 5);
  
  // Return immediately with job ID
  res.json({ jobId, message: 'Scraper started', status: 'running' });
  
  // Run scraper in background
  (async () => {
    try {
      await updateScrapeJob(jobId, { progress: 10, current_step: 'Fetching fiscal year data...' });
      const result = await scrapeFiscalYear(fiscalYear);
      
      await updateScrapeJob(jobId, { 
        progress: 100, 
        status: 'completed',
        records_found: result.totalRecords,
        records_imported: result.importedRecords,
        current_step: 'Complete',
        result_summary: JSON.stringify(result)
      });
    } catch (error: any) {
      await updateScrapeJob(jobId, { 
        status: 'failed', 
        errors: error.message,
        current_step: 'Failed'
      });
    }
  })();
}));

// NH DAS Contracts Scraper with job tracking  
app.post('/api/scraper/contracts/start', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const keyword = req.body.keyword || 'childcare';
  
  // Create job
  const jobId = await createScrapeJob('nh-das-contracts', 10);
  
  // Return immediately with job ID
  res.json({ jobId, message: 'Scraper started', status: 'running' });
  
  // Run scraper in background
  (async () => {
    try {
      await updateScrapeJob(jobId, { progress: 10, current_step: `Searching for "${keyword}" contracts...` });
      
      const result = await searchContracts(keyword);
      
      if (result.success && result.contracts.length > 0) {
        await updateScrapeJob(jobId, { progress: 50, current_step: 'Saving contracts to database...' });
        const savedCount = await saveScrapedContracts(result.contracts);
        
        await updateScrapeJob(jobId, { 
          progress: 100, 
          status: 'completed',
          records_found: result.contracts.length,
          records_imported: savedCount,
          current_step: 'Complete',
          result_summary: JSON.stringify({ ...result, savedCount })
        });
      } else {
        await updateScrapeJob(jobId, { 
          progress: 100, 
          status: 'completed',
          records_found: 0,
          records_imported: 0,
          current_step: 'Complete - No results',
          result_summary: JSON.stringify(result)
        });
      }
    } catch (error: any) {
      await updateScrapeJob(jobId, { 
        status: 'failed', 
        errors: error.message,
        current_step: 'Failed'
      });
    }
  })();
}));

// Full childcare contracts scraper
app.post('/api/scraper/contracts/full/start', asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  // Create job
  const jobId = await createScrapeJob('nh-das-full', CHILDCARE_KEYWORDS.length);
  
  // Return immediately with job ID
  res.json({ jobId, message: 'Full scraper started', status: 'running', keywords: CHILDCARE_KEYWORDS });
  
  // Run scraper in background
  (async () => {
    try {
      let totalFound = 0;
      let totalImported = 0;
      const errors: string[] = [];
      
      for (let i = 0; i < CHILDCARE_KEYWORDS.length; i++) {
        const keyword = CHILDCARE_KEYWORDS[i];
        await updateScrapeJob(jobId, { 
          progress: Math.round(((i + 1) / CHILDCARE_KEYWORDS.length) * 100),
          current_step: `Searching: "${keyword}" (${i + 1}/${CHILDCARE_KEYWORDS.length})...`
        });
        
        try {
          const result = await searchContracts(keyword);
          if (result.success && result.contracts.length > 0) {
            totalFound += result.contracts.length;
            const savedCount = await saveScrapedContracts(result.contracts);
            totalImported += savedCount;
          }
        } catch (err: any) {
          errors.push(`${keyword}: ${err.message}`);
        }
        
        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      await updateScrapeJob(jobId, { 
        progress: 100, 
        status: errors.length === CHILDCARE_KEYWORDS.length ? 'failed' : 'completed',
        records_found: totalFound,
        records_imported: totalImported,
        current_step: 'Complete',
        errors: errors.length > 0 ? errors.join('; ') : undefined,
        result_summary: JSON.stringify({ totalFound, totalImported, errors })
      });
    } catch (error: any) {
      await updateScrapeJob(jobId, { 
        status: 'failed', 
        errors: error.message,
        current_step: 'Failed'
      });
    }
  })();
}));

// Fraud Analysis with job tracking
app.post('/api/analyze/fraud/start', asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  // Create job
  const jobId = await createScrapeJob('fraud-analysis', 4);
  
  // Return immediately with job ID
  res.json({ jobId, message: 'Fraud analysis started', status: 'running' });
  
  // Run analysis in background
  (async () => {
    try {
      await updateScrapeJob(jobId, { progress: 25, current_step: 'Detecting structuring patterns...' });
      await updateScrapeJob(jobId, { progress: 50, current_step: 'Finding duplicate payments...' });
      await updateScrapeJob(jobId, { progress: 75, current_step: 'Analyzing vendor concentration...' });
      
      const result = await runFullFraudAnalysis();
      
      const totalIndicators = result.structuring.length + result.duplicates.length + result.vendorConcentration.length;
      
      await updateScrapeJob(jobId, { 
        progress: 100, 
        status: 'completed',
        records_found: totalIndicators,
        records_imported: result.savedIndicators || 0,
        current_step: 'Complete',
        result_summary: JSON.stringify(result)
      });
    } catch (error: any) {
      await updateScrapeJob(jobId, { 
        status: 'failed', 
        errors: error.message,
        current_step: 'Failed'
      });
    }
  })();
}));

// Search
app.get('/api/search', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const searchQuery = req.query.q as string;
  if (!searchQuery) {
    return res.status(400).json({ error: 'Query parameter "q" is required' });
  }
  
  const searchTerm = `%${searchQuery}%`;
  
  const providers = await query(`
    SELECT id, name, 'provider' as type FROM providers 
    WHERE name LIKE ? OR owner_name LIKE ? OR notes LIKE ?
    LIMIT 10
  `, [searchTerm, searchTerm, searchTerm]);
  
  const expenditures = await query(`
    SELECT id, vendor_name as name, 'expenditure' as type FROM expenditures
    WHERE vendor_name LIKE ? OR description LIKE ?
    LIMIT 10
  `, [searchTerm, searchTerm]);
  
  const fraudIndicators = await query(`
    SELECT id, description as name, 'fraud_indicator' as type FROM fraud_indicators 
    WHERE description LIKE ? OR evidence LIKE ?
    LIMIT 10
  `, [searchTerm, searchTerm]);
  
  res.json({
    providers,
    expenditures,
    fraud_indicators: fraudIndicators,
  });
}));

// Error handling middleware
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error('Error:', err);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

// Export for Vercel
export default async function handler(req: VercelRequest, res: VercelResponse) {
  // Handle the request with Express
  return new Promise((resolve) => {
    app(req as any, res as any, () => {
      resolve(undefined);
    });
  });
}
