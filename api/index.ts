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
import { tasks, runs, configure } from '@trigger.dev/sdk/v3';
import type { scrapeTransparentNH } from '../src/trigger/scrape-transparent-nh.js';
import type { scrapeNHDASContracts } from '../src/trigger/scrape-nh-das-contracts.js';
import type { scrapeNHLicensing } from '../src/trigger/scrape-nh-licensing.js';
import type { runFraudAnalysis } from '../src/trigger/run-fraud-analysis.js';
import type { scrapeUSASpendingTask } from '../src/trigger/scrape-usaspending.js';
import type { scrapeACFCCDFTask } from '../src/trigger/scrape-acf-ccdf.js';
import { scrapeUSASpending, getNHStateOverview } from '../src/scraper/usaspending-scraper.js';
import { scrapeACFData, getNHCCDFStats, getAvailableFiscalYears as getACFFiscalYears } from '../src/scraper/acf-ccdf-scraper.js';

// Configure Trigger.dev with secret key
configure({
  secretKey: process.env.TRIGGER_SECRET_KEY,
});

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

// API Key Authentication Middleware
const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  const apiKey = req.headers['x-api-key'] as string;
  const expectedKey = process.env.ADMIN_API_KEY;
  
  // If no ADMIN_API_KEY is configured, allow access (for local dev)
  if (!expectedKey) {
    console.warn('WARNING: ADMIN_API_KEY not configured - auth is disabled');
    return next();
  }
  
  if (!apiKey) {
    return res.status(401).json({ 
      error: 'Unauthorized', 
      message: 'Missing x-api-key header' 
    });
  }
  
  if (apiKey !== expectedKey) {
    return res.status(401).json({ 
      error: 'Unauthorized', 
      message: 'Invalid API key' 
    });
  }
  
  next();
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

app.patch('/api/fraud-indicators/:id', requireAuth, asyncHandler(async (req, res) => {
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

// CSV Import (protected)
app.post('/api/import/csv', requireAuth, asyncHandler(async (req, res) => {
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

// NH DAS Scraper (protected)
app.post('/api/scraper/search', requireAuth, asyncHandler(async (req, res) => {
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

app.post('/api/scraper/full', requireAuth, asyncHandler(async (req, res) => {
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

app.post('/api/scraper/transparent-nh', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  
  if (!fiscalYear) {
    return res.status(400).json({ error: 'fiscalYear is required' });
  }
  
  const result = await scrapeFiscalYear(fiscalYear);
  res.json(result);
}));

app.post('/api/scraper/transparent-nh/recent', requireAuth, asyncHandler(async (req, res) => {
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

// Seed sample data for testing (admin endpoint - protected)
app.post('/api/admin/seed-sample-data', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  const results: string[] = [];
  
  // Sample NH childcare providers
  const sampleProviders = [
    { name: 'Little Stars Daycare', city: 'Manchester', capacity: 45, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
    { name: 'Sunshine Learning Center', city: 'Nashua', capacity: 60, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
    { name: 'ABC Family Childcare', city: 'Concord', capacity: 12, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Family' },
    { name: 'Happy Kids Preschool', city: 'Dover', capacity: 35, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
    { name: 'Rainbow Child Development', city: 'Manchester', capacity: 80, accepts_ccdf: 1, is_immigrant_owned: 1, provider_type: 'Center' },
    { name: 'Tiny Tots Academy', city: 'Portsmouth', capacity: 50, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
    { name: 'First Steps Daycare', city: 'Keene', capacity: 30, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
    { name: 'Growing Minds LLC', city: 'Derry', capacity: 40, accepts_ccdf: 1, is_immigrant_owned: 1, provider_type: 'Center' },
    { name: 'Bright Futures Childcare', city: 'Rochester', capacity: 25, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
    { name: 'Little Explorers', city: 'Salem', capacity: 55, accepts_ccdf: 1, is_immigrant_owned: 0, provider_type: 'Center' },
  ];
  
  for (const provider of sampleProviders) {
    try {
      await execute(`
        INSERT OR IGNORE INTO providers (name, city, state, capacity, accepts_ccdf, is_immigrant_owned, provider_type, notes)
        VALUES (?, ?, 'NH', ?, ?, ?, ?, 'Sample data for testing')
      `, [provider.name, provider.city, provider.capacity, provider.accepts_ccdf, provider.is_immigrant_owned, provider.provider_type]);
    } catch (e) {
      // Ignore duplicates
    }
  }
  results.push(`Added ${sampleProviders.length} sample providers`);
  
  // Get provider IDs
  const providers = await query('SELECT id, name, capacity FROM providers');
  
  // Sample payments/expenditures (CCDF scholarship payments)
  const fiscalYears = [2024, 2025];
  let paymentCount = 0;
  
  for (const provider of providers) {
    for (const fy of fiscalYears) {
      // Generate monthly payments with some variation
      for (let month = 1; month <= 12; month++) {
        const baseAmount = (provider.capacity as number) * 800; // ~$800 per child per month
        const variance = (Math.random() - 0.5) * baseAmount * 0.3; // +/- 15% variance
        const amount = Math.round(baseAmount + variance);
        
        try {
          await execute(`
            INSERT INTO payments (provider_id, fiscal_year, fiscal_month, amount, payment_type, funding_source)
            VALUES (?, ?, ?, ?, 'CCDF Scholarship', 'Federal CCDF')
          `, [provider.id, fy, month, amount]);
          paymentCount++;
        } catch (e) {
          // Ignore errors
        }
        
        // Also add to expenditures for cross-referencing
        try {
          await execute(`
            INSERT INTO expenditures (provider_id, fiscal_year, department, vendor_name, amount, description, source_url)
            VALUES (?, ?, 'Health and Human Services', ?, ?, 'CCDF Scholarship Payment', 'Sample Data')
          `, [provider.id, fy, provider.name, amount]);
        } catch (e) {
          // Ignore errors
        }
      }
    }
  }
  results.push(`Added ${paymentCount} sample payments`);
  
  // Add some fraud indicators for testing
  const fraudIndicators = [
    { provider_name: 'Rainbow Child Development', indicator_type: 'high_growth', severity: 'medium', description: 'Payment amounts increased 150% in 6 months' },
    { provider_name: 'Growing Minds LLC', indicator_type: 'over_capacity', severity: 'high', description: 'Billing for 52 children but licensed for 40' },
    { provider_name: 'Little Stars Daycare', indicator_type: 'duplicate_payment', severity: 'low', description: 'Potential duplicate payment detected for March 2025' },
  ];
  
  for (const indicator of fraudIndicators) {
    const providerRows = await query('SELECT id FROM providers WHERE name = ?', [indicator.provider_name]);
    if (providerRows.length > 0) {
      try {
        await execute(`
          INSERT INTO fraud_indicators (provider_id, indicator_type, severity, description, status)
          VALUES (?, ?, ?, ?, 'open')
        `, [providerRows[0].id, indicator.indicator_type, indicator.severity, indicator.description]);
      } catch (e) {
        // Ignore errors
      }
    }
  }
  results.push(`Added ${fraudIndicators.length} sample fraud indicators`);
  
  // Get final counts
  const providerCount = await query('SELECT COUNT(*) as count FROM providers');
  const paymentCountFinal = await query('SELECT COUNT(*) as count FROM payments');
  const expenditureCount = await query('SELECT COUNT(*) as count FROM expenditures');
  const fraudCount = await query('SELECT COUNT(*) as count FROM fraud_indicators');
  
  res.json({
    success: true,
    message: 'Sample data seeded successfully',
    results,
    counts: {
      providers: providerCount[0]?.count,
      payments: paymentCountFinal[0]?.count,
      expenditures: expenditureCount[0]?.count,
      fraud_indicators: fraudCount[0]?.count,
    }
  });
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
// Fraud Analysis (protected - can modify database)
app.post('/api/analyze/fraud', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const result = await runFullFraudAnalysis();
  res.json({ success: true, ...result });
}));

app.get('/api/analyze/structuring', requireAuth, asyncHandler(async (req, res) => {
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

app.get('/api/analyze/duplicates', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const duplicates = await detectDuplicates();
  res.json({ duplicates, count: duplicates.length });
}));

app.get('/api/analyze/concentration', requireAuth, asyncHandler(async (req, res) => {
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

// Clean up duplicates (admin endpoint - protected)
app.post('/api/admin/cleanup-duplicates', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  await dbHelpers.cleanupDuplicateDataSources();
  const sources = await dbHelpers.getDataSources();
  res.json({ 
    message: 'Duplicates cleaned up', 
    remaining: sources.length,
    sources 
  });
}));

// Force schema creation and seeding (admin endpoint - protected)
app.post('/api/admin/init-db', requireAuth, asyncHandler(async (req, res) => {
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
      `CREATE TABLE IF NOT EXISTS providers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        dba_name TEXT,
        address TEXT,
        city TEXT,
        state TEXT DEFAULT 'NH',
        zip TEXT,
        phone TEXT,
        email TEXT,
        website TEXT,
        license_number TEXT,
        license_type TEXT,
        license_status TEXT,
        capacity INTEGER,
        age_range TEXT,
        hours_operation TEXT,
        provider_type TEXT,
        is_immigrant_owned INTEGER DEFAULT 0,
        owner_name TEXT,
        owner_background TEXT,
        language_services TEXT,
        accepts_ccdf INTEGER DEFAULT 0,
        ccdf_provider_id TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_id INTEGER REFERENCES providers(id),
        fiscal_year INTEGER,
        fiscal_month INTEGER,
        amount REAL,
        children_served INTEGER,
        payment_type TEXT,
        funding_source TEXT,
        program_type TEXT,
        check_number TEXT,
        payment_date TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS fraud_indicators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_id INTEGER REFERENCES providers(id),
        contract_id INTEGER REFERENCES contracts(id),
        contractor_id INTEGER REFERENCES contractors(id),
        expenditure_id INTEGER REFERENCES expenditures(id),
        payment_id INTEGER REFERENCES payments(id),
        indicator_type TEXT NOT NULL,
        severity TEXT DEFAULT 'medium',
        description TEXT,
        evidence TEXT,
        amount_flagged REAL,
        status TEXT DEFAULT 'open',
        reviewed_by TEXT,
        reviewed_at TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )`,
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
      )`,
      `CREATE TABLE IF NOT EXISTS scrape_jobs (
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

// Force schema creation (admin endpoint - protected)
app.post('/api/admin/create-schema', requireAuth, asyncHandler(async (req, res) => {
  console.log('Manually creating schema...');
  const { executeRaw } = await import('../src/db/db-adapter.js');
  const { postgresSchema } = await import('../src/db/schema-postgres.js');
  
  try {
    await executeRaw(postgresSchema);
    res.json({ message: 'Schema created successfully' });
  } catch (error: any) {
    console.error('Schema creation error:', error);
    res.status(500).json({ error: error.message });
  }
}));

// Debug database info (protected)
app.get('/api/admin/db-info', requireAuth, asyncHandler(async (req, res) => {
  const { IS_TURSO, IS_LOCAL, initDb, query } = await import('../src/db/db-adapter.js');
  
  // Initialize DB first
  await initDb();
  
  const env: any = {
    TURSO_DATABASE_URL: process.env.TURSO_DATABASE_URL ? 'SET' : 'NOT SET',
    TURSO_AUTH_TOKEN: process.env.TURSO_AUTH_TOKEN ? 'SET' : 'NOT SET',
    BLOB_READ_WRITE_TOKEN: process.env.BLOB_READ_WRITE_TOKEN ? 'SET' : 'NOT SET',
    AI_GATEWAY_API_KEY: process.env.AI_GATEWAY_API_KEY ? 'SET' : 'NOT SET',
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
// TRIGGER.DEV JOB ENDPOINTS (protected)
// ============================================

// Trigger TransparentNH Scraper
app.post('/api/trigger/transparent-nh', requireAuth, asyncHandler(async (req, res) => {
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  const recentYears = req.body.recentYears || false;
  const crawl = req.body.crawl || false;
  const crawlIngest = req.body.crawlIngest || false;
  const dryRun = req.body.dryRun || false;

  try {
    const handle = await tasks.trigger<typeof scrapeTransparentNH>('scrape-transparent-nh', {
      fiscalYear: fiscalYear ? parseInt(fiscalYear) : undefined,
      recentYears,
      crawl,
      crawlIngest,
      dryRun,
    });

    res.json({
      success: true,
      runId: handle.id,
      message: crawlIngest
        ? 'TransparentNH crawl+ingest task triggered'
        : (crawl ? 'TransparentNH crawl task triggered' : 'TransparentNH scraper task triggered'),
    });
  } catch (error: any) {
    console.error('Failed to trigger TransparentNH task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Trigger NH DAS Contracts Scraper
app.post('/api/trigger/contracts', requireAuth, asyncHandler(async (req, res) => {
  const keyword = req.body.keyword;
  const fullScrape = req.body.fullScrape || false;

  try {
    const handle = await tasks.trigger<typeof scrapeNHDASContracts>('scrape-nh-das-contracts', {
      keyword,
      fullScrape,
    });

    res.json({
      success: true,
      runId: handle.id,
      message: fullScrape ? 'Full contracts scraper task triggered' : 'Contracts search task triggered',
    });
  } catch (error: any) {
    console.error('Failed to trigger NH DAS Contracts task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Trigger NH Licensing Scraper
app.post('/api/trigger/licensing', requireAuth, asyncHandler(async (req, res) => {
  const forceRefresh = req.body.forceRefresh || false;

  try {
    const handle = await tasks.trigger<typeof scrapeNHLicensing>('scrape-nh-licensing', {
      forceRefresh,
    });

    res.json({
      success: true,
      runId: handle.id,
      message: 'NH Licensing scraper task triggered',
    });
  } catch (error: any) {
    console.error('Failed to trigger NH Licensing task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Trigger Fraud Analysis
app.post('/api/trigger/fraud-analysis', requireAuth, asyncHandler(async (req, res) => {
  const analysisType = req.body.analysisType || 'full';
  const structuringThreshold = req.body.structuringThreshold;
  const topVendors = req.body.topVendors;

  try {
    const handle = await tasks.trigger<typeof runFraudAnalysis>('run-fraud-analysis', {
      analysisType,
      structuringThreshold,
      topVendors,
    });

    res.json({
      success: true,
      runId: handle.id,
      message: `Fraud analysis (${analysisType}) task triggered`,
    });
  } catch (error: any) {
    console.error('Failed to trigger Fraud Analysis task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Trigger USAspending.gov Scraper
app.post('/api/trigger/usaspending', requireAuth, asyncHandler(async (req, res) => {
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  const includeSummary = req.body.includeSummary !== false;

  try {
    const handle = await tasks.trigger<typeof scrapeUSASpendingTask>('scrape-usaspending', {
      fiscalYear: fiscalYear ? parseInt(fiscalYear) : undefined,
      includeSummary,
    });

    res.json({
      success: true,
      runId: handle.id,
      message: 'USAspending.gov scraper task triggered',
    });
  } catch (error: any) {
    console.error('Failed to trigger USAspending task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Trigger ACF CCDF Data Scraper
app.post('/api/trigger/acf-ccdf', requireAuth, asyncHandler(async (req, res) => {
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  const allYears = req.body.allYears || false;

  try {
    const handle = await tasks.trigger<typeof scrapeACFCCDFTask>('scrape-acf-ccdf', {
      fiscalYear: fiscalYear ? parseInt(fiscalYear) : undefined,
      allYears,
    });

    res.json({
      success: true,
      runId: handle.id,
      message: 'ACF CCDF data scraper task triggered',
    });
  } catch (error: any) {
    console.error('Failed to trigger ACF CCDF task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Direct USAspending.gov endpoint (runs synchronously, for testing - protected)
app.post('/api/scraper/usaspending', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  
  const result = await scrapeUSASpending(fiscalYear ? parseInt(fiscalYear) : undefined);
  res.json(result);
}));

// Get USAspending NH state overview
app.get('/api/scraper/usaspending/overview', asyncHandler(async (req, res) => {
  const overview = await getNHStateOverview();
  res.json(overview || { error: 'Could not fetch overview' });
}));

// Direct ACF CCDF endpoint (runs synchronously, for testing - protected)
app.post('/api/scraper/acf-ccdf', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  
  const result = await scrapeACFData(fiscalYear ? parseInt(fiscalYear) : undefined);
  res.json(result);
}));

// Get available ACF fiscal years
app.get('/api/scraper/acf-ccdf/years', asyncHandler(async (req, res) => {
  const years = getACFFiscalYears();
  res.json({ years });
}));

// Get NH CCDF stats directly
app.get('/api/scraper/acf-ccdf/stats/:year', asyncHandler(async (req, res) => {
  const year = parseInt(req.params.year as string);
  const stats = getNHCCDFStats(year);
  
  if (!stats) {
    return res.status(404).json({ error: `No stats available for FY${year}` });
  }
  
  res.json({ fiscalYear: year, ...stats });
}));

// Get Trigger.dev run status
app.get('/api/trigger/runs/:runId', requireAuth, asyncHandler(async (req, res) => {
  const runId = req.params.runId as string;

  try {
    const run = await runs.retrieve(runId);

    res.json({
      id: run.id,
      status: run.status,
      taskIdentifier: run.taskIdentifier,
      createdAt: run.createdAt,
      updatedAt: run.updatedAt,
      startedAt: run.startedAt,
      finishedAt: run.finishedAt,
      output: run.output,
      error: run.status === 'FAILED' ? run.error : undefined,
    });
  } catch (error: any) {
    console.error('Failed to retrieve run status:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// List recent Trigger.dev runs
app.get('/api/trigger/runs', requireAuth, asyncHandler(async (req, res) => {
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 20;
  const taskId = req.query.taskId as string | undefined;

  try {
    const runsList = await runs.list({
      limit,
      taskIdentifier: taskId ? [taskId] : undefined,
    });

    res.json({
      runs: runsList.data.map(run => ({
        id: run.id,
        status: run.status,
        taskIdentifier: run.taskIdentifier,
        createdAt: run.createdAt,
        finishedAt: run.finishedAt,
      })),
    });
  } catch (error: any) {
    console.error('Failed to list runs:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}))

// ============================================
// INGESTION STATUS ENDPOINTS
// ============================================

// Get ingestion run history from database
app.get('/api/ingestion/runs', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 50;
  const source = req.query.source as string | undefined;

  try {
    const sourceClause = source ? 'WHERE source = ?' : '';
    const params = source ? [source, limit] : [limit];

    const ingestionRuns = await query(`
      SELECT id, source, status, started_at, completed_at,
             records_processed, records_imported, details, error_message
      FROM ingestion_runs
      ${sourceClause}
      ORDER BY started_at DESC
      LIMIT ?
    `, params);

    // Get summary by source
    const summary = await query(`
      SELECT source,
             COUNT(*) as total_runs,
             SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_runs,
             SUM(records_imported) as total_imported,
             MAX(completed_at) as last_run
      FROM ingestion_runs
      GROUP BY source
      ORDER BY last_run DESC
    `);

    res.json({
      success: true,
      runs: ingestionRuns,
      summary,
      totalRuns: ingestionRuns.length
    });
  } catch (error: any) {
    console.error('Failed to get ingestion runs:', error);
    res.status(500).json({ success: false, error: error.message });
  }
}));

// Get scheduled task overview
app.get('/api/ingestion/schedule', requireAuth, asyncHandler(async (req, res) => {
  const scheduleOverview = {
    tasks: [
      { id: 'daily-usaspending-ingest', source: 'USAspending.gov', schedule: 'Daily 6 AM UTC', description: 'Federal CCDF awards' },
      { id: 'weekly-acf-ccdf-ingest', source: 'ACF CCDF', schedule: 'Monday 4 AM UTC', description: 'HHS expenditure statistics' },
      { id: 'weekly-propublica-990-ingest', source: 'ProPublica 990', schedule: 'Tuesday 4 AM UTC', description: 'Nonprofit IRS filings' },
      { id: 'weekly-census-saipe-ingest', source: 'Census SAIPE', schedule: 'Wednesday 4 AM UTC', description: 'County poverty data' },
      { id: 'weekly-datagov-ccdf-ingest', source: 'Data.gov CCDF', schedule: 'Thursday 4 AM UTC', description: 'CCDF administrative data' },
      { id: 'weekly-fraud-analysis', source: 'Fraud Analysis', schedule: 'Monday 8 AM UTC', description: 'Pattern detection' },
      { id: 'weekly-full-refresh', source: 'Full Refresh', schedule: 'Sunday 2 AM UTC', description: 'Complete data sync' },
      { id: 'monthly-data-quality-check', source: 'Data Quality', schedule: '1st of month 3 AM UTC', description: 'Integrity validation' },
    ],
    dataSources: [
      { name: 'USAspending.gov', type: 'API', status: 'active', dataType: 'Federal CCDF awards' },
      { name: 'ACF.hhs.gov', type: 'API/Fallback', status: 'active', dataType: 'HHS statistics' },
      { name: 'ProPublica Nonprofit Explorer', type: 'API', status: 'active', dataType: 'IRS 990 filings' },
      { name: 'Census SAIPE', type: 'API', status: 'active', dataType: 'Poverty/income data' },
      { name: 'Data.gov CCDF', type: 'Bulk/Fallback', status: 'active', dataType: 'CCDF admin data' },
      { name: 'TransparentNH', type: 'Web Scraper', status: 'blocked', dataType: 'State expenditures' },
      { name: 'NH DAS Contracts', type: 'Web Scraper', status: 'blocked', dataType: 'State contracts' },
    ]
  };

  res.json({ success: true, ...scheduleOverview });
}));

// ============================================
// FEDERAL DATA ENDPOINTS (Public - real data!)
// ============================================

// Get federal CCDF awards from USAspending.gov (cached in DB + live fetch option)
app.get('/api/federal/awards', asyncHandler(async (req, res) => {
  await ensureInitialized();
  const refresh = req.query.refresh === 'true';
  const fiscalYear = req.query.fiscal_year ? parseInt(req.query.fiscal_year as string) : undefined;
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 50;

  if (refresh) {
    const expectedKey = process.env.ADMIN_API_KEY;
    if (expectedKey) {
      const apiKey = req.headers['x-api-key'] as string;
      if (!apiKey) {
        return res.status(401).json({
          error: 'Unauthorized',
          message: 'Missing x-api-key header'
        });
      }
      if (apiKey !== expectedKey) {
        return res.status(401).json({
          error: 'Unauthorized',
          message: 'Invalid API key'
        });
      }
    }
  }
  
  // Check if we have recent data in the database
  let awards = await query(`
    SELECT * FROM expenditures 
    WHERE source_url LIKE 'USAspending:%' 
    ${fiscalYear ? 'AND fiscal_year = ?' : ''}
    ORDER BY amount DESC 
    LIMIT ?
  `, fiscalYear ? [fiscalYear, limit] : [limit]);
  
  // If no data or refresh requested, fetch from API
  if (awards.length === 0 || refresh) {
    try {
      const result = await scrapeUSASpending(fiscalYear);
      if (result.success && result.awards) {
        // Return fresh data
        res.json({
          source: 'USAspending.gov (live)',
          cached: false,
          totalAwards: result.totalAwards,
          totalAmount: result.totalAmount,
          fiscalYears: result.fiscalYears,
          importedRecords: result.importedRecords,
          awards: result.awards.slice(0, limit),
        });
        return;
      }
    } catch (error) {
      console.error('Error fetching live USAspending data:', error);
      // Fall through to return cached data if available
    }
  }
  
  // Return cached data from database
  const totalResult = await query(`
    SELECT 
      COUNT(*) as count,
      SUM(amount) as total,
      GROUP_CONCAT(DISTINCT fiscal_year) as years
    FROM expenditures 
    WHERE source_url LIKE 'USAspending:%'
    ${fiscalYear ? 'AND fiscal_year = ?' : ''}
  `, fiscalYear ? [fiscalYear] : []);
  
  const stats = totalResult[0] || { count: 0, total: 0, years: '' };
  
  res.json({
    source: 'USAspending.gov (cached)',
    cached: true,
    totalAwards: stats.count || 0,
    totalAmount: stats.total || 0,
    fiscalYears: stats.years ? stats.years.split(',').map(Number).sort((a: number, b: number) => b - a) : [],
    awards: awards.map((a: any) => ({
      awardId: a.source_url?.replace('USAspending:', '') || 'Unknown',
      recipient: a.vendor_name,
      amount: a.amount,
      fiscalYear: a.fiscal_year,
      description: a.description,
      agency: a.agency,
      activity: a.activity,
    })),
  });
}));

// Get federal funding summary for dashboard
app.get('/api/federal/summary', asyncHandler(async (req, res) => {
  await ensureInitialized();
  
  // Get summary from database
  const dbSummary = await query(`
    SELECT 
      fiscal_year,
      COUNT(*) as award_count,
      SUM(amount) as total_amount
    FROM expenditures 
    WHERE source_url LIKE 'USAspending:%'
    GROUP BY fiscal_year
    ORDER BY fiscal_year DESC
  `);
  
  // Calculate totals
  const totalAmount = dbSummary.reduce((sum: number, row: any) => sum + (row.total_amount || 0), 0);
  const totalAwards = dbSummary.reduce((sum: number, row: any) => sum + (row.award_count || 0), 0);
  
  // Try to get live summary if no cached data
  let liveData: { statePopulation: unknown; totalFederalAmount: unknown } | null = null;
  if (totalAwards === 0) {
    try {
      const overview = await getNHStateOverview();
      if (overview) {
        liveData = {
          statePopulation: (overview as any).population,
          totalFederalAmount: (overview as any).total_face_value_prime_awards,
        };
      }
    } catch (error) {
      console.error('Error fetching NH overview:', error);
    }
  }
  
  res.json({
    hasCachedData: totalAwards > 0,
    totalFederalAmount: totalAmount,
    totalAwards,
    byFiscalYear: dbSummary,
    recentFiscalYear: dbSummary.length > 0 ? dbSummary[0].fiscal_year : null,
    recentYearAmount: dbSummary.length > 0 ? dbSummary[0].total_amount : 0,
    liveData,
    lastUpdated: new Date().toISOString(),
    ccdfPrograms: [
      { cfda: '93.575', name: 'Child Care and Development Block Grant' },
      { cfda: '93.596', name: 'Child Care Mandatory and Matching Funds' },
    ],
  });
}));

// Refresh federal data (protected - triggers API fetch and DB update)
app.post('/api/federal/refresh', requireAuth, asyncHandler(async (req, res) => {
  await ensureInitialized();
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  
  try {
    const result = await scrapeUSASpending(fiscalYear ? parseInt(fiscalYear) : undefined);
    res.json({
      success: result.success,
      message: result.success ? 'Federal data refreshed successfully' : 'Failed to refresh data',
      totalAwards: result.totalAwards,
      totalAmount: result.totalAmount,
      importedRecords: result.importedRecords,
      fiscalYears: result.fiscalYears,
      error: result.error,
    });
  } catch (error: any) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
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

// Catch-all 404 handler for /api/*
app.use('/api', (req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: `API route not found: ${req.method} ${req.originalUrl}`,
    url: req.url,
    path: req.path
  });
});

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
