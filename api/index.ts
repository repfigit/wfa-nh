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
  const { initDb, query } = await import('../src/db/db-adapter.js');
  const { postgresSchema, postgresDataSources, postgresKeywords } = await import('../src/db/schema-postgres.js');
  const { createClient } = await import('@libsql/client');
  
  try {
    // Connect directly to Turso
    const client = createClient({
      url: process.env.TURSO_DATABASE_URL!,
      authToken: process.env.TURSO_AUTH_TOKEN,
    });
    
    // Split schema into individual statements
    const schemaStatements = postgresSchema
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0 && !s.startsWith('--'));
    
    console.log(`Executing ${schemaStatements.length} schema statements...`);
    
    const results: string[] = [];
    for (let i = 0; i < schemaStatements.length; i++) {
      const stmt = schemaStatements[i];
      try {
        await client.execute(stmt);
        results.push(`Statement ${i + 1}: OK`);
      } catch (err: any) {
        results.push(`Statement ${i + 1}: ERROR - ${err.message}`);
      }
    }
    
    // Check tables now
    const tables = await client.execute("SELECT name FROM sqlite_master WHERE type='table'");
    
    // Seed data sources if table exists and is empty
    let dsCount = 0;
    try {
      const countResult = await client.execute('SELECT COUNT(*) as count FROM data_sources');
      dsCount = Number(countResult.rows[0]?.count) || 0;
    } catch (e) {
      // Ignore
    }
    
    if (dsCount === 0) {
      console.log('Seeding data sources...');
      const seedStatements = postgresDataSources
        .split(';')
        .map(s => s.trim())
        .filter(s => s.length > 0 && !s.startsWith('--'));
      
      for (const stmt of seedStatements) {
        try {
          await client.execute(stmt);
          results.push('Data sources seeded: OK');
        } catch (err: any) {
          results.push(`Data sources seed: ERROR - ${err.message}`);
        }
      }
      
      // Keywords
      const keywordStatements = postgresKeywords
        .split(';')
        .map(s => s.trim())
        .filter(s => s.length > 0 && !s.startsWith('--'));
      
      for (const stmt of keywordStatements) {
        try {
          await client.execute(stmt);
          results.push('Keywords seeded: OK');
        } catch (err: any) {
          results.push(`Keywords seed: ERROR - ${err.message}`);
        }
      }
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
