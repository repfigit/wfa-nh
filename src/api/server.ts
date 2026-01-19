import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { initializeDb, dbHelpers, getDb, saveDb } from '../db/database.js';
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
} from '../scraper/transparent-nh-scraper.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' })); // Increase limit for CSV uploads
app.use(express.text({ limit: '50mb', type: 'text/csv' }));
app.use(express.static(path.join(__dirname, '../../public')));

// Error handler
const asyncHandler = (fn: (req: Request, res: Response, next: NextFunction) => Promise<any>) => 
  (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };

// API Routes

// Dashboard stats
app.get('/api/dashboard', asyncHandler(async (req, res) => {
  const stats = await dbHelpers.getDashboardStats();
  res.json(stats);
}));

// Providers (Daycares)
app.get('/api/providers', asyncHandler(async (req, res) => {
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
  const id = parseInt(req.params.id as string);
  const provider = await dbHelpers.getProviderWithPayments(id);
  if (!provider) {
    return res.status(404).json({ error: 'Provider not found' });
  }
  res.json(provider);
}));

app.get('/api/providers/:id/payments', asyncHandler(async (req, res) => {
  const id = parseInt(req.params.id as string);
  const payments = await dbHelpers.getAllPayments({ provider_id: id });
  res.json(payments);
}));

// Payments
app.get('/api/payments', asyncHandler(async (req, res) => {
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
  const filters = {
    status: req.query.status as string | undefined,
    severity: req.query.severity as string | undefined,
    provider_id: req.query.provider_id ? parseInt(req.query.provider_id as string) : undefined,
  };
  const indicators = await dbHelpers.getFraudIndicators(filters);
  res.json(indicators);
}));

app.patch('/api/fraud-indicators/:id', asyncHandler(async (req, res) => {
  const { status, notes } = req.body;
  const id = parseInt(req.params.id as string);
  
  const db = await getDb();
  
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
  
  const sql = `UPDATE fraud_indicators SET ${updateParts.join(', ')} WHERE id = ?`;
  db.run(sql, params);
  await saveDb();
  
  const stmt = db.prepare('SELECT * FROM fraud_indicators WHERE id = ?');
  stmt.bind([id]);
  let result = null;
  if (stmt.step()) {
    result = stmt.getAsObject();
  }
  stmt.free();
  
  if (!result) {
    return res.status(404).json({ error: 'Fraud indicator not found' });
  }
  
  res.json(result);
}));

// ============================================
// NEW: CSV Import Endpoints
// ============================================

// Upload and import CSV from TransparentNH
app.post('/api/import/csv', asyncHandler(async (req, res) => {
  const csvContent = req.body.csv || req.body;
  const filterChildcare = req.query.filter_childcare === 'true';
  const createProviders = req.query.create_providers === 'true';
  
  if (!csvContent || typeof csvContent !== 'string') {
    return res.status(400).json({ error: 'CSV content required in request body' });
  }
  
  console.log(`Importing CSV (${csvContent.length} bytes), filterChildcare=${filterChildcare}`);
  
  const result = await importCSV(csvContent, {
    filterChildcare,
    createProviders,
  });
  
  res.json(result);
}));

// Get import statistics
app.get('/api/import/stats', asyncHandler(async (req, res) => {
  const stats = await getImportStats();
  res.json(stats);
}));

// Get expenditures (imported data)
app.get('/api/expenditures', asyncHandler(async (req, res) => {
  const db = await getDb();
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 100;
  const fiscal_year = req.query.fiscal_year ? parseInt(req.query.fiscal_year as string) : undefined;
  
  let sql = `
    SELECT e.*, p.name as provider_name
    FROM expenditures e
    LEFT JOIN providers p ON e.provider_id = p.id
    WHERE 1=1
  `;
  const params: any[] = [];
  
  if (fiscal_year) {
    sql += ' AND e.fiscal_year = ?';
    params.push(fiscal_year);
  }
  
  sql += ' ORDER BY e.amount DESC LIMIT ?';
  params.push(limit);
  
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const results: any[] = [];
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  
  res.json(results);
}));

// ============================================
// NEW: Scraper Endpoints
// ============================================

// Search NH DAS contracts
app.post('/api/scraper/search', asyncHandler(async (req, res) => {
  const keyword = req.body.keyword || 'daycare';
  
  console.log(`Scraping NH DAS for keyword: ${keyword}`);
  
  const result = await searchContracts(keyword);
  
  if (result.success && result.contracts.length > 0) {
    const saved = await saveScrapedContracts(result.contracts);
    res.json({
      ...result,
      saved,
    });
  } else {
    res.json(result);
  }
}));

// Run full scrape for all childcare keywords
app.post('/api/scraper/full', asyncHandler(async (req, res) => {
  console.log('Starting full childcare contracts scrape...');
  
  const result = await scrapeAllChildcareContracts();
  
  res.json({
    success: result.errors.length === 0,
    keywords_searched: CHILDCARE_KEYWORDS.length,
    ...result,
  });
}));

// ============================================
// TransparentNH Scraper Endpoints
// ============================================

// Get available fiscal years
app.get('/api/scraper/transparent-nh/years', asyncHandler(async (req, res) => {
  const years = getAvailableFiscalYears();
  res.json({ years });
}));

// Scrape a specific fiscal year
app.post('/api/scraper/transparent-nh', asyncHandler(async (req, res) => {
  const fiscalYear = req.body.fiscalYear || req.body.fiscal_year;
  
  if (!fiscalYear) {
    return res.status(400).json({ error: 'fiscalYear is required' });
  }
  
  console.log(`Scraping TransparentNH FY${fiscalYear}...`);
  
  const result = await scrapeFiscalYear(fiscalYear);
  res.json(result);
}));

// Scrape recent fiscal years (current and previous 2)
app.post('/api/scraper/transparent-nh/recent', asyncHandler(async (req, res) => {
  console.log('Scraping recent TransparentNH fiscal years...');
  
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
  const db = await getDb();
  const limit = req.query.limit ? parseInt(req.query.limit as string) : 50;
  
  const stmt = db.prepare(`
    SELECT c.*, co.name as contractor_name
    FROM contracts c
    LEFT JOIN contractors co ON c.contractor_id = co.id
    ORDER BY c.created_at DESC
    LIMIT ?
  `);
  stmt.bind([limit]);
  
  const results: any[] = [];
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  
  res.json(results);
}));

// ============================================
// NEW: Fraud Analysis Endpoints
// ============================================

// Run full fraud analysis
app.post('/api/analyze/fraud', asyncHandler(async (req, res) => {
  console.log('Running full fraud analysis...');
  
  const result = await runFullFraudAnalysis();
  
  res.json({
    success: true,
    ...result,
  });
}));

// Detect structuring patterns
app.get('/api/analyze/structuring', asyncHandler(async (req, res) => {
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

// Detect duplicate transactions
app.get('/api/analyze/duplicates', asyncHandler(async (req, res) => {
  const duplicates = await detectDuplicates();
  
  res.json({
    duplicates,
    count: duplicates.length,
  });
}));

// Get vendor concentration analysis
app.get('/api/analyze/concentration', asyncHandler(async (req, res) => {
  const topN = req.query.top_n ? parseInt(req.query.top_n as string) : 10;
  
  const concentration = await analyzeVendorConcentration(topN);
  
  res.json({
    top_n: topN,
    vendors: concentration,
  });
}));

// ============================================
// Reports and Data Sources
// ============================================

// Reports
app.get('/api/reports/summary', asyncHandler(async (req, res) => {
  const report = await dbHelpers.getReportSummary();
  res.json(report);
}));

// Data Sources
app.get('/api/data-sources', asyncHandler(async (req, res) => {
  const sources = await dbHelpers.getDataSources();
  res.json(sources);
}));

// Search across all entities
app.get('/api/search', asyncHandler(async (req, res) => {
  const query = req.query.q as string;
  if (!query) {
    return res.status(400).json({ error: 'Query parameter "q" is required' });
  }
  
  const db = await getDb();
  const searchTerm = `%${query}%`;
  
  const runSearch = (sql: string, params: any[]) => {
    const stmt = db.prepare(sql);
    stmt.bind(params);
    const results: any[] = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject());
    }
    stmt.free();
    return results;
  };
  
  const providers = runSearch(`
    SELECT id, name, 'provider' as type FROM providers 
    WHERE name LIKE ? OR owner_name LIKE ? OR notes LIKE ?
    LIMIT 10
  `, [searchTerm, searchTerm, searchTerm]);
  
  const expenditures = runSearch(`
    SELECT id, vendor_name as name, 'expenditure' as type FROM expenditures
    WHERE vendor_name LIKE ? OR description LIKE ?
    LIMIT 10
  `, [searchTerm, searchTerm]);
  
  const fraudIndicators = runSearch(`
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

// Serve frontend for all other routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../../public/index.html'));
});

export async function startServer() {
  // Initialize database
  await initializeDb();
  
  app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
    console.log(`API available at http://localhost:${PORT}/api`);
    console.log('\nNew endpoints:');
    console.log('  POST /api/import/csv - Import TransparentNH CSV');
    console.log('  POST /api/scraper/search - Search NH DAS contracts');
    console.log('  POST /api/scraper/full - Full childcare scrape');
    console.log('  GET  /api/scraper/transparent-nh/years - Get available fiscal years');
    console.log('  POST /api/scraper/transparent-nh - Scrape specific fiscal year');
    console.log('  POST /api/scraper/transparent-nh/recent - Scrape recent years');
    console.log('  POST /api/analyze/fraud - Run fraud analysis');
    console.log('  GET  /api/analyze/structuring - Detect structuring');
    console.log('  GET  /api/analyze/duplicates - Find duplicates');
  });
}

export default app;
