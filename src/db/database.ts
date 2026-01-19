/**
 * Database Module
 * Uses PostgreSQL in production (Vercel) and SQLite for local development
 */

import { 
  IS_TURSO, 
  IS_LOCAL, 
  initDb, 
  getSqliteDb, 
  saveSqliteDb, 
  closeDb as closeAdapter,
  query,
  execute,
  executeRaw,
} from './db-adapter.js';
import { schema, seedData } from './schema.js';
import { postgresSchema, postgresDataSources, postgresKeywords } from './schema-postgres.js';

// Re-export for backward compatibility
export { IS_TURSO, IS_LOCAL };

let initialized = false;
let seeded = false;

/**
 * Initialize the database with schema
 */
export async function initializeDb(): Promise<void> {
  if (initialized) return;
  
  await initDb();
  
  if (IS_TURSO()) {
    // Initialize Turso with PostgreSQL schemas
    console.log('Initializing Turso schema...');
    try {
      await executeRaw(postgresSchema);
      console.log('Turso schema executed successfully');
    } catch (error) {
      console.error('Error executing Turso schema:', error);
      throw error;
    }
    
    // Check if database is already seeded
    const existingSources = await query('SELECT COUNT(*) as count FROM data_sources');
    const count = existingSources[0]?.count || 0;
    
    if (count === 0) {
      console.log('Seeding Turso database...');
      await executeRaw(postgresDataSources);
      await executeRaw(postgresKeywords);
    } else {
      console.log(`Turso database already seeded with ${count} data sources`);
    }
  } else {
    // Initialize local SQLite schema
    console.log('Initializing SQLite schema...');
    const db = await getSqliteDb();
    db.run(schema);
    
    // Check if database is already seeded
    const existingSources = db.exec('SELECT COUNT(*) as count FROM data_sources');
    const count = existingSources[0]?.values[0]?.[0] || 0;
    
    if (count === 0) {
      console.log('Seeding database...');
      db.run(seedData);
      seeded = true;
    } else {
      console.log(`Database already seeded with ${count} data sources`);
    }
    
    await saveSqliteDb();
  }
  
  initialized = true;
  console.log('Database initialized successfully');
}

/**
 * Get raw database access (for backward compatibility)
 * Note: Prefer using query() and execute() from db-adapter
 */
export async function getDb() {
  if (IS_TURSO()) {
    // Return a wrapper that mimics sql.js interface for PostgreSQL
    return {
      run: async (sqlQuery: string, params: any[] = []) => {
        await execute(sqlQuery, params);
      },
      exec: async (sqlQuery: string) => {
        const result = await query(sqlQuery);
        return [{ values: result.map(r => Object.values(r)) }];
      },
      prepare: (sqlQuery: string) => {
        // Return a statement-like object
        let boundParams: any[] = [];
        let results: any[] | null = null;
        let currentIndex = 0;
        
        return {
          bind: (params: any[]) => { boundParams = params; },
          step: () => {
            if (results === null) {
              // This is a sync operation, which won't work well with async PG
              // For now, we'll handle this in the calling code
              return false;
            }
            return currentIndex < results.length;
          },
          getAsObject: () => results?.[currentIndex++] ?? {},
          free: () => { results = null; },
        };
      },
    };
  } else {
    return getSqliteDb();
  }
}

/**
 * Save database (SQLite only)
 */
export async function saveDb(): Promise<void> {
  if (IS_LOCAL()) {
    await saveSqliteDb();
  }
}

/**
 * Close database connection
 */
export function closeDb(): void {
  closeAdapter();
  initialized = false;
}

// ============================================
// Database Helpers
// ============================================

export const dbHelpers = {
  // Providers (Daycares)
  getAllProviders: async (immigrantOwnedOnly = false): Promise<any[]> => {
    await initializeDb();
    const sql = immigrantOwnedOnly 
      ? 'SELECT * FROM providers WHERE is_immigrant_owned = 1 ORDER BY name'
      : 'SELECT * FROM providers ORDER BY name';
    return query(sql);
  },

  getProviderById: async (id: number): Promise<any | null> => {
    await initializeDb();
    const results = await query('SELECT * FROM providers WHERE id = ?', [id]);
    return results[0] || null;
  },

  getProviderWithPayments: async (id: number): Promise<any | null> => {
    await initializeDb();
    const providers = await query('SELECT * FROM providers WHERE id = ?', [id]);
    const provider = providers[0];
    if (!provider) return null;
    
    const payments = await query(`
      SELECT * FROM payments WHERE provider_id = ? ORDER BY fiscal_year DESC, fiscal_month DESC
    `, [id]);
    
    const fraudIndicators = await query(`
      SELECT * FROM fraud_indicators WHERE provider_id = ? ORDER BY severity DESC
    `, [id]);
    
    return { ...provider, payments, fraud_indicators: fraudIndicators };
  },

  // Payments
  getAllPayments: async (filters?: { provider_id?: number; fiscal_year?: number; limit?: number }): Promise<any[]> => {
    await initializeDb();
    let sql = `
      SELECT p.*, pr.name as provider_name, pr.is_immigrant_owned
      FROM payments p
      JOIN providers pr ON p.provider_id = pr.id
      WHERE 1=1
    `;
    const params: any[] = [];
    
    if (filters?.provider_id) {
      sql += ' AND p.provider_id = ?';
      params.push(filters.provider_id);
    }
    if (filters?.fiscal_year) {
      sql += ' AND p.fiscal_year = ?';
      params.push(filters.fiscal_year);
    }
    sql += ' ORDER BY p.amount DESC';
    if (filters?.limit) {
      sql += ' LIMIT ?';
      params.push(filters.limit);
    }
    
    return query(sql, params);
  },

  // Fraud Indicators
  getFraudIndicators: async (filters?: { status?: string; severity?: string; provider_id?: number }): Promise<any[]> => {
    await initializeDb();
    let sql = `
      SELECT fi.*, p.name as provider_name
      FROM fraud_indicators fi
      LEFT JOIN providers p ON fi.provider_id = p.id
      WHERE 1=1
    `;
    const params: any[] = [];

    if (filters?.status) {
      sql += ' AND fi.status = ?';
      params.push(filters.status);
    }
    if (filters?.severity) {
      sql += ' AND fi.severity = ?';
      params.push(filters.severity);
    }
    if (filters?.provider_id) {
      sql += ' AND fi.provider_id = ?';
      params.push(filters.provider_id);
    }

    // Use CASE for cross-database compatible ordering
    sql += ` ORDER BY CASE fi.severity 
      WHEN 'critical' THEN 1 
      WHEN 'high' THEN 2 
      WHEN 'medium' THEN 3 
      ELSE 4 END, fi.created_at DESC`;
    
    return query(sql, params);
  },

  // Dashboard Stats
  getDashboardStats: async () => {
    await initializeDb();
    
    const totalProviders = (await query('SELECT COUNT(*) as count FROM providers'))[0]?.count || 0;
    const immigrantProviders = (await query('SELECT COUNT(*) as count FROM providers WHERE is_immigrant_owned = 1'))[0]?.count || 0;
    const totalPayments = (await query('SELECT COALESCE(SUM(amount), 0) as total FROM payments'))[0]?.total || 0;
    const paymentCount = (await query('SELECT COUNT(*) as count FROM payments'))[0]?.count || 0;
    
    const fraudBySeverity = await query(`
      SELECT severity, COUNT(*) as count 
      FROM fraud_indicators 
      WHERE status != 'dismissed'
      GROUP BY severity
    `);

    const topProviders = await query(`
      SELECT p.*, 
        SUM(pay.amount) as total_payments,
        COUNT(pay.id) as payment_count,
        MAX(pay.children_served) as max_children,
        (SELECT COUNT(*) FROM fraud_indicators WHERE provider_id = p.id AND status != 'dismissed') as fraud_count
      FROM providers p
      LEFT JOIN payments pay ON p.id = pay.provider_id
      GROUP BY p.id
      ORDER BY total_payments DESC
      LIMIT 10
    `);

    const recentPayments = await query(`
      SELECT pay.*, p.name as provider_name, p.is_immigrant_owned, p.capacity
      FROM payments pay
      JOIN providers p ON pay.provider_id = p.id
      ORDER BY pay.created_at DESC
      LIMIT 10
    `);

    const paymentsByMonth = await query(`
      SELECT fiscal_year, fiscal_month, SUM(amount) as total, COUNT(*) as count
      FROM payments
      GROUP BY fiscal_year, fiscal_month
      ORDER BY fiscal_year, fiscal_month
    `);

    // Convert string counts to numbers (PostgreSQL returns strings for COUNT)
    const toNum = (v: any) => typeof v === 'string' ? parseInt(v, 10) : (v || 0);

    return {
      total_providers: toNum(totalProviders),
      immigrant_providers: toNum(immigrantProviders),
      total_payments: parseFloat(totalPayments) || 0,
      payment_count: toNum(paymentCount),
      fraud_indicators_count: fraudBySeverity.reduce((sum, row) => sum + toNum(row.count), 0),
      fraud_indicators_by_severity: {
        low: toNum(fraudBySeverity.find(r => r.severity === 'low')?.count),
        medium: toNum(fraudBySeverity.find(r => r.severity === 'medium')?.count),
        high: toNum(fraudBySeverity.find(r => r.severity === 'high')?.count),
        critical: toNum(fraudBySeverity.find(r => r.severity === 'critical')?.count),
      },
      top_providers: topProviders,
      recent_payments: recentPayments,
      payments_by_month: paymentsByMonth,
    };
  },

  // Search
  searchProviders: async (searchQuery: string): Promise<any[]> => {
    await initializeDb();
    const searchTerm = `%${searchQuery}%`;
    return query(`
      SELECT p.*, 
        (SELECT SUM(amount) FROM payments WHERE provider_id = p.id) as total_payments,
        (SELECT COUNT(*) FROM fraud_indicators WHERE provider_id = p.id) as fraud_count
      FROM providers p
      WHERE p.name LIKE ? OR p.city LIKE ? OR p.owner_name LIKE ? OR p.notes LIKE ?
      ORDER BY total_payments DESC
    `, [searchTerm, searchTerm, searchTerm, searchTerm]);
  },

  // Reports
  getReportSummary: async () => {
    await initializeDb();
    
    const byCity = await query(`
      SELECT p.city, COUNT(DISTINCT p.id) as provider_count, 
        SUM(pay.amount) as total_payments,
        SUM(CASE WHEN p.is_immigrant_owned = 1 THEN 1 ELSE 0 END) as immigrant_count
      FROM providers p
      LEFT JOIN payments pay ON p.id = pay.provider_id
      WHERE p.city IS NOT NULL
      GROUP BY p.city
      ORDER BY total_payments DESC
    `);

    const byProviderType = await query(`
      SELECT p.provider_type, COUNT(DISTINCT p.id) as count, SUM(pay.amount) as total
      FROM providers p
      LEFT JOIN payments pay ON p.id = pay.provider_id
      WHERE p.provider_type IS NOT NULL
      GROUP BY p.provider_type
      ORDER BY total DESC
    `);

    const fraudByType = await query(`
      SELECT indicator_type, COUNT(*) as count
      FROM fraud_indicators
      WHERE status != 'dismissed'
      GROUP BY indicator_type
      ORDER BY count DESC
    `);

    const overCapacityProviders = await query(`
      SELECT p.name, p.capacity, MAX(pay.children_served) as max_children,
        SUM(pay.amount) as total_payments
      FROM providers p
      JOIN payments pay ON p.id = pay.provider_id
      WHERE p.capacity IS NOT NULL
      GROUP BY p.id, p.name, p.capacity
      HAVING MAX(pay.children_served) > p.capacity
      ORDER BY (MAX(pay.children_served) - p.capacity) DESC
    `);

    return {
      by_city: byCity,
      by_provider_type: byProviderType,
      fraud_by_type: fraudByType,
      over_capacity_providers: overCapacityProviders,
    };
  },

  // Data Sources
  getDataSources: async (): Promise<any[]> => {
    await initializeDb();
    return query('SELECT * FROM data_sources');
  },

  // Clean up duplicate data sources (one-time migration)
  cleanupDuplicateDataSources: async (): Promise<void> => {
    await initializeDb();
    console.log('Cleaning up duplicate data sources...');
    
    if (IS_TURSO()) {
      // For PostgreSQL, use a more complex query to keep one of each unique combination
      await execute(`
        DELETE FROM data_sources 
        WHERE id NOT IN (
          SELECT MIN(id) 
          FROM data_sources 
          GROUP BY name, url, type, notes
        )
      `);
    } else {
      // For SQLite
      const db = await getSqliteDb();
      db.run(`
        DELETE FROM data_sources 
        WHERE id NOT IN (
          SELECT MIN(id) 
          FROM data_sources 
          GROUP BY name, url, type, notes
        )
      `);
      await saveSqliteDb();
    }
    
    console.log('Duplicate cleanup completed');
  },
};

export default { 
  getDb, 
  initializeDb, 
  closeDb, 
  saveDb, 
  dbHelpers,
  IS_TURSO,
  IS_LOCAL,
};
