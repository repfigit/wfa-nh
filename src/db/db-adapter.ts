/**
 * Database Adapter
 * Uses Turso (libSQL) by default if available, falls back to SQLite locally
 * Since Turso is SQLite-compatible, we use the same SQL syntax everywhere
 */

import { createClient, Client } from '@libsql/client';
import initSqlJs, { Database as SqlJsDatabase } from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, '../../data/childcare.db');

// Detect environment - prefer Turso if URL is set, fallback to SQLite
let useTurso = false;

// For backward compatibility
export const IS_TURSO = () => useTurso;
export const IS_LOCAL = () => !useTurso;

// Database instances
let tursoClient: Client | null = null;
let sqliteDb: SqlJsDatabase | null = null;
let SQL: any = null;

/**
 * Initialize the database connection
 */
export async function initDb(): Promise<void> {
  // Try Turso first if URL is set
  if (process.env.TURSO_DATABASE_URL) {
    try {
      console.log('Attempting to use Turso...');
      if (!tursoClient) {
        tursoClient = createClient({
          url: process.env.TURSO_DATABASE_URL!,
          authToken: process.env.TURSO_AUTH_TOKEN,
        });
      }
      // Test the connection with a simple query
      await tursoClient.execute('SELECT 1');
      useTurso = true;
      console.log('Using Turso successfully');
      return;
    } catch (error) {
      console.warn('Turso connection failed, falling back to SQLite:', error);
    }
  }
  
  // Fallback to SQLite
  console.log('Using SQLite');
  if (!SQL) {
    SQL = await initSqlJs();
  }
  
  if (!sqliteDb) {
    try {
      if (fs.existsSync(DB_PATH)) {
        const fileBuffer = fs.readFileSync(DB_PATH);
        sqliteDb = new SQL.Database(fileBuffer);
      } else {
        sqliteDb = new SQL.Database();
      }
    } catch (err) {
      sqliteDb = new SQL.Database();
    }
  }
}

/**
 * Get SQLite database instance (local only)
 */
export async function getSqliteDb(): Promise<SqlJsDatabase> {
  if (!sqliteDb) {
    await initDb();
  }
  return sqliteDb!;
}

/**
 * Save SQLite database to file (local only)
 */
export async function saveSqliteDb(): Promise<void> {
  if (useTurso || !sqliteDb) return;
  
  const data = sqliteDb.export();
  const buffer = Buffer.from(data);
  
  const dataDir = path.dirname(DB_PATH);
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  
  fs.writeFileSync(DB_PATH, buffer);
}

/**
 * Close database connection
 */
export function closeDb(): void {
  if (sqliteDb) {
    sqliteDb.close();
    sqliteDb = null;
  }
  // Turso client doesn't need explicit closing
}

/**
 * Execute a query and return results as objects
 */
export async function query<T = any>(
  sql: string,
  params: any[] = []
): Promise<T[]> {
  if (useTurso) {
    const result = await tursoClient!.execute({
      sql,
      args: params,
    });
    return result.rows as T[];
  } else {
    const db = await getSqliteDb();
    const stmt = db.prepare(sql);
    if (params.length > 0) {
      stmt.bind(params);
    }
    
    const results: T[] = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject() as T);
    }
    stmt.free();
    return results;
  }
}

/**
 * Execute a statement (INSERT, UPDATE, DELETE)
 */
export async function execute(
  sql: string,
  params: any[] = []
): Promise<{ lastId?: number; changes?: number }> {
  if (useTurso) {
    const result = await tursoClient!.execute({
      sql,
      args: params,
    });
    return {
      lastId: Number(result.lastInsertRowid) || undefined,
      changes: result.rowsAffected,
    };
  } else {
    const db = await getSqliteDb();
    db.run(sql, params);
    
    const lastIdResult = db.exec('SELECT last_insert_rowid() as id');
    const lastId = lastIdResult[0]?.values[0]?.[0] as number | undefined;
    
    return { lastId, changes: db.getRowsModified() };
  }
}

/**
 * Execute raw SQL (for schema creation, etc.)
 * Handles multiple statements separated by semicolons
 */
export async function executeRaw(sql: string): Promise<void> {
  if (useTurso) {
    // Split into individual statements and execute
    const statements = sql
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0 && !s.startsWith('--'));
    
    for (const stmt of statements) {
      try {
        await tursoClient!.execute(stmt);
      } catch (err: any) {
        // Ignore "already exists" errors
        if (!err.message?.includes('already exists')) {
          console.error('SQL Error:', err.message);
        }
      }
    }
  } else {
    const db = await getSqliteDb();
    db.run(sql);
  }
}

/**
 * Execute multiple statements in a batch (Turso optimization)
 */
export async function executeBatch(
  statements: { sql: string; args?: any[] }[]
): Promise<void> {
  if (useTurso) {
    await tursoClient!.batch(
      statements.map(s => ({
        sql: s.sql,
        args: s.args || [],
      }))
    );
  } else {
    const db = await getSqliteDb();
    for (const stmt of statements) {
      db.run(stmt.sql, stmt.args || []);
    }
  }
}

export default {
  IS_TURSO,
  IS_LOCAL,
  initDb,
  getSqliteDb,
  saveSqliteDb,
  closeDb,
  query,
  execute,
  executeRaw,
  executeBatch,
};
