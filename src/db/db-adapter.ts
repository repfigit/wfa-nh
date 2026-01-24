/**
 * Database Adapter
 * Uses Turso (libSQL) by default if available, falls back to SQLite locally
 * Since Turso is SQLite-compatible, we use the same SQL syntax everywhere
 */

import { createClient, Client } from '@libsql/client';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Detect environment - prefer Turso if URL is set
let useTurso = true;

// For backward compatibility
export const IS_TURSO = () => true;
export const IS_LOCAL = () => false;

// Database instances
let tursoClient: Client | null = null;

/**
 * Initialize the database connection
 */
export async function initDb(): Promise<void> {
  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;

  if (!url) {
    throw new Error('TURSO_DATABASE_URL environment variable is required. No local database fallback allowed.');
  }

  if (!tursoClient) {
    tursoClient = createClient({
      url,
      authToken,
    });
  }

  // Test connection
  try {
    await tursoClient.execute('SELECT 1');
    useTurso = true;
  } catch (error) {
    console.error('Failed to connect to Turso:', error);
    throw error;
  }
}

/**
 * Close database connection
 */
export function closeDb(): void {
  // Turso client doesn't need explicit closing
}

/**
 * Execute a query and return results as objects
 */
export async function query<T = any>(
  sql: string,
  params: any[] = []
): Promise<T[]> {
  if (!tursoClient) await initDb();
  
  const result = await tursoClient!.execute({
    sql,
    args: params.map(p => (p === undefined || p === '') ? null : p),
  });
  return result.rows as T[];
}

/**
 * Execute a statement (INSERT, UPDATE, DELETE)
 */
export async function execute(
  sql: string,
  params: any[] = []
): Promise<{ lastId?: number; changes?: number }> {
  if (!tursoClient) await initDb();

  const result = await tursoClient!.execute({
    sql,
    args: params.map(p => (p === undefined || p === '') ? null : p),
  });
  return {
    lastId: Number(result.lastInsertRowid) || undefined,
    changes: result.rowsAffected,
  };
}

/**
 * Execute raw SQL (for schema creation, etc.)
 * Handles multiple statements separated by semicolons
 */
export async function executeRaw(sql: string): Promise<void> {
  if (!tursoClient) await initDb();

  // Split into individual statements and execute
  const statements = sql
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0)
    // Remove comment-only lines from each statement
    .map(s => s.split('\n').filter(line => !line.trim().startsWith('--')).join('\n').trim())
    .filter(s => s.length > 0);
  
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    try {
      await tursoClient!.execute(stmt);
    } catch (err: any) {
      console.error(`SQL Error on statement ${i + 1}:`, err.message);
      console.error('Statement was:', stmt);
    }
  }
}

/**
 * Execute multiple statements in a batch (Turso optimization)
 */
export async function executeBatch(
  statements: { sql: string; args?: any[] }[]
): Promise<void> {
  if (!tursoClient) await initDb();

  await tursoClient!.batch(
    statements.map(s => ({
      sql: s.sql,
      args: (s.args || []).map(p => (p === undefined || p === '') ? null : p),
    }))
  );
}

export default {
  IS_TURSO,
  IS_LOCAL,
  initDb,
  closeDb,
  query,
  execute,
  executeRaw,
  executeBatch,
};
