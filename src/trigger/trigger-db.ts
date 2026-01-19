/**
 * Trigger.dev Database Adapter
 * Uses @libsql/client/http for serverless/worker environments
 * This avoids native binding issues in Trigger.dev containers
 */

import { createClient, Client } from '@libsql/client/http';

let tursoClient: Client | null = null;

/**
 * Get or create Turso client using HTTP transport
 */
function getClient(): Client {
  if (!tursoClient) {
    const url = process.env.TURSO_DATABASE_URL;
    const authToken = process.env.TURSO_AUTH_TOKEN;

    if (!url) {
      throw new Error('TURSO_DATABASE_URL environment variable is not set');
    }

    tursoClient = createClient({
      url,
      authToken,
    });
  }
  return tursoClient;
}

/**
 * Execute a query and return results as objects
 */
export async function query<T = any>(
  sql: string,
  params: any[] = []
): Promise<T[]> {
  const client = getClient();
  const result = await client.execute({
    sql,
    args: params,
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
  const client = getClient();
  const result = await client.execute({
    sql,
    args: params,
  });
  return {
    lastId: Number(result.lastInsertRowid) || undefined,
    changes: result.rowsAffected,
  };
}

/**
 * Execute multiple statements in a batch
 */
export async function executeBatch(
  statements: { sql: string; args?: any[] }[]
): Promise<void> {
  const client = getClient();
  await client.batch(
    statements.map(s => ({
      sql: s.sql,
      args: s.args || [],
    }))
  );
}

export default {
  query,
  execute,
  executeBatch,
};
