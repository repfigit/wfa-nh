/**
 * Migration script to transfer local SQLite data to Turso
 * Run this once to migrate from local development to Turso
 */

import { createClient } from '@libsql/client';
import initSqlJs from 'sql.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, '../../data/childcare.db');

// Load environment variables from .env.local
function loadEnvFile() {
  const envPath = path.join(__dirname, '../../.env.local');
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    const lines = envContent.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith('#')) {
        const [key, ...valueParts] = trimmed.split('=');
        if (key && valueParts.length > 0) {
          const value = valueParts.join('=').replace(/^["']|["']$/g, ''); // Remove quotes
          process.env[key] = value;
        }
      }
    }
    console.log('Loaded environment variables from .env.local');
  } else {
    console.log('No .env.local file found');
  }
}

async function migrateToTurso() {
  console.log('Starting migration from local SQLite to Turso...');

  // Load environment variables
  loadEnvFile();

  // Check environment variables
  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  console.log('TURSO_DATABASE_URL:', tursoUrl ? 'Set' : 'Not set');
  console.log('TURSO_AUTH_TOKEN:', tursoToken ? 'Set (length: ' + tursoToken.length + ')' : 'Not set');

  if (!tursoUrl || !tursoToken) {
    throw new Error('TURSO_DATABASE_URL and TURSO_AUTH_TOKEN environment variables must be set');
  }

  // Connect to Turso
  const tursoClient = createClient({
    url: tursoUrl,
    authToken: tursoToken,
  });

  // Load local SQLite database
  if (!fs.existsSync(DB_PATH)) {
    console.log('No local database found at', DB_PATH);
    console.log('Creating schema in Turso only...');
    return;
  }

  const SQL = await initSqlJs();
  const fileBuffer = fs.readFileSync(DB_PATH);
  const sqliteDb = new SQL.Database(fileBuffer);

  console.log('Connected to both databases');

  // Get all table names from SQLite
  const tablesResult = sqliteDb.exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
  const tables = tablesResult[0]?.values.flat().map(v => v?.toString() || '') || [];

  console.log('Found tables:', tables);

  for (const table of tables) {
    console.log(`\nMigrating table: ${table}`);

    // Get table schema
    const schemaResult = sqliteDb.exec(`PRAGMA table_info(${table})`);
    const columns = schemaResult[0]?.values.map(row => ({
      name: row[1]?.toString(),
      type: row[2]?.toString(),
      notnull: row[3],
      default: row[4],
      pk: row[5]
    })) || [];

    console.log(`  Columns: ${columns.map(c => c.name).join(', ')}`);

    // Get all data from the table
    const dataResult = sqliteDb.exec(`SELECT * FROM ${table}`);
    const rows = dataResult[0]?.values || [];
    const columnNames = dataResult[0]?.columns || [];

    console.log(`  Found ${rows.length} rows`);

    if (rows.length === 0) continue;

    // Insert data into Turso
    // First, create the table if it doesn't exist (schema should already be created, but just in case)
    try {
      // Insert in batches to avoid memory issues
      const batchSize = 100;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);

        // Build INSERT statement
        const placeholders = batch.map(() => `(${columnNames.map(() => '?').join(', ')})`).join(', ');
        const values = batch.flat();

        const insertSql = `INSERT OR IGNORE INTO ${table} (${columnNames.join(', ')}) VALUES ${placeholders}`;

        await tursoClient.execute({
          sql: insertSql,
          args: values
        });

        console.log(`  Inserted ${Math.min(i + batchSize, rows.length)}/${rows.length} rows`);
      }
    } catch (error) {
      console.error(`Error migrating table ${table}:`, error);
      // Continue with other tables
    }
  }

  sqliteDb.close();
  console.log('\nMigration completed successfully!');
  console.log('You can now remove the local SQLite files if desired.');
}

// Run the migration
migrateToTurso().catch(console.error);