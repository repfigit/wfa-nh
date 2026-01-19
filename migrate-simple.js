// Simple migration script - row by row
const { createClient } = require('@libsql/client');
const initSqlJs = require('sql.js');
const fs = require('fs');
const path = require('path');

// Load environment variables
const envPath = path.join(__dirname, '.env.local');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  const lines = envContent.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const [key, ...valueParts] = trimmed.split('=');
      if (key && valueParts.length > 0) {
        const value = valueParts.join('=').replace(/^["']|["']$/g, '');
        process.env[key] = value;
      }
    }
  }
}

async function migrateData() {
  console.log('Starting data migration to Turso...');

  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoUrl || !tursoToken) {
    console.error('Environment variables not set');
    process.exit(1);
  }

  const dbPath = path.join(__dirname, 'data', 'childcare.db');
  if (!fs.existsSync(dbPath)) {
    console.log('No local database found');
    return;
  }

  try {
    // Load local SQLite
    const SQL = await initSqlJs();
    const fileBuffer = fs.readFileSync(dbPath);
    const sqliteDb = new SQL.Database(fileBuffer);

    // Connect to Turso
    const tursoClient = createClient({
      url: tursoUrl,
      authToken: tursoToken,
    });

    // Get tables
    const tablesResult = sqliteDb.exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
    const tables = tablesResult[0]?.values.flat().map(v => v.toString()).filter(t => t !== 'keywords' && t !== 'data_sources' && t !== 'scraped_documents') || [];

    console.log('Found tables:', tables);

    for (const table of tables) {
      console.log(`\nMigrating ${table}...`);

      // Get data
      const dataResult = sqliteDb.exec(`SELECT * FROM ${table}`);
      const rows = dataResult[0]?.values || [];
      const columns = dataResult[0]?.columns || [];

      console.log(`Found ${rows.length} rows`);

      if (rows.length === 0) continue;

      // Insert row by row
      let inserted = 0;
      for (const row of rows) {
        try {
          const placeholders = columns.map(() => '?').join(', ');
          const sql = `INSERT OR IGNORE INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;

          await tursoClient.execute({
            sql: sql,
            args: row
          });

          inserted++;
          if (inserted % 10 === 0) {
            console.log(`Inserted ${inserted}/${rows.length} rows`);
          }
        } catch (error) {
          console.error(`Error inserting row:`, error.message);
        }
      }

      console.log(`Completed ${table}: ${inserted} rows inserted`);
    }

    sqliteDb.close();
    tursoClient.close();

    console.log('Migration completed!');

  } catch (error) {
    console.error('Migration failed:', error.message);
  }
}

migrateData();