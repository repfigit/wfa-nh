/**
 * Test Turso connection and check schema
 */

import { createClient } from '@libsql/client';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

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

async function testTursoConnection() {
  console.log('Testing Turso connection...');

  // Load environment variables
  loadEnvFile();

  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  console.log('TURSO_DATABASE_URL:', tursoUrl ? 'Set' : 'Not set');
  console.log('TURSO_AUTH_TOKEN:', tursoToken ? 'Set (length: ' + tursoToken.length + ')' : 'Not set');

  if (!tursoUrl || !tursoToken) {
    throw new Error('TURSO_DATABASE_URL and TURSO_AUTH_TOKEN environment variables must be set');
  }

  try {
    // Connect to Turso
    const tursoClient = createClient({
      url: tursoUrl,
      authToken: tursoToken,
    });

    console.log('Connected to Turso successfully');

    // Test basic query
    const result = await tursoClient.execute('SELECT 1 as test');
    console.log('Basic query result:', result.rows);

    // Check if tables exist
    const tablesResult = await tursoClient.execute(`
      SELECT name FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `);

    console.log('Tables in database:', tablesResult.rows.map(row => row.name));

    if (tablesResult.rows.length === 0) {
      console.log('No tables found - schema needs to be initialized');
    } else {
      console.log('Schema appears to be initialized');

      // Test a simple query on providers table
      try {
        const providersResult = await tursoClient.execute('SELECT COUNT(*) as count FROM providers');
        console.log('Providers count:', providersResult.rows[0].count);
      } catch (error: any) {
        console.log('Error querying providers table:', error.message);
      }
    }

    tursoClient.close();
    console.log('Connection test completed successfully');

  } catch (error) {
    console.error('Connection test failed:', error);
    throw error;
  }
}

// Run the test
testTursoConnection().catch(console.error);