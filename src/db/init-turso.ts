/**
 * Initialize Turso database with schema
 * Run this to create tables in Turso before or after migration
 */

import { createClient } from '@libsql/client';
import { schema } from './schema.js';
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

async function initializeTursoSchema() {
  console.log('Initializing Turso database schema...');

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

  console.log('Connected to Turso');

  // Execute schema
  console.log('Creating tables...');
  const statements = schema
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0 && !s.startsWith('--'));

  for (const statement of statements) {
    try {
      await tursoClient.execute(statement);
      console.log('✓ Executed statement');
  } catch (error: any) {
    // Ignore "already exists" errors
    if (!error.message?.includes('already exists') && !error.message?.includes('duplicate column name')) {
      console.error('SQL Error:', error.message);
      console.error('Statement:', statement);
      throw error; // Re-throw the error instead of continuing
    } else {
      console.log('✓ Table/column already exists, skipping');
    }
  }
  }

  console.log('Schema initialization completed!');
}

// Run the initialization
initializeTursoSchema().catch((error) => {
  console.error('Schema initialization failed:', error);
  process.exit(1);
});