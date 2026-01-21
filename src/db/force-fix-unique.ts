import { createClient } from '@libsql/client';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '../../.env.local') });

async function forceFixUniqueColumns() {
  console.log('Force fixing UNIQUE columns...');
  
  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoUrl || !tursoToken) {
    console.error('Turso credentials not found');
    return;
  }

  const client = createClient({
    url: tursoUrl,
    authToken: tursoToken,
  });

  try {
    // 1. Fix providers table (add provider_id without UNIQUE first)
    console.log('\n--- Providers Table ---');
    try {
      // First check if column exists
      await client.execute('SELECT provider_id FROM providers LIMIT 1');
      console.log('provider_id column already exists');
    } catch (e) {
      console.log('Adding provider_id column (non-unique first)...');
      await client.execute('ALTER TABLE providers ADD COLUMN provider_id TEXT');
      // We can't easily add UNIQUE constraint to existing column in SQLite/LibSQL without recreation
      // But at least the column will exist for the application code
      console.log('✓ Added provider_id column');
      
      // Create a unique index instead to enforce uniqueness
      await client.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_provider_id ON providers(provider_id) WHERE provider_id IS NOT NULL');
      console.log('✓ Created unique index on provider_id');
    }

    // 2. Fix contractors table (add vendor_code without UNIQUE first)
    console.log('\n--- Contractors Table ---');
    try {
      await client.execute('SELECT vendor_code FROM contractors LIMIT 1');
      console.log('vendor_code column already exists');
    } catch (e) {
      console.log('Adding vendor_code column (non-unique first)...');
      await client.execute('ALTER TABLE contractors ADD COLUMN vendor_code TEXT');
      console.log('✓ Added vendor_code column');
      
      await client.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_contractors_vendor_code_unique ON contractors(vendor_code) WHERE vendor_code IS NOT NULL');
      console.log('✓ Created unique index on vendor_code');
    }

  } catch (error: any) {
    console.error('Fix failed:', error.message);
  } finally {
    client.close();
  }
}

forceFixUniqueColumns();
