import { createClient } from '@libsql/client';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '../../.env.local') });

async function fixSchema() {
  console.log('Attempting to fix schema columns...');
  
  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoUrl || !tursoToken) {
    console.error('Turso credentials not found in .env.local');
    return;
  }

  const client = createClient({
    url: tursoUrl,
    authToken: tursoToken,
  });

  try {
    // 1. Fix contractors table
    console.log('\n--- Fixing contractors table ---');
    const contractorsColumns = [
      'vendor_code TEXT UNIQUE',
      'dba_name TEXT',
      'is_immigrant_owned INTEGER DEFAULT 0',
      'owner_background TEXT',
      'vendor_id TEXT',
      'notes TEXT'
    ];

    for (const colDef of contractorsColumns) {
      const colName = colDef.split(' ')[0];
      try {
        await client.execute(`ALTER TABLE contractors ADD COLUMN ${colDef}`);
        console.log(`✓ Added ${colName} to contractors`);
      } catch (e: any) {
        if (e.message.includes('duplicate column name')) {
          console.log(`- ${colName} already exists in contractors`);
        } else {
          console.warn(`! Could not add ${colName} to contractors: ${e.message}`);
        }
      }
    }

    // 2. Fix contracts table
    console.log('\n--- Fixing contracts table ---');
    const contractsColumns = [
      'provider_id INTEGER REFERENCES providers(id)',
      'title TEXT',
      'status TEXT',
      'contract_type TEXT',
      'approval_date TEXT',
      'gc_item_number TEXT',
      'notes TEXT',
      'source_document TEXT'
    ];

    for (const colDef of contractsColumns) {
      const colName = colDef.split(' ')[0];
      try {
        await client.execute(`ALTER TABLE contracts ADD COLUMN ${colDef}`);
        console.log(`✓ Added ${colName} to contracts`);
      } catch (e: any) {
        if (e.message.includes('duplicate column name')) {
          console.log(`- ${colName} already exists in contracts`);
        } else {
          console.warn(`! Could not add ${colName} to contracts: ${e.message}`);
        }
      }
    }

    // 3. Fix expenditures table
    console.log('\n--- Fixing expenditures table ---');
    const expendituresColumns = [
      'check_number TEXT'
    ];

    for (const colDef of expendituresColumns) {
      const colName = colDef.split(' ')[0];
      try {
        await client.execute(`ALTER TABLE expenditures ADD COLUMN ${colDef}`);
        console.log(`✓ Added ${colName} to expenditures`);
      } catch (e: any) {
        if (e.message.includes('duplicate column name')) {
          console.log(`- ${colName} already exists in expenditures`);
        } else {
          console.warn(`! Could not add ${colName} to expenditures: ${e.message}`);
        }
      }
    }
    
    // 4. Fix providers table (just in case)
    console.log('\n--- Fixing providers table ---');
    const providersColumns = [
       'provider_id TEXT UNIQUE'
    ];
    for (const colDef of providersColumns) {
      const colName = colDef.split(' ')[0];
      try {
        await client.execute(`ALTER TABLE providers ADD COLUMN ${colDef}`);
        console.log(`✓ Added ${colName} to providers`);
      } catch (e: any) {
        if (e.message.includes('duplicate column name')) {
          console.log(`- ${colName} already exists in providers`);
        } else {
          console.warn(`! Could not add ${colName} to providers: ${e.message}`);
        }
      }
    }


  } catch (error: any) {
    console.error('Migration failed:', error.message);
  } finally {
    client.close();
    console.log('\nSchema fix script finished.');
  }
}

fixSchema();
