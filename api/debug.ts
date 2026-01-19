import type { VercelRequest, VercelResponse } from '@vercel/node';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    // Check environment variables
    const tursoUrl = process.env.TURSO_DATABASE_URL;
    const tursoToken = process.env.TURSO_AUTH_TOKEN;

    const envStatus = {
      TURSO_DATABASE_URL: tursoUrl ? 'Set' : 'Not set',
      TURSO_AUTH_TOKEN: tursoToken ? 'Set (length: ' + tursoToken.length + ')' : 'Not set',
    };

    console.log('Environment check:', envStatus);

    // Try basic Turso connection test
    if (tursoUrl && tursoToken) {
      try {
        const { createClient } = await import('@libsql/client');
        const client = createClient({
          url: tursoUrl,
          authToken: tursoToken,
        });

        const result = await client.execute('SELECT 1 as test');
        console.log('Turso connection successful:', result.rows);

        // Try to initialize database schema
        console.log('Attempting to initialize database...');
        try {
          const { initializeDb } = await import('../src/db/database.js');
          await initializeDb();
          console.log('Database initialization completed');
        } catch (initError: any) {
          console.error('Database initialization failed:', initError);
        }

        // Check tables
        const tablesResult = await client.execute(`
          SELECT name FROM sqlite_master
          WHERE type='table' AND name NOT LIKE 'sqlite_%'
          ORDER BY name
        `);

        console.log('Tables found:', tablesResult.rows.length);

        // Try a simple query
        let providersCount = 'N/A';
        let paymentsCount = 'N/A';
        try {
          if (tablesResult.rows.some(row => row.name === 'providers')) {
            const countResult = await client.execute('SELECT COUNT(*) as count FROM providers');
            providersCount = countResult.rows[0].count?.toString() || '0';
          }
          if (tablesResult.rows.some(row => row.name === 'payments')) {
            const countResult = await client.execute('SELECT COUNT(*) as count FROM payments');
            paymentsCount = countResult.rows[0].count?.toString() || '0';
          }
        } catch (countError: any) {
          console.error('Error counting records:', countError);
        }

        client.close();

        return res.json({
          success: true,
          environment: envStatus,
          connection: 'successful',
          tables: tablesResult.rows.map(row => row.name),
          table_count: tablesResult.rows.length,
          providers_count: providersCount,
          payments_count: paymentsCount,
          test_query: result.rows,
        });

      } catch (error: any) {
        console.error('Turso connection failed:', error);
        return res.json({
          success: false,
          environment: envStatus,
          connection: 'failed',
          error: error.message,
        });
      }
    } else {
      return res.json({
        success: false,
        environment: envStatus,
        error: 'Environment variables not set',
      });
    }

  } catch (error: any) {
    console.error('Handler error:', error);
    return res.json({
      success: false,
      error: error.message,
    });
  }
}