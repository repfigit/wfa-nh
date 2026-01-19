import { startServer } from './api/server.js';

import dotenv from 'dotenv';
dotenv.config({ path: '.env.local' });

console.log(`
╔════════════════════════════════════════════════════════════════╗
║     NH Immigrant Contracts Tracker                             ║
║     Tracking state contracts with immigrant-related services   ║
╚════════════════════════════════════════════════════════════════╝
`);

startServer().catch(console.error);
