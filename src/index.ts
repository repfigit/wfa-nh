import dotenv from 'dotenv';
dotenv.config();

import { startServer } from './api/server.js';

console.log(`
╔════════════════════════════════════════════════════════════════╗
║     NH Immigrant Contracts Tracker                             ║
║     Tracking state contracts with immigrant-related services   ║
╚════════════════════════════════════════════════════════════════╝
`);

startServer().catch(console.error);
