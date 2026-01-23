import dotenv from 'dotenv';
dotenv.config();

import { startServer } from './api/server.js';

startServer().catch(console.error);
