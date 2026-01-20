import { app } from './app.js';
import { initializeDb } from '../db/database.js';

const PORT = process.env.PORT || 3001;

export async function startServer() {
  await initializeDb();
  app.listen(PORT, () => {
    console.log(`
╔════════════════════════════════════════════════════════════════╗
║     NH Childcare Payments Tracker                              ║
║     Local Server running at http://localhost:${PORT}           ║
╚════════════════════════════════════════════════════════════════╝
    `);
  });
}
