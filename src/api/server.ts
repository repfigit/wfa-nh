import { app } from './app.js';
import { initializeDb } from '../db/database.js';

const PORT = process.env.PORT || 3000;

export async function startServer() {
  await initializeDb();
  app.listen(PORT, () => {
    console.log(`✓ Server running at http://localhost:${PORT}`);
  });
}
