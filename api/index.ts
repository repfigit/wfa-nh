import type { VercelRequest, VercelResponse } from '@vercel/node';
import { app } from '../src/api/app.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  return new Promise((resolve) => {
    app(req as any, res as any, () => {
      resolve(undefined);
    });
  });
}
