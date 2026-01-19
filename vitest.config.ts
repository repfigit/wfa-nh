import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.test.ts', 'src/**/*.spec.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/*.test.ts',
        'src/**/*.spec.ts',
        'src/db/migrate-*.ts',
        'src/db/seed.ts',
        'src/trigger/**',
      ],
    },
    testTimeout: 10000,
  },
  resolve: {
    alias: {
      '@': '/workspaces/wfa-nh/src',
    },
  },
});
