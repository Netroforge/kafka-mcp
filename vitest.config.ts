import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
    // The integration tests share one broker and seed it in beforeAll; run files one at a time.
    fileParallelism: false
  }
})
