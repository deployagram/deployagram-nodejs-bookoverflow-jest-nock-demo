/** @type {import('jest').Config} */
export default {
  // Jest runs in CommonJS mode, so it needs no --experimental-vm-modules flag.
  transform: {
    '^.+\\.ts$': ['@swc/jest', { jsc: { parser: { syntax: 'typescript' }, target: 'es2022' }, module: { type: 'commonjs' } }],
  },
  moduleNameMapper: { '^(\\.{1,2}/.*)\\.js$': '$1' },
  testMatch: ['<rootDir>/test/**/*.test.ts'],
  // Starts Kafka and the Schema Registry once for the run, and stops them at the end.
  globalSetup: "<rootDir>/test/global-setup.cjs",
  globalTeardown: "<rootDir>/test/global-teardown.cjs",
  // The test files share the broker, so run them one after another.
  maxWorkers: 1,
  testTimeout: 30_000,
};
