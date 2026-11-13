import { createDeployagramSuite, defaultSuiteDependencies, type DeployagramSuiteOptions } from '@deployagram/core';
import { afterAll, afterEach, beforeAll, beforeEach, expect } from '@jest/globals';
import { collectorUrlFromEnvironment } from './stack.js';

/**
 * The Jest adapter for @deployagram/core: wires the suite into Jest's hooks. The Collector is the one that global setup
 * found or started, and its URL comes from the environment. Looking it up again inside a test file fails under Jest,
 * because Testcontainers' Docker client cannot be used there. The suite posts no Proxy configuration, and marks each
 * test's end.
 */
export function useDeployagram(options: DeployagramSuiteOptions) {
  const suite = createDeployagramSuite(options, {
    ...defaultSuiteDependencies,
    startEnvironment: async () => ({
      logger: { url: collectorUrlFromEnvironment(), source: 'url' },
      async stop() {
        // The run stops the Collector, if it started one. See global-teardown.ts.
      },
    }),
  });

  beforeAll(() => suite.beforeAll(), 180_000);
  beforeEach(() => suite.beforeEach());
  afterEach(() => suite.afterEach(testNameWithoutSuite(options.name)));
  afterAll(() => suite.stop());

  return suite;
}

/**
 * Jest's current test name is the describe name and the test name, joined by a space. The core marks
 * "<suite>.<test>", so this takes off the describe name.
 */
function testNameWithoutSuite(suiteName: string): string {
  const fullName = expect.getState().currentTestName ?? '';
  const prefix = `${suiteName} `;
  return fullName.startsWith(prefix) ? fullName.slice(prefix.length) : fullName;
}
