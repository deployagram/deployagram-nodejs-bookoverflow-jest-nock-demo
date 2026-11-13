// Jest loads its global setup as plain CommonJS. tsx's CommonJS hook lets it load the TypeScript setup, whose imports
// end in .js, as the rest of the project's do.
require('tsx/cjs');
module.exports = require('./support/global-setup.ts').default;
