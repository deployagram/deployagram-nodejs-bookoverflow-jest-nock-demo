// Jest loads its global teardown as plain CommonJS. See global-setup.cjs.
require('tsx/cjs');
module.exports = require('./support/global-teardown.ts').default;
