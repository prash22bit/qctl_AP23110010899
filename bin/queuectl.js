#!/usr/bin/env node

const { setupCLI } = require('../src/cli');

const program = setupCLI();
program.parseAsync(process.argv).catch(err => {
  console.error('Unhandled CLI error:', err);
  process.exit(1);
});
