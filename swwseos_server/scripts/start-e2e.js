process.env.E2E_TEST_MODE = process.env.E2E_TEST_MODE || '1';
process.env.PORT = process.env.PORT || '5100';

if (!String(process.env.PLAYWRIGHT_TEST_API_KEY || '').trim()) {
  console.error('PLAYWRIGHT_TEST_API_KEY is required for start:e2e');
  process.exit(1);
}

require('../app');
