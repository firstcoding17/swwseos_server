const { Pool } = require('pg');

function toBool(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function buildConfig() {
  const databaseUrl = String(process.env.DATABASE_URL || '').trim();
  if (databaseUrl) {
    return {
      connectionString: databaseUrl,
      ssl: toBool(process.env.DB_SSL, false) ? { rejectUnauthorized: false } : false,
    };
  }

  return {
    host: process.env.DB_HOST || '127.0.0.1',
    port: Number(process.env.DB_PORT || 5432),
    database: process.env.DB_NAME || 'ngnl_db',
    user: process.env.DB_USER || 'ngnl_user',
    password: process.env.DB_PASSWORD || undefined,
    ssl: toBool(process.env.DB_SSL, false) ? { rejectUnauthorized: false } : false,
  };
}

const pool = new Pool(buildConfig());

pool.on('error', (error) => {
  console.error('postgres pool error:', error);
});

module.exports = pool;
