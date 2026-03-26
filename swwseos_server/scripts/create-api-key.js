try { require('dotenv').config(); } catch (_) {}

const crypto = require('crypto');
const pool = require('../config/db');

function argValue(flag) {
  const item = process.argv.find((value) => value.startsWith(`${flag}=`));
  return item ? item.slice(flag.length + 1) : '';
}

async function main() {
  const ownerName = argValue('--owner') || null;
  const label = argValue('--label') || null;
  const rawKey = `ngnl_${crypto.randomBytes(24).toString('hex')}`;
  const keyPrefix = rawKey.slice(0, 20);
  const keyHash = crypto.createHash('sha256').update(rawKey, 'utf8').digest('hex');

  const result = await pool.query(
    `
      INSERT INTO api_keys (
        key_prefix,
        key_hash,
        owner_name,
        label,
        status
      )
      VALUES ($1, $2, $3, $4, 'active')
      RETURNING id, key_prefix, owner_name, label, status, created_at
    `,
    [keyPrefix, keyHash, ownerName, label]
  );

  const row = result.rows[0];
  console.log(JSON.stringify({
    ok: true,
    apiKey: rawKey,
    record: row,
  }, null, 2));
  console.error('Store the raw apiKey securely. It will not be shown again by the server.');
  await pool.end();
}

main().catch(async (error) => {
  console.error('create-api-key error:', error);
  try {
    await pool.end();
  } catch (_) {}
  process.exitCode = 1;
});
