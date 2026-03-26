const crypto = require('crypto');
const pool = require('../config/db');

function hashApiKey(rawKey) {
  return crypto.createHash('sha256').update(String(rawKey || ''), 'utf8').digest('hex');
}

function publicApiKeyRecord(row) {
  return {
    id: row.id,
    keyPrefix: row.key_prefix,
    ownerName: row.owner_name || null,
    label: row.label || null,
    status: row.status,
    expiresAt: row.expires_at,
    minuteLimit: row.minute_limit,
    dailyLimit: row.daily_limit,
  };
}

module.exports = async function apiKeyAuth(req, res, next) {
  try {
    const rawKey = String(req.header('X-API-Key') || '').trim();
    if (!rawKey) {
      return res.status(401).json({
        ok: false,
        code: 'API_KEY_REQUIRED',
        error: 'X-API-Key header is required',
      });
    }

    const keyPrefix = rawKey.slice(0, 20);
    const keyHash = hashApiKey(rawKey);
    const result = await pool.query(
      `
        SELECT *
        FROM api_keys
        WHERE key_prefix = $1
          AND key_hash = $2
          AND status = 'active'
          AND (expires_at IS NULL OR expires_at > NOW())
        LIMIT 1
      `,
      [keyPrefix, keyHash]
    );

    if (!result.rows.length) {
      return res.status(401).json({
        ok: false,
        code: 'API_KEY_INVALID',
        error: 'API key is invalid or expired',
      });
    }

    const row = result.rows[0];
    req.apiKey = publicApiKeyRecord(row);
    req.apiKeyRaw = rawKey;

    await pool.query(
      `
        UPDATE api_keys
        SET last_used_at = NOW(),
            last_ip = $1,
            last_user_agent = $2
        WHERE id = $3
      `,
      [
        String(req.ip || '').replace(/^::ffff:/, '') || null,
        req.get('user-agent') || null,
        row.id,
      ]
    );

    return next();
  } catch (error) {
    console.error('apiKeyAuth error:', error);
    return res.status(500).json({
      ok: false,
      code: 'API_KEY_AUTH_ERROR',
      error: 'Internal server error',
    });
  }
};
