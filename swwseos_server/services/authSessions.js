const crypto = require('crypto');
const pool = require('../config/db');
const e2eRuntime = require('../lib/e2eRuntime');

function sessionTtlSeconds() {
  const value = Number(process.env.SESSION_TTL_SECONDS || 300);
  if (!Number.isFinite(value) || value <= 0) return 300;
  return Math.floor(value);
}

function normalizeIpAddress(value) {
  if (!value) return null;
  return String(value).replace(/^::ffff:/, '');
}

function serializeSession(row) {
  if (!row) return null;
  return {
    id: row.id,
    clientId: row.client_id,
    sessionToken: row.session_token,
    createdAt: row.created_at,
    lastHeartbeatAt: row.last_heartbeat_at,
    expiresAt: row.expires_at,
  };
}

async function cleanupExpiredSessions(apiKeyId) {
  if (e2eRuntime.isE2ETestMode()) {
    return e2eRuntime.cleanupExpiredSessions(apiKeyId);
  }
  if (!apiKeyId) return;
  await pool.query(
    `
      DELETE FROM active_sessions
      WHERE api_key_id = $1
        AND expires_at <= NOW()
    `,
    [apiKeyId]
  );
}

async function findActiveSession(apiKeyId) {
  if (e2eRuntime.isE2ETestMode()) {
    return e2eRuntime.findActiveSession(apiKeyId);
  }
  const result = await pool.query(
    `
      SELECT *
      FROM active_sessions
      WHERE api_key_id = $1
        AND expires_at > NOW()
      ORDER BY last_heartbeat_at DESC, created_at DESC
      LIMIT 1
    `,
    [apiKeyId]
  );
  return result.rows[0] || null;
}

async function createSession({ apiKeyId, clientId, ipAddress, userAgent }) {
  if (e2eRuntime.isE2ETestMode()) {
    return e2eRuntime.createSession({ apiKeyId, clientId, ipAddress, userAgent });
  }
  const token = crypto.randomBytes(24).toString('hex');
  const result = await pool.query(
    `
      INSERT INTO active_sessions (
        api_key_id,
        session_token,
        client_id,
        ip_address,
        user_agent,
        expires_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        NOW() + ($6 * INTERVAL '1 second')
      )
      RETURNING *
    `,
    [
      apiKeyId,
      token,
      clientId,
      normalizeIpAddress(ipAddress),
      userAgent || null,
      sessionTtlSeconds(),
    ]
  );
  return result.rows[0] || null;
}

async function touchSessionById(sessionId, { ipAddress, userAgent }) {
  if (e2eRuntime.isE2ETestMode()) {
    return e2eRuntime.touchSessionById(sessionId, { ipAddress, userAgent });
  }
  const result = await pool.query(
    `
      UPDATE active_sessions
      SET last_heartbeat_at = NOW(),
          expires_at = NOW() + ($1 * INTERVAL '1 second'),
          ip_address = $2,
          user_agent = $3
      WHERE id = $4
      RETURNING *
    `,
    [
      sessionTtlSeconds(),
      normalizeIpAddress(ipAddress),
      userAgent || null,
      sessionId,
    ]
  );
  return result.rows[0] || null;
}

async function touchSessionByToken({ apiKeyId, clientId, sessionToken, ipAddress, userAgent }) {
  if (e2eRuntime.isE2ETestMode()) {
    return e2eRuntime.touchSessionByToken({ apiKeyId, clientId, sessionToken, ipAddress, userAgent });
  }
  const result = await pool.query(
    `
      UPDATE active_sessions
      SET last_heartbeat_at = NOW(),
          expires_at = NOW() + ($1 * INTERVAL '1 second'),
          ip_address = $2,
          user_agent = $3
      WHERE api_key_id = $4
        AND client_id = $5
        AND session_token = $6
        AND expires_at > NOW()
      RETURNING *
    `,
    [
      sessionTtlSeconds(),
      normalizeIpAddress(ipAddress),
      userAgent || null,
      apiKeyId,
      clientId,
      sessionToken,
    ]
  );
  return result.rows[0] || null;
}

async function deleteSession({ apiKeyId, clientId, sessionToken }) {
  if (e2eRuntime.isE2ETestMode()) {
    return e2eRuntime.deleteSession({ apiKeyId, clientId, sessionToken });
  }
  const result = await pool.query(
    `
      DELETE FROM active_sessions
      WHERE api_key_id = $1
        AND client_id = $2
        AND session_token = $3
      RETURNING id
    `,
    [apiKeyId, clientId, sessionToken]
  );
  return result.rowCount || 0;
}

module.exports = {
  cleanupExpiredSessions,
  createSession,
  deleteSession,
  findActiveSession,
  serializeSession: e2eRuntime.isE2ETestMode() ? e2eRuntime.serializeSession : serializeSession,
  sessionTtlSeconds,
  touchSessionById,
  touchSessionByToken,
};
