const crypto = require('crypto');

function isE2ETestMode() {
  return ['1', 'true', 'yes', 'on'].includes(String(process.env.E2E_TEST_MODE || '').trim().toLowerCase());
}

function getConfiguredTestApiKey() {
  const value = String(process.env.PLAYWRIGHT_TEST_API_KEY || '').trim();
  if (!value) {
    throw new Error('PLAYWRIGHT_TEST_API_KEY is required when E2E_TEST_MODE is enabled');
  }
  return value;
}

function sessionTtlSeconds() {
  const value = Number(process.env.SESSION_TTL_SECONDS || 300);
  if (!Number.isFinite(value) || value <= 0) return 300;
  return Math.floor(value);
}

function makeSessionToken() {
  return crypto.randomBytes(24).toString('hex');
}

function now() {
  return new Date();
}

function expiresAtFromNow() {
  return new Date(Date.now() + sessionTtlSeconds() * 1000);
}

const state = {
  nextSessionId: 1,
  sessionsByApiKeyId: new Map(),
  usageLogs: [],
};

function buildApiKeyRecord(rawKey) {
  return {
    id: 'e2e-api-key',
    keyPrefix: String(rawKey || '').slice(0, 20),
    ownerName: 'playwright',
    label: 'playwright-e2e',
    status: 'active',
    expiresAt: null,
    minuteLimit: null,
    dailyLimit: null,
  };
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

function snapshotSessions() {
  return Array.from(state.sessionsByApiKeyId.values()).map((session) => serializeSession(session));
}

function snapshotUsageLogs() {
  return state.usageLogs.map((entry) => ({ ...entry }));
}

function resetE2EState() {
  state.nextSessionId = 1;
  state.sessionsByApiKeyId.clear();
  state.usageLogs = [];
}

function recordUsage(entry) {
  const payload = {
    at: now().toISOString(),
    ...entry,
  };
  state.usageLogs.push(payload);
  if (state.usageLogs.length > 200) {
    state.usageLogs = state.usageLogs.slice(-200);
  }
  console.log(
    `[e2e request] ${payload.method} ${payload.endpoint} -> ${payload.statusCode} (${payload.responseTimeMs}ms)`
  );
}

async function cleanupExpiredSessions(apiKeyId) {
  const active = state.sessionsByApiKeyId.get(String(apiKeyId || ''));
  if (!active) return;
  if (new Date(active.expires_at).getTime() <= Date.now()) {
    state.sessionsByApiKeyId.delete(String(apiKeyId || ''));
  }
}

async function findActiveSession(apiKeyId) {
  await cleanupExpiredSessions(apiKeyId);
  return state.sessionsByApiKeyId.get(String(apiKeyId || '')) || null;
}

async function createSession({ apiKeyId, clientId, ipAddress, userAgent }) {
  const createdAt = now();
  const row = {
    id: state.nextSessionId++,
    api_key_id: String(apiKeyId || ''),
    session_token: makeSessionToken(),
    client_id: String(clientId || ''),
    ip_address: normalizeIpAddress(ipAddress),
    user_agent: userAgent || null,
    created_at: createdAt,
    last_heartbeat_at: createdAt,
    expires_at: expiresAtFromNow(),
  };
  state.sessionsByApiKeyId.set(String(apiKeyId || ''), row);
  return row;
}

async function touchSessionById(sessionId, { ipAddress, userAgent }) {
  for (const [apiKeyId, row] of state.sessionsByApiKeyId.entries()) {
    if (String(row.id) !== String(sessionId)) continue;
    const touched = {
      ...row,
      ip_address: normalizeIpAddress(ipAddress),
      user_agent: userAgent || null,
      last_heartbeat_at: now(),
      expires_at: expiresAtFromNow(),
    };
    state.sessionsByApiKeyId.set(apiKeyId, touched);
    return touched;
  }
  return null;
}

async function touchSessionByToken({ apiKeyId, clientId, sessionToken, ipAddress, userAgent }) {
  const active = await findActiveSession(apiKeyId);
  if (!active) return null;
  if (String(active.client_id) !== String(clientId || '')) return null;
  if (String(active.session_token) !== String(sessionToken || '')) return null;
  const touched = {
    ...active,
    ip_address: normalizeIpAddress(ipAddress),
    user_agent: userAgent || null,
    last_heartbeat_at: now(),
    expires_at: expiresAtFromNow(),
  };
  state.sessionsByApiKeyId.set(String(apiKeyId || ''), touched);
  return touched;
}

async function deleteSession({ apiKeyId, clientId, sessionToken }) {
  const active = await findActiveSession(apiKeyId);
  if (!active) return 0;
  if (String(active.client_id) !== String(clientId || '')) return 0;
  if (String(active.session_token) !== String(sessionToken || '')) return 0;
  state.sessionsByApiKeyId.delete(String(apiKeyId || ''));
  return 1;
}

function getE2EState() {
  return {
    ok: true,
    auth: {
      enabled: true,
      apiKeyConfigured: Boolean(String(process.env.PLAYWRIGHT_TEST_API_KEY || '').trim()),
    },
    sessions: snapshotSessions(),
    usageLogs: snapshotUsageLogs(),
  };
}

module.exports = {
  buildApiKeyRecord,
  cleanupExpiredSessions,
  createSession,
  deleteSession,
  findActiveSession,
  getConfiguredTestApiKey,
  getE2EState,
  isE2ETestMode,
  recordUsage,
  resetE2EState,
  serializeSession,
  touchSessionById,
  touchSessionByToken,
};
