try { require('dotenv').config(); } catch (_) {}

const express = require('express');
const http = require('http');
const cors = require('cors');
const apiKeyAuth = require('./middleware/apiKeyAuth');
const requestLogger = require('./middleware/requestLogger');
const sessionLock = require('./middleware/sessionLock');
const pythonRoutes = require('./routes/python');
const statRoutes = require('./routes/stat');
const tmpUploadRoutes = require('./routes/tmp-upload');
const vizRoutes = require('./routes/viz');
const aggregateRoutes = require('./routes/aggregate');
const mlRoutes = require('./routes/ml');
const mcpRoutes = require('./routes/mcp');
const { initializeWebSocket } = require('./services/socket');
const { deleteSession, serializeSession, touchSessionByToken } = require('./services/authSessions');
const { getE2EState, isE2ETestMode, resetE2EState } = require('./lib/e2eRuntime');

const app = express();
const server = http.createServer(app);

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'X-API-Key', 'X-Client-Id', 'X-Session-Token'],
  exposedHeaders: ['X-Session-Token'],
}));

app.use(express.json({ limit: '25mb' }));

app.get('/auth/verify', apiKeyAuth, sessionLock, requestLogger, (req, res) => {
  if (req.sessionInfo?.sessionToken) {
    res.set('X-Session-Token', req.sessionInfo.sessionToken);
  }
  res.status(200).json({
    ok: true,
    message: 'API key is valid',
    apiKey: req.apiKey,
    session: req.sessionInfo ? {
      id: req.sessionInfo.id,
      clientId: req.sessionInfo.clientId,
      createdAt: req.sessionInfo.createdAt,
      lastHeartbeatAt: req.sessionInfo.lastHeartbeatAt,
      expiresAt: req.sessionInfo.expiresAt,
    } : null,
  });
});

app.post('/auth/heartbeat', apiKeyAuth, async (req, res) => {
  try {
    const clientId = String(req.header('X-Client-Id') || '').trim();
    const sessionToken = String(req.header('X-Session-Token') || '').trim();
    if (!clientId || !sessionToken) {
      return res.status(400).json({
        ok: false,
        code: 'SESSION_HEADERS_REQUIRED',
        error: 'X-Client-Id and X-Session-Token are required',
      });
    }

    const session = await touchSessionByToken({
      apiKeyId: req.apiKey.id,
      clientId,
      sessionToken,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
    });

    if (!session) {
      return res.status(409).json({
        ok: false,
        code: 'SESSION_INVALID',
        error: 'Session expired or invalid',
      });
    }

    res.set('X-Session-Token', session.session_token);
    return res.json({
      ok: true,
      session: serializeSession(session),
    });
  } catch (error) {
    console.error('heartbeat error:', error);
    return res.status(500).json({ ok: false, error: 'Internal server error' });
  }
});

app.post('/auth/logout', apiKeyAuth, async (req, res) => {
  try {
    const clientId = String(req.header('X-Client-Id') || '').trim();
    const sessionToken = String(req.header('X-Session-Token') || '').trim();
    if (!clientId || !sessionToken) {
      return res.status(400).json({
        ok: false,
        code: 'SESSION_HEADERS_REQUIRED',
        error: 'X-Client-Id and X-Session-Token are required',
      });
    }

    await deleteSession({
      apiKeyId: req.apiKey.id,
      clientId,
      sessionToken,
    });

    return res.json({ ok: true });
  } catch (error) {
    console.error('logout error:', error);
    return res.status(500).json({ ok: false, error: 'Internal server error' });
  }
});

app.get('/healthz', (_req, res) => res.json({ ok: true }));

if (isE2ETestMode()) {
  app.post('/__e2e__/reset', (_req, res) => {
    resetE2EState();
    return res.json({ ok: true });
  });

  app.get('/__e2e__/state', (_req, res) => res.json(getE2EState()));
}

const guarded = [apiKeyAuth, sessionLock, requestLogger];

app.use('/api', ...guarded, pythonRoutes);
app.use('/tmp-upload', ...guarded, tmpUploadRoutes);
app.use('/viz/aggregate', ...guarded, aggregateRoutes);
app.use('/viz', ...guarded, vizRoutes);
app.use('/stat', ...guarded, statRoutes);
app.use('/ml', ...guarded, mlRoutes);
app.use('/mcp', ...guarded, mcpRoutes);

initializeWebSocket(server);

const PORT = Number(process.env.PORT || 5000);
server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
