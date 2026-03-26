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
const { initializeWebSocket } = require('./services/socet');

const app = express();
const server = http.createServer(app);
const pool = require('./config/db');

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'X-API-Key'],
}));

app.use(express.json({ limit: '25mb' }));

app.get('/auth/verify', apiKeyAuth, sessionLock, requestLogger, (req, res) => {
  res.status(200).json({
    ok: true,
    message: 'API key is valid',
    apiKey: req.apiKey,
    session: req.sessionInfo,
  });
});

app.post('/auth/logout', apiKeyAuth, async (req, res) => {

  try {

    const clientId = req.header('X-Client-Id');

    const sessionToken = req.header('X-Session-Token');



    if (!clientId || !sessionToken) {

      return res.status(400).json({

        error: 'X-Client-Id and X-Session-Token are required',

      });

    }



    await pool.query(

      `

      DELETE FROM active_sessions

      WHERE api_key_id = $1

        AND client_id = $2

        AND session_token = $3

      `,

      [req.apiKey.id, clientId, sessionToken]

    );



    return res.json({ ok: true });

  } catch (err) {

    console.error('logout error:', err);

    return res.status(500).json({ error: 'Internal server error' });

  }

});



app.post('/auth/heartbeat', apiKeyAuth, async (req, res) => {

  try {

    const clientId = req.header('X-Client-Id');

    const sessionToken = req.header('X-Session-Token');



    if (!clientId || !sessionToken) {

      return res.status(400).json({

        error: 'X-Client-Id and X-Session-Token are required',

      });

    }



    const result = await pool.query(

      `

      UPDATE active_sessions

      SET last_heartbeat_at = NOW(),

          expires_at = NOW() + interval '5 minutes',

          ip_address = $1,

          user_agent = $2

      WHERE api_key_id = $3

        AND client_id = $4

        AND session_token = $5

        AND expires_at > NOW()

      RETURNING id

      `,

      [req.ip || null, req.get('user-agent') || null, req.apiKey.id, clientId, sessionToken]

    );



    if (result.rows.length === 0) {

      return res.status(409).json({

        error: 'Session expired or invalid',

        code: 'SESSION_INVALID',

      });

    }



    return res.json({ ok: true });

  } catch (err) {

    console.error('heartbeat error:', err);

    return res.status(500).json({ error: 'Internal server error' });

  }

});

// Public health endpoint for runtime checks (no API key required)
app.get('/healthz', (req, res) => res.json({ ok: true }));

// Guarded API surface
// - /api: legacy compatibility endpoints (upload/process/generate-graph/run-python)
// - /tmp-upload: optional temporary upload signing/deletion
// - /viz, /viz/aggregate: new visualization preparation/aggregation
// - /stat: standardized statistics contract (/stat/run)
// - /ml: model training playground (ML + neural baseline)
// - /mcp: MCP-compatible discovery/call bridge
app.use('/api', apiKeyAuth,sessionLock, requestLogger, pythonRoutes);
app.use('/tmp-upload', apiKeyAuth,sessionLock, requestLogger, tmpUploadRoutes);
app.use('/viz', apiKeyAuth,sessionLock, requestLogger, vizRoutes);
app.use('/viz/aggregate', apiKeyAuth,sessionLock, requestLogger, aggregateRoutes);
app.use('/stat', apiKeyAuth,sessionLock, requestLogger, statRoutes);
app.use('/ml', apiKeyAuth,sessionLock, requestLogger, mlRoutes);
app.use('/mcp', apiKeyAuth,sessionLock, requestLogger, mcpRoutes);

initializeWebSocket(server);

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

