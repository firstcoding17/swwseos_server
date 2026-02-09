try { require('dotenv').config(); } catch (_) {}
const express = require('express');
const http = require('http');
const cors = require('cors');

const pythonRoutes = require('./routes/python');
const statRoutes = require('./routes/stat');
const tmpUploadRoutes = require('./routes/tmp-upload');
const vizRoutes = require('./routes/viz');
const aggregateRoutes = require('./routes/aggregate');
const { initializeWebSocket } = require('./services/socet');

const app = express();
const server = http.createServer(app);

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'X-API-Key'],
}));

app.use(express.json({ limit: '25mb' }));

function apiKeyGuard(req, res, next) {
  const allowed = (process.env.ALLOWED_API_KEYS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const key = req.header('X-API-Key') || req.query.api_key;

  if (!key || !allowed.includes(key)) {
    return res.status(401).json({
      ok: false,
      code: 'AUTH_INVALID_API_KEY',
      message: 'Invalid or missing API key',
      error: 'Invalid or missing API key',
    });
  }
  return next();
}

app.get('/auth/verify', apiKeyGuard, (req, res) => {
  res.json({ ok: true });
});

app.get('/healthz', (req, res) => res.json({ ok: true }));

app.use('/api', apiKeyGuard, pythonRoutes);
app.use('/tmp-upload', apiKeyGuard, tmpUploadRoutes);
app.use('/viz', apiKeyGuard, vizRoutes);
app.use('/viz/aggregate', apiKeyGuard, aggregateRoutes);
app.use('/stat', apiKeyGuard, statRoutes);

initializeWebSocket(server);

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

