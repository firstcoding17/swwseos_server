const pool = require('../config/db');

function featureCodeFromPath(pathname) {
  const path = String(pathname || '');
  if (path.startsWith('/auth/verify')) return 'auth.verify';
  if (path.startsWith('/api')) return 'api';
  if (path.startsWith('/tmp-upload')) return 'tmp-upload';
  if (path.startsWith('/viz/aggregate')) return 'viz.aggregate';
  if (path.startsWith('/viz')) return 'viz';
  if (path.startsWith('/stat')) return 'stat';
  if (path.startsWith('/ml')) return 'ml';
  if (path.startsWith('/mcp')) return 'mcp';
  return 'unknown';
}

module.exports = function requestLogger(req, res, next) {
  if (res.locals.__usageLogAttached) return next();
  res.locals.__usageLogAttached = true;

  const startedAt = Date.now();
  res.on('finish', () => {
    const apiKeyId = req.apiKey?.id || null;
    const endpoint = String(req.originalUrl || req.path || '').split('?')[0] || '/';
    const featureCode = featureCodeFromPath(endpoint);
    const responseTimeMs = Date.now() - startedAt;

    pool.query(
      `
        INSERT INTO usage_logs (
          api_key_id,
          feature_code,
          endpoint,
          method,
          status_code,
          response_time_ms,
          ip_address,
          user_agent
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
      [
        apiKeyId,
        featureCode,
        endpoint,
        req.method,
        res.statusCode,
        responseTimeMs,
        String(req.ip || '').replace(/^::ffff:/, '') || null,
        req.get('user-agent') || null,
      ]
    ).catch((error) => {
      console.error('requestLogger insert error:', error);
    });
  });

  return next();
};
