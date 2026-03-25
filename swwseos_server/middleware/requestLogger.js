const pool = require('../config/db');



function resolveFeatureCode(req) {

  const path = req.baseUrl + req.path;



  if (path.startsWith('/stat')) return 'stat';

  if (path.startsWith('/viz/aggregate')) return 'viz_aggregate';

  if (path.startsWith('/viz')) return 'viz';

  if (path.startsWith('/tmp-upload')) return 'tmp_upload';

  if (path.startsWith('/ml')) return 'ml';

  if (path.startsWith('/mcp')) return 'mcp';

  if (path.startsWith('/api')) return 'api';

  if (path.startsWith('/auth/verify')) return 'auth_verify';



  return 'unknown';

}



function requestLogger(req, res, next) {

  const start = Date.now();



  res.on('finish', async () => {

    try {

      if (!req.apiKey?.id) return;



      const responseTimeMs = Date.now() - start;

      const featureCode = resolveFeatureCode(req);



      await pool.query(

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

          req.apiKey.id,

          featureCode,

          req.originalUrl,

          req.method,

          res.statusCode,

          responseTimeMs,

          req.ip || null,

          req.get('user-agent') || null,

        ]

      );

    } catch (err) {

      console.error('requestLogger error:', err);

    }

  });



  next();

}



module.exports = requestLogger;
