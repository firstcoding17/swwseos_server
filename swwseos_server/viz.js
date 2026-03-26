const crypto = require('crypto');

const pool = require('../config/db');



function hashApiKey(apiKey) {

  return crypto.createHash('sha256').update(apiKey).digest('hex');

}



async function apiKeyAuth(req, res, next) {

  try {

    const rawApiKey = req.header('X-API-Key');



    if (!rawApiKey) {

      return res.status(401).json({ error: 'API key is required' });

    }



    const keyHash = hashApiKey(rawApiKey);

    const keyPrefix = rawApiKey.slice(0, 12);



    const result = await pool.query(

      `

      SELECT *

      FROM api_keys

      WHERE key_prefix = $1

        AND key_hash = $2

      LIMIT 1

      `,

      [keyPrefix, keyHash]

    );



    if (result.rows.length === 0) {

      return res.status(401).json({ error: 'Invalid API key' });

    }



    const apiKeyRow = result.rows[0];



    if (apiKeyRow.status !== 'active') {

      return res.status(403).json({ error: 'API key is not active' });

    }



    if (apiKeyRow.expires_at && new Date(apiKeyRow.expires_at) < new Date()) {

      return res.status(403).json({ error: 'API key has expired' });

    }



    await pool.query(

      `

      UPDATE api_keys

      SET last_used_at = NOW(),

          last_ip = $1,

          last_user_agent = $2

      WHERE id = $3

      `,

      [req.ip, req.get('user-agent') || null, apiKeyRow.id]

    );



    req.apiKey = {

      id: apiKeyRow.id,

      ownerName: apiKeyRow.owner_name,

      label: apiKeyRow.label,

      minuteLimit: apiKeyRow.minute_limit,

      dailyLimit: apiKeyRow.daily_limit,

    };



    next();

  } catch (err) {

    console.error('apiKeyAuth error:', err);

    return res.status(500).json({ error: 'Internal server error' });

  }

}



module.exports = apiKeyAuth;
