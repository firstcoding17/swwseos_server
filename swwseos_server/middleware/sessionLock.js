const crypto = require('crypto');

const pool = require('../config/db');



const SESSION_TTL_SECONDS = 300; // 5분



function makeSessionToken() {

  return crypto.randomBytes(24).toString('hex');

}



async function sessionLock(req, res, next) {

  try {

    if (!req.apiKey?.id) {

      return res.status(500).json({ error: 'API key context missing' });

    }



    const apiKeyId = req.apiKey.id;

    const clientId = req.header('X-Client-Id');

    const userAgent = req.get('user-agent') || null;

    const ipAddress = req.ip || null;



    if (!clientId) {

      return res.status(400).json({ error: 'X-Client-Id is required' });

    }



    const current = await pool.query(

      `

      SELECT *

      FROM active_sessions

      WHERE api_key_id = $1

        AND expires_at > NOW()

      ORDER BY created_at DESC

      LIMIT 1

      `,

      [apiKeyId]

    );



    if (current.rows.length > 0) {

      const session = current.rows[0];



      if (session.client_id !== clientId) {

        return res.status(409).json({

          error: 'This API key is already in use on another client',

          code: 'SESSION_ALREADY_ACTIVE',

        });

      }



      req.sessionInfo = {

        sessionToken: session.session_token,

        clientId: session.client_id,

      };



      return next();

    }



    const sessionToken = makeSessionToken();



    await pool.query(

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

        $1, $2, $3, $4, $5,

        NOW() + ($6 || ' seconds')::interval

      )

      `,

      [apiKeyId, sessionToken, clientId, ipAddress, userAgent, SESSION_TTL_SECONDS]

    );



    req.sessionInfo = {

      sessionToken,

      clientId,

    };



    res.setHeader('X-Session-Token', sessionToken);



    next();

  } catch (err) {

    console.error('sessionLock error:', err);

    return res.status(500).json({ error: 'Internal server error' });

  }

}



module.exports = sessionLock;
