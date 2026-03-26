require('dotenv').config();

const crypto = require('crypto');

const pool = require('../config/db');



function generateApiKey() {

  return 'ngnl_' + crypto.randomBytes(24).toString('hex');

}



function hashApiKey(apiKey) {

  return crypto.createHash('sha256').update(apiKey).digest('hex');

}



async function createApiKey() {

  const ownerName = process.argv[2] || 'test-user';

  const label = process.argv[3] || 'default';

  const expiresDays = Number(process.argv[4] || 30);



  const apiKey = generateApiKey();

  const keyHash = hashApiKey(apiKey);

  const keyPrefix = apiKey.slice(0, 12);

  const expiresAt = new Date();

  expiresAt.setDate(expiresAt.getDate() + expiresDays);



  try {

    const result = await pool.query(

      `

      INSERT INTO api_keys (

        key_prefix,

        key_hash,

        owner_name,

        label,

        status,

        expires_at,

        minute_limit,

        daily_limit

      )

      VALUES ($1, $2, $3, $4, 'active', $5, 60, 1000)

      RETURNING id, owner_name, label, expires_at, created_at

      `,

      [keyPrefix, keyHash, ownerName, label, expiresAt]

    );



    console.log('API key created successfully');

    console.log('--------------------------------');

    console.log('id         :', result.rows[0].id);

    console.log('owner_name :', result.rows[0].owner_name);

    console.log('label      :', result.rows[0].label);

    console.log('expires_at :', result.rows[0].expires_at);

    console.log('created_at :', result.rows[0].created_at);

    console.log('--------------------------------');

    console.log('IMPORTANT: copy this API key now. It will not be shown again.');

    console.log('API_KEY =', apiKey);

  } catch (err) {

    console.error('Failed to create API key:', err);

  } finally {

    await pool.end();

  }

}



createApiKey();
