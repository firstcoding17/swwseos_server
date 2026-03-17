const express = require('express');

const router = express.Router();

// Issue a fake signed URL for temporary upload
router.post('/sign', async (req, res) => {
  const { name } = req.body || {};
  const safeName = name || 'unnamed.bin';
  const url = `https://example.com/fake-signed-url/${encodeURIComponent(safeName)}`;
  const key = `tmp/${Date.now()}-${safeName}`;
  const ttlSec = 3600;

  res.json({ ok: true, data: { url, key, ttlSec } });
});

// Delete temp object
router.post('/delete', async (req, res) => {
  const { key } = req.body || {};
  res.json({ ok: true, data: { key } });
});

module.exports = router;
