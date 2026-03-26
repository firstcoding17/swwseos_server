const express = require('express');
const fs = require('fs/promises');
const path = require('path');

const router = express.Router();
const TMP_ROOT = path.join(__dirname, '..', 'tmp', 'uploads');

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

function normalizeKey(rawKey) {
  const key = String(rawKey || '').trim();
  if (!key.startsWith('tmp/')) {
    throw new Error('temporary object key must start with tmp/');
  }
  if (key.includes('..')) {
    throw new Error('invalid temporary object key');
  }
  return key;
}

function tempPathForKey(key) {
  const safeKey = normalizeKey(key);
  const resolved = path.resolve(TMP_ROOT, safeKey.replace(/\//g, path.sep));
  const root = path.resolve(TMP_ROOT);
  if (!resolved.startsWith(root)) {
    throw new Error('temporary object path escaped root');
  }
  return resolved;
}

router.post('/sign', async (req, res) => {
  try {
    const name = String(req.body?.name || 'upload.bin').trim() || 'upload.bin';
    const timestamp = Date.now();
    const key = `tmp/${timestamp}-${name.replace(/[^\w.\- ]+/g, '_').trim().replace(/\s+/g, '_')}`;
    const ttlSec = Number(process.env.TMP_UPLOAD_TTL_SEC || 3600);
    return res.json({
      ok: true,
      data: {
        key,
        ttlSec,
        url: `/tmp-upload/object/${encodeURIComponent(key)}?filename=${encodeURIComponent(name)}`,
      },
    });
  } catch (error) {
    return res.status(400).json({
      ok: false,
      code: 'TMP_UPLOAD_SIGN_ERROR',
      message: error.message,
    });
  }
});

router.put('/object/*', express.raw({ type: '*/*', limit: '50mb' }), async (req, res) => {
  try {
    const key = decodeURIComponent(req.params[0] || '');
    const targetPath = tempPathForKey(key);
    await ensureDir(path.dirname(targetPath));
    await fs.writeFile(targetPath, req.body || Buffer.from(''));
    return res.status(200).end();
  } catch (error) {
    return res.status(400).json({
      ok: false,
      code: 'TMP_UPLOAD_PUT_ERROR',
      message: error.message,
    });
  }
});

router.post('/delete', async (req, res) => {
  try {
    const key = normalizeKey(req.body?.key);
    const targetPath = tempPathForKey(key);
    await fs.rm(targetPath, { force: true });
    return res.json({
      ok: true,
      data: { key },
    });
  } catch (error) {
    return res.status(400).json({
      ok: false,
      code: 'TMP_UPLOAD_DELETE_ERROR',
      message: error.message,
    });
  }
});

module.exports = router;
