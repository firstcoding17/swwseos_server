const path = require('path');
const express = require('express');
const { spawn } = require('child_process');

const router = express.Router();
const SCRIPTS_DIR = path.resolve(__dirname, '../scripts');

router.post('/prepare', async (req, res) => {
  try {
    const py = spawn(process.env.PYTHON_BIN || 'python', ['viz_prepare.py'], {
      cwd: SCRIPTS_DIR,
      env: { ...process.env, PYTHONUTF8: '1' },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    const payload = JSON.stringify(req.body || {});
    py.stdin.write(payload);
    py.stdin.end();

    let out = '';
    let err = '';
    py.stdout.on('data', (d) => (out += d.toString()));
    py.stderr.on('data', (d) => (err += d.toString()));

    py.on('close', (code) => {
      if (code !== 0) {
        return res.status(500).json({
          ok: false,
          code: 'VIZ_PREPARE_FAILED',
          message: 'viz prepare failed',
          details: err,
          error: 'viz prepare failed',
        });
      }
      try {
        const obj = JSON.parse(out);
        return res.json({ ok: true, ...(obj || {}) });
      } catch {
        return res.status(500).json({
          ok: false,
          code: 'VIZ_PREPARE_INVALID_JSON',
          message: 'invalid json from python',
          details: out,
          error: 'invalid json from python',
        });
      }
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      code: 'VIZ_PREPARE_EXCEPTION',
      message: 'viz prepare exception',
      details: String(e),
      error: 'viz prepare exception',
    });
  }
});

module.exports = router;
