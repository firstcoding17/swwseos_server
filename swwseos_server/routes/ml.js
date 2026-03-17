const express = require('express');
const { spawn } = require('child_process');
const path = require('path');

const router = express.Router();
const SCRIPTS_DIR = path.resolve(__dirname, '../scripts');
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';

function runMl(payload, res, statusOnLogicalFail = 400) {
  try {
    const py = spawn(PYTHON_BIN, ['ml_run.py'], {
      cwd: SCRIPTS_DIR,
      env: { ...process.env, PYTHONUTF8: '1' },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    py.stdin.write(JSON.stringify(payload || {}));
    py.stdin.end();

    let out = '';
    let err = '';
    py.stdout.on('data', (d) => (out += d.toString()));
    py.stderr.on('data', (d) => (err += d.toString()));

    py.on('close', (code) => {
      if (code !== 0) {
        return res.status(500).json({
          ok: false,
          code: 'ML_RUN_FAILED',
          message: 'ml run failed',
          details: err || out,
        });
      }
      try {
        const obj = JSON.parse(out || '{}');
        if (obj && obj.ok === false) return res.status(statusOnLogicalFail).json(obj);
        return res.json(obj && typeof obj === 'object' ? obj : { ok: true, data: obj });
      } catch (e) {
        return res.status(500).json({
          ok: false,
          code: 'ML_RUN_INVALID_JSON',
          message: 'invalid json from python',
          details: String(e),
        });
      }
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      code: 'ML_RUN_EXCEPTION',
      message: 'ml run exception',
      details: String(e),
    });
  }
}

router.get('/capabilities', async (_req, res) => {
  return runMl({ op: 'capabilities' }, res, 500);
});

router.post('/run', async (req, res) => {
  return runMl(req.body || {}, res);
});

module.exports = router;
