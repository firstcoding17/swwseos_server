const express = require('express');
const { spawn } = require('child_process');
const path = require('path');

const router = express.Router();
const SCRIPTS_DIR = path.resolve(__dirname, '../scripts');

router.post('/', async (req, res) => {
  try {
    const py = spawn(process.env.PYTHON_BIN || 'python', ['aggregate.py'], {
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
        console.error('[aggregate.py] exit', code, '\n', err);
        return res.status(500).json({
          ok: false,
          code: 'AGGREGATE_FAILED',
          message: 'aggregate failed',
          details: err,
          error: 'aggregate failed',
        });
      }
      try {
        const obj = JSON.parse(out);
        if (obj && obj.ok === false) return res.status(400).json(obj);
        return res.json({ ok: true, data: obj || {} });
      } catch {
        console.error('[aggregate.py] invalid JSON', out);
        return res.status(500).json({
          ok: false,
          code: 'AGGREGATE_INVALID_JSON',
          message: 'invalid json from python',
          details: out,
          error: 'invalid json from python',
        });
      }
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({
      ok: false,
      code: 'AGGREGATE_EXCEPTION',
      message: 'aggregate exception',
      details: String(e),
      error: 'aggregate exception',
    });
  }
});

module.exports = router;
