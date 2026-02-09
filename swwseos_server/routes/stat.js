const express = require('express');
const { spawn } = require('child_process');
const path = require('path');

const router = express.Router();
const SCRIPTS_DIR = path.resolve(__dirname, '../scripts');
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';

function runStat(payload, res) {
  try {
    const py = spawn(PYTHON_BIN, ['stat_run.py'], {
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
        console.error('[stat_run.py] exit', code, '\n', err);
        return res.status(500).json({
          ok: false,
          code: 'STAT_RUN_FAILED',
          message: 'stat run failed',
          details: err,
          error: 'stat run failed',
        });
      }
      try {
        const obj = JSON.parse(out);
        if (obj && typeof obj === 'object' && Object.prototype.hasOwnProperty.call(obj, 'ok')) {
          return res.json(obj);
        }
        return res.json({ ok: true, ...(obj || {}) });
      } catch {
        console.error('[stat_run.py] invalid JSON', out);
        return res.status(500).json({
          ok: false,
          code: 'STAT_RUN_INVALID_JSON',
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
      code: 'STAT_RUN_EXCEPTION',
      message: 'stat run exception',
      details: String(e),
      error: 'stat run exception',
    });
  }
}

router.get('/capabilities', async (_req, res) => {
  try {
    const py = spawn(
      PYTHON_BIN,
      [
        '-c',
        "import json, importlib.util as u; print(json.dumps({'ok': True, 'data': {'scipy': bool(u.find_spec('scipy')), 'statsmodels': bool(u.find_spec('statsmodels'))}}, ensure_ascii=False))",
      ],
      { env: { ...process.env, PYTHONUTF8: '1' }, cwd: SCRIPTS_DIR, stdio: ['ignore', 'pipe', 'pipe'] },
    );

    let out = '';
    let err = '';
    py.stdout.on('data', (d) => (out += d.toString()));
    py.stderr.on('data', (d) => (err += d.toString()));

    py.on('close', (code) => {
      if (code !== 0) {
        return res.status(500).json({
          ok: false,
          code: 'STAT_CAPABILITY_CHECK_FAILED',
          message: 'failed to check stat capabilities',
          details: err,
          error: 'failed to check stat capabilities',
        });
      }
      try {
        const obj = JSON.parse(out);
        return res.json(obj);
      } catch (e) {
        return res.status(500).json({
          ok: false,
          code: 'STAT_CAPABILITY_INVALID_JSON',
          message: 'invalid capability response from python',
          details: String(e),
          error: 'invalid capability response from python',
        });
      }
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      code: 'STAT_CAPABILITY_EXCEPTION',
      message: 'stat capability exception',
      details: String(e),
      error: 'stat capability exception',
    });
  }
});

function normalizeLegacyPayload(op, body) {
  const payload = body && typeof body === 'object' ? { ...body } : {};
  if (op === 'describe') return { ...payload, op: 'describe' };
  if (op === 'corr') return { ...payload, op: 'corr' };
  if (op === 'ttest') {
    return {
      ...payload,
      op: 'ttest',
      args: {
        value: payload.value ?? payload.valueCol ?? payload.args?.value,
        group: payload.group ?? payload.groupCol ?? payload.args?.group,
      },
    };
  }
  if (op === 'chisq') {
    return {
      ...payload,
      op: 'chisq',
      args: {
        a: payload.a ?? payload.colA ?? payload.args?.a,
        b: payload.b ?? payload.colB ?? payload.args?.b,
      },
    };
  }
  if (op === 'ols') {
    return {
      ...payload,
      op: 'ols',
      args: {
        y: payload.y ?? payload.target ?? payload.args?.y,
        x: payload.x ?? payload.features ?? payload.args?.x,
      },
      options: {
        ...(payload.options || {}),
        ...(payload.addIntercept !== undefined ? { addIntercept: payload.addIntercept } : {}),
        ...(payload.dummy !== undefined ? { dummy: payload.dummy } : {}),
        ...(payload.dropFirst !== undefined ? { dropFirst: payload.dropFirst } : {}),
        ...(payload.robust !== undefined ? { robust: payload.robust } : {}),
      },
    };
  }
  return payload;
}

router.post('/run', async (req, res) => runStat(req.body || {}, res));
router.post('/basic', async (req, res) => runStat(normalizeLegacyPayload('describe', req.body), res));
router.post('/summary', async (req, res) => runStat(normalizeLegacyPayload('describe', req.body), res));
router.post('/correlation', async (req, res) => runStat(normalizeLegacyPayload('corr', req.body), res));
router.post('/ttest', async (req, res) => runStat(normalizeLegacyPayload('ttest', req.body), res));
router.post('/chi2', async (req, res) => runStat(normalizeLegacyPayload('chisq', req.body), res));
router.post('/linreg', async (req, res) => runStat(normalizeLegacyPayload('ols', req.body), res));
router.post('/distribution', async (req, res) => {
  return res.status(410).json({
    ok: false,
    code: 'STAT_DISTRIBUTION_DEPRECATED',
    message: 'distribution endpoint is deprecated; use /stat/run',
    details: { op: 'describe|corr' },
    error: 'distribution endpoint is deprecated',
  });
});

module.exports = router;
