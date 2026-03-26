const express = require('express');
const { runPythonJson } = require('../lib/runPythonJson');

const router = express.Router();

async function runMl(payload) {
  return runPythonJson(pathJoin('scripts', 'ml_run.py'), payload);
}

function pathJoin(...parts) {
  return parts.join('/');
}

router.get('/capabilities', async (_req, res) => {
  try {
    const data = await runMl({ op: 'capabilities' });
    return res.json(data);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      code: 'ML_CAPABILITIES_ERROR',
      message: error.message,
    });
  }
});

router.post('/run', async (req, res) => {
  try {
    const payload = {
      ...(req.body || {}),
      op: req.body?.op || 'train',
    };
    const data = await runMl(payload);
    return res.json(data);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      code: 'ML_RUN_ERROR',
      message: error.message,
    });
  }
});

module.exports = router;
