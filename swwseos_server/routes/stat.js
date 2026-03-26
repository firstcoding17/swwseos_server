const express = require('express');
const { runPythonJson } = require('../lib/runPythonJson');

const router = express.Router();

router.get('/capabilities', async (_req, res) => {
  try {
    const data = await runPythonJson('scripts/stat_run.py', { op: 'capabilities' });
    return res.json(data);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      code: 'STAT_CAPABILITIES_ERROR',
      message: error.message,
    });
  }
});

router.post('/run', async (req, res) => {
  try {
    const data = await runPythonJson('scripts/stat_run.py', req.body || {});
    return res.json(data);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      code: 'STAT_RUN_ERROR',
      message: error.message,
    });
  }
});

module.exports = router;
