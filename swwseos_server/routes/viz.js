const express = require('express');
const { buildPreparedFigure } = require('../services/vizEngine');

const router = express.Router();

router.post('/prepare', async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    const spec = req.body?.spec || {};
    const prepared = buildPreparedFigure(rows, spec);
    return res.json({
      ok: true,
      fig_json: prepared.fig_json,
      data: {
        fig_json: prepared.fig_json,
        rowsUsed: rows.length,
      },
    });
  } catch (error) {
    return res.status(400).json({
      ok: false,
      code: 'VIZ_PREPARE_ERROR',
      message: error.message,
    });
  }
});

module.exports = router;
