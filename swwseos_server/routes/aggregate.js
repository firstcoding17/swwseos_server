const express = require('express');
const { aggregateForSpec } = require('../services/vizEngine');

const router = express.Router();

router.post('/', async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    const spec = req.body?.spec || {};
    const { result, op } = aggregateForSpec(rows, spec);
    return res.json({
      ok: true,
      data: {
        result,
        meta: {
          op,
          rowsUsed: rows.length,
        },
      },
    });
  } catch (error) {
    return res.status(400).json({
      ok: false,
      code: 'VIZ_AGGREGATE_ERROR',
      message: error.message,
    });
  }
});

module.exports = router;
