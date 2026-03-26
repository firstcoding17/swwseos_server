const express = require('express');
const { createMcpRequestGuard } = require('../services/mcpRequestGuards');
const {
  buildInfoData,
  buildToolsData,
  runCallRequest,
  runChatRequest,
} = require('../services/mcpRouteHandlers');
const { executeTool } = require('../services/mcpToolExecutor');

const router = express.Router();

function sendError(res, error) {
  return res.status(error?.status || 500).json({
    ok: false,
    code: error?.code || 'MCP_ROUTE_ERROR',
    message: error?.exposeMessage || error?.message || 'Internal server error',
    details: error?.details,
    error: error?.exposeMessage || error?.message || 'Internal server error',
  });
}

async function runToolInternal(tool, input, keyHeader, datasetContext) {
  return executeTool({
    tool,
    input,
    keyHeader,
    datasetContext,
  });
}

router.use(createMcpRequestGuard());

router.get('/info', (req, res) => {
  return res.json({
    ok: true,
    data: buildInfoData(),
  });
});

router.get('/tools', (req, res) => {
  return res.json({
    ok: true,
    data: buildToolsData(),
  });
});

router.post('/call', async (req, res) => {
  try {
    const authHeaders = {
      'X-API-Key': req.get('X-API-Key') || '',
      'X-Client-Id': req.get('X-Client-Id') || '',
      'X-Session-Token': req.get('X-Session-Token') || '',
    };
    const data = await runCallRequest({
      tool: req.body?.tool,
      input: req.body?.input,
      datasetContext: req.body?.datasetContext,
      keyHeader: authHeaders,
      runToolInternal,
    });
    return res.json({ ok: true, data });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post('/chat', async (req, res) => {
  try {
    const authHeaders = {
      'X-API-Key': req.get('X-API-Key') || '',
      'X-Client-Id': req.get('X-Client-Id') || '',
      'X-Session-Token': req.get('X-Session-Token') || '',
    };
    const data = await runChatRequest({
      message: req.body?.message,
      datasetContext: req.body?.datasetContext,
      history: req.body?.history,
      keyHeader: authHeaders,
      runToolInternal,
    });
    return res.json({ ok: true, data });
  } catch (error) {
    return sendError(res, error);
  }
});

module.exports = router;
