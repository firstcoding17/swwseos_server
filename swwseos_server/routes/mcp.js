const express = require('express');
const {
  createMcpRequestGuard,
} = require('../services/mcpRequestGuards');
const {
  executeTool,
} = require('../services/mcpToolExecutor');
const {
  buildInfoData,
  buildToolsData,
  runChatRequest,
  runCallRequest,
} = require('../services/mcpRouteHandlers');

const router = express.Router();
const MCP_MAX_CONTEXT_ROWS = 120;
const MCP_MAX_TOOL_ROWS = 500;
const MCP_MAX_WORKSPACE_DATASETS = 4;
const MCP_MAX_HISTORY_MESSAGES = 8;
const MCP_MAX_MESSAGE_CHARS = 2000;

router.use(createMcpRequestGuard({
  contextRowLimit: MCP_MAX_CONTEXT_ROWS,
  toolRowLimit: MCP_MAX_TOOL_ROWS,
  workspaceDatasetLimit: MCP_MAX_WORKSPACE_DATASETS,
  historyLimit: MCP_MAX_HISTORY_MESSAGES,
  messageCharLimit: MCP_MAX_MESSAGE_CHARS,
}));

const runToolInternal = (tool, input, keyHeader, datasetContext = {}) =>
  executeTool({
    tool,
    input,
    keyHeader,
    datasetContext,
    contextRowLimit: MCP_MAX_CONTEXT_ROWS,
    toolRowLimit: MCP_MAX_TOOL_ROWS,
  });

router.get('/info', (_req, res) => {
  return res.json({ ok: true, data: buildInfoData() });
});

router.get('/tools', (_req, res) => {
  return res.json({ ok: true, data: buildToolsData() });
});

router.post('/chat', async (req, res) => {
  try {
    const data = await runChatRequest({
      message: req.body?.message,
      datasetContext: req.body?.datasetContext || {},
      history: req.body?.history || [],
      keyHeader: req.header('X-API-Key'),
      runToolInternal,
    });
    return res.json({ ok: true, data });
  } catch (e) {
    return res.status(e.status || 500).json({
      ok: false,
      code: e.code || 'MCP_CHAT_EXCEPTION',
      message: e.exposeMessage || e.message || 'mcp chat exception',
      details: e.details || String(e),
      error: 'mcp chat exception',
    });
  }
});

router.post('/call', async (req, res) => {
  try {
    const data = await runCallRequest({
      tool: req.body?.tool,
      input: req.body?.input,
      keyHeader: req.header('X-API-Key'),
      datasetContext: req.body?.datasetContext || {},
      runToolInternal,
    });
    return res.json({ ok: true, data });
  } catch (e) {
    return res.status(e.status || 500).json({
      ok: false,
      code: e.code || 'MCP_CALL_EXCEPTION',
      message: e.message || 'mcp call exception',
      details: e.details || String(e),
      error: 'mcp call exception',
    });
  }
});

module.exports = router;
