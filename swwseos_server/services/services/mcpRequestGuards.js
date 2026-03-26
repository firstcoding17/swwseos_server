const { sampleRows } = require('./datasetProfiler');

function sanitizeHistory(history, historyLimit) {
  return Array.isArray(history) ? history.slice(-historyLimit) : [];
}

function sanitizeDatasetContext(datasetContext, options = {}) {
  const contextRowLimit = Number(options.contextRowLimit || 120);
  const workspaceDatasetLimit = Number(options.workspaceDatasetLimit || 4);
  const source = datasetContext && typeof datasetContext === 'object' ? datasetContext : {};
  const workspaceDatasets = Array.isArray(source.workspaceDatasets)
    ? source.workspaceDatasets.slice(0, workspaceDatasetLimit).map((dataset) => ({
      ...(dataset && typeof dataset === 'object' ? dataset : {}),
      sampleRows: sampleRows(dataset?.sampleRows || [], contextRowLimit),
      columns: Array.isArray(dataset?.columns) ? dataset.columns.slice(0, 200) : [],
    }))
    : [];
  return {
    ...source,
    rows: sampleRows(source.rows || [], contextRowLimit),
    sampleRows: sampleRows(source.sampleRows || source.rows || [], contextRowLimit),
    columns: Array.isArray(source.columns) ? source.columns.slice(0, 200) : [],
    workspaceDatasets,
  };
}

function toolRowCount(input) {
  if (!input || typeof input !== 'object') return 0;
  if (Array.isArray(input.rows)) return input.rows.length;
  return 0;
}

function createMcpRequestGuard(options = {}) {
  const contextRowLimit = Number(options.contextRowLimit || 120);
  const toolRowLimit = Number(options.toolRowLimit || 500);
  const workspaceDatasetLimit = Number(options.workspaceDatasetLimit || 4);
  const historyLimit = Number(options.historyLimit || 8);
  const messageCharLimit = Number(options.messageCharLimit || 2000);

  return function mcpRequestGuard(req, res, next) {
    if (req.method !== 'POST') return next();
    if (req.path === '/chat') {
      const message = String(req.body?.message || '');
      if (message.length > messageCharLimit) {
        return res.status(413).json({
          ok: false,
          code: 'MCP_CHAT_MESSAGE_TOO_LARGE',
          message: `message is too large (max ${messageCharLimit} chars)`,
          error: 'message is too large',
        });
      }
      req.body = {
        ...(req.body || {}),
        history: sanitizeHistory(req.body?.history, historyLimit),
        datasetContext: sanitizeDatasetContext(req.body?.datasetContext, {
          contextRowLimit,
          workspaceDatasetLimit,
        }),
      };
      return next();
    }
    if (req.path === '/call') {
      if (toolRowCount(req.body?.input) > toolRowLimit) {
        return res.status(413).json({
          ok: false,
          code: 'MCP_TOOL_ROWS_LIMIT',
          message: `tool input rows exceed limit (${toolRowLimit})`,
          error: 'tool input rows exceed limit',
          details: { maxRows: toolRowLimit, rowCount: toolRowCount(req.body?.input) },
        });
      }
      req.body = {
        ...(req.body || {}),
        datasetContext: sanitizeDatasetContext(req.body?.datasetContext, {
          contextRowLimit,
          workspaceDatasetLimit,
        }),
      };
    }
    return next();
  };
}

module.exports = {
  createMcpRequestGuard,
};
