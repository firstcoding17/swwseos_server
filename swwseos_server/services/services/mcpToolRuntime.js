const { sampleRows } = require('./datasetProfiler');
const {
  buildProfileResult,
  buildFlagsResult,
  buildWorkspaceCurrentResult,
  buildWorkspaceListResult,
} = require('./mcpLocalDatasetTools');
const {
  buildWorkspaceCompareDescribeResult,
  buildWorkspaceCompareChartPlanResult,
  buildWorkspaceCompareStatDiffResult,
  buildWorkspaceRecommendAnalysisResult,
  buildWorkspaceFormalComparePlanResult,
} = require('./mcpWorkspaceCompareTools');

const CONTEXT_ONLY_TOOLS = new Set([
  'workspace.current_dataset',
  'workspace.list_datasets',
  'workspace.compare_describe',
  'workspace.compare_chart_plan',
  'workspace.compare_stat_diff',
  'workspace.recommend_analysis',
  'workspace.formal_compare_plan',
  'dataset.profile',
  'dataset.flags',
]);

const SAMPLE_ROW_TOOLS = new Set([
  'stat.run',
  'stat.recommend',
  'viz.prepare',
  'viz.aggregate',
  'ml.run',
]);

const COLUMN_AWARE_TOOLS = new Set([
  'viz.prepare',
  'viz.aggregate',
  'ml.run',
]);

function buildToolInput(tool, input, datasetContext, options = {}) {
  const contextRowLimit = Number(options.contextRowLimit || 120);
  const toolRowLimit = Number(options.toolRowLimit || 500);
  const payload = input && typeof input === 'object' ? { ...input } : {};
  if (tool === 'viz.aggregate' && !payload.spec) {
    payload.spec = { type: 'bar' };
  }
  if (tool === 'stat.recommend') payload.op = 'recommend';
  const sample = sampleRows(datasetContext?.sampleRows || datasetContext?.rows || [], contextRowLimit);
  if (CONTEXT_ONLY_TOOLS.has(tool) && !Object.keys(payload).length) {
    return { ...(datasetContext || {}), sampleRows: sample };
  }
  if (SAMPLE_ROW_TOOLS.has(tool) && !payload.rows && sample.length) {
    payload.rows = sample;
  }
  if (Array.isArray(payload.rows)) {
    payload.rows = sampleRows(payload.rows, toolRowLimit);
  }
  if (COLUMN_AWARE_TOOLS.has(tool) && !payload.columns && Array.isArray(datasetContext?.columns)) {
    payload.columns = datasetContext.columns;
  }
  return payload;
}

async function runLocalTool(tool, hydratedInput, keyHeader) {
  if (tool === 'workspace.current_dataset') return buildWorkspaceCurrentResult(hydratedInput);
  if (tool === 'workspace.list_datasets') return buildWorkspaceListResult(hydratedInput);
  if (tool === 'workspace.compare_describe') return buildWorkspaceCompareDescribeResult(hydratedInput, keyHeader);
  if (tool === 'workspace.compare_chart_plan') return buildWorkspaceCompareChartPlanResult(hydratedInput);
  if (tool === 'workspace.compare_stat_diff') return buildWorkspaceCompareStatDiffResult(hydratedInput);
  if (tool === 'workspace.recommend_analysis') return buildWorkspaceRecommendAnalysisResult(hydratedInput);
  if (tool === 'workspace.formal_compare_plan') return buildWorkspaceFormalComparePlanResult(hydratedInput);
  if (tool === 'dataset.profile') return buildProfileResult(hydratedInput);
  if (tool === 'dataset.flags') return buildFlagsResult(hydratedInput);
  return { ok: false, code: 'MCP_LOCAL_HANDLER_MISSING', message: 'local handler missing' };
}

function baseUrl() {
  return process.env.MCP_INTERNAL_BASE || `http://127.0.0.1:${process.env.PORT || 5000}`;
}

async function runUpstreamTool(tool, target, hydratedInput, keyHeader) {
  if (typeof fetch !== 'function') {
    const err = new Error('server runtime fetch is unavailable');
    err.status = 500;
    err.code = 'MCP_FETCH_UNAVAILABLE';
    throw err;
  }

  const headers = { 'Content-Type': 'application/json' };
  if (keyHeader && typeof keyHeader === 'object') {
    if (keyHeader['X-API-Key']) headers['X-API-Key'] = keyHeader['X-API-Key'];
    if (keyHeader['X-Client-Id']) headers['X-Client-Id'] = keyHeader['X-Client-Id'];
    if (keyHeader['X-Session-Token']) headers['X-Session-Token'] = keyHeader['X-Session-Token'];
  } else if (keyHeader) {
    headers['X-API-Key'] = keyHeader;
  }

  const url = `${baseUrl()}${target.path}`;
  const upstream = await fetch(url, {
    method: target.method,
    headers,
    ...(target.method === 'POST' ? { body: JSON.stringify(hydratedInput || {}) } : {}),
  });

  const text = await upstream.text();
  let parsed = {};
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch {
    parsed = { ok: false, code: 'UPSTREAM_INVALID_JSON', message: 'invalid upstream response', details: text };
  }

  if (!upstream.ok) {
    const err = new Error(`tool call failed: ${tool}`);
    err.status = upstream.status;
    err.code = 'MCP_TOOL_FAILED';
    err.details = parsed;
    throw err;
  }

  return parsed;
}

module.exports = {
  buildToolInput,
  runLocalTool,
  runUpstreamTool,
};
