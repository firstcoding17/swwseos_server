const TOOL_MAP = {
  'workspace.current_dataset': {
    method: 'LOCAL',
    path: 'workspace.current_dataset',
    description: 'Return the current active dataset summary from workspace context.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'workspace.list_datasets': {
    method: 'LOCAL',
    path: 'workspace.list_datasets',
    description: 'List open workspace datasets and shared columns.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'workspace.compare_describe': {
    method: 'LOCAL',
    path: 'workspace.compare_describe',
    description: 'Run capped descriptive comparisons across open workspace datasets.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'workspace.compare_chart_plan': {
    method: 'LOCAL',
    path: 'workspace.compare_chart_plan',
    description: 'Build comparison chart specs across open workspace datasets.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'workspace.compare_stat_diff': {
    method: 'LOCAL',
    path: 'workspace.compare_stat_diff',
    description: 'Summarize statistical differences across open workspace datasets using capped samples.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'workspace.recommend_analysis': {
    method: 'LOCAL',
    path: 'workspace.recommend_analysis',
    description: 'Prioritize datasets and next analyses across the open workspace.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'workspace.formal_compare_plan': {
    method: 'LOCAL',
    path: 'workspace.formal_compare_plan',
    description: 'Plan aligned formal comparisons and cautions across open workspace datasets.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'dataset.profile': {
    method: 'LOCAL',
    path: 'dataset.profile',
    description: 'Build dataset profile summary from provided context.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'dataset.flags': {
    method: 'LOCAL',
    path: 'dataset.flags',
    description: 'Detect notable dataset warnings and red flags.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'health.check': {
    method: 'GET',
    path: '/healthz',
    description: 'Check server health.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: false },
  },
  'stat.capabilities': {
    method: 'GET',
    path: '/stat/capabilities',
    description: 'List statistics backend capabilities.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: false },
  },
  'ml.capabilities': {
    method: 'GET',
    path: '/ml/capabilities',
    description: 'List ML backend capabilities.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: false },
  },
  'stat.run': {
    method: 'POST',
    path: '/stat/run',
    description: 'Run a statistics operation.',
    safe: true,
    input_schema: { type: 'object', properties: { op: { type: 'string' } }, additionalProperties: true },
  },
  'stat.recommend': {
    method: 'POST',
    path: '/stat/run',
    description: 'Run the statistics recommendation engine for the current dataset.',
    safe: true,
    input_schema: { type: 'object', properties: { args: { type: 'object' } }, additionalProperties: true },
  },
  'viz.prepare': {
    method: 'POST',
    path: '/viz/prepare',
    description: 'Prepare chart-ready visualization payloads.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'viz.aggregate': {
    method: 'POST',
    path: '/viz/aggregate',
    description: 'Aggregate data for charting.',
    safe: true,
    input_schema: { type: 'object', properties: {}, additionalProperties: true },
  },
  'ml.run': {
    method: 'POST',
    path: '/ml/run',
    description: 'Run ML or time-series workflows.',
    safe: false,
    input_schema: { type: 'object', properties: { task: { type: 'string' }, model: { type: 'string' } }, additionalProperties: true },
  },
};

function toolList() {
  return Object.entries(TOOL_MAP).map(([name, meta]) => ({
    name,
    method: meta.method,
    path: meta.path,
    description: meta.description || '',
    safe: meta.safe !== false,
    input_schema: meta.input_schema || { type: 'object', properties: {}, additionalProperties: true },
  }));
}

function toolNames() {
  return Object.keys(TOOL_MAP);
}

module.exports = {
  TOOL_MAP,
  toolList,
  toolNames,
};
