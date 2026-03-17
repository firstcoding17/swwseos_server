const { TOOL_MAP, toolNames } = require('./mcpToolRegistry');
const {
  buildToolInput,
  runLocalTool,
  runUpstreamTool,
} = require('./mcpToolRuntime');

async function executeTool(options = {}) {
  const {
    tool,
    input,
    keyHeader,
    datasetContext = {},
    contextRowLimit = 120,
    toolRowLimit = 500,
  } = options;

  const target = TOOL_MAP[tool];
  if (!target) {
    const err = new Error(`unsupported tool: ${tool}`);
    err.status = 400;
    err.code = 'MCP_TOOL_NOT_FOUND';
    err.details = { available: toolNames() };
    throw err;
  }

  const hydratedInput = buildToolInput(tool, input, datasetContext, {
    contextRowLimit,
    toolRowLimit,
  });

  if (target.method === 'LOCAL') {
    return runLocalTool(tool, hydratedInput, keyHeader);
  }

  return runUpstreamTool(tool, target, hydratedInput, keyHeader);
}

module.exports = {
  executeTool,
};
