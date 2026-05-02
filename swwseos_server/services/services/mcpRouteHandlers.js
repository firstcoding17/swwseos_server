const { isClaudeConfigured, runClaudeChat } = require('./claudeClient');
const {
  buildRuleBasedChatData,
  enrichRuleBasedChatDataWithStatsRecommendation,
  buildClaudeChatSuccessData,
  buildClaudeFallbackChatData,
} = require('./mcpOrchestrator');
const {
  toolList,
  toolNames,
} = require('./mcpToolRegistry');

function buildInfoData() {
  return {
    name: 'swwseos-mcp-bridge',
    protocol: 'mcp-compat-v1',
    tools: toolNames(),
  };
}

function buildToolsData() {
  return {
    tools: toolList(),
  };
}

async function runChatRequest(options = {}) {
  const {
    message,
    datasetContext = {},
    history = [],
    keyHeader,
    runToolInternal,
  } = options;
  const trimmedMessage = String(message || '').trim();
  if (!trimmedMessage) {
    const err = new Error('message is required');
    err.status = 400;
    err.code = 'MCP_CHAT_MESSAGE_REQUIRED';
    err.exposeMessage = 'message is required';
    throw err;
  }

  if (!isClaudeConfigured()) {
    return enrichRuleBasedChatDataWithStatsRecommendation({
      data: buildRuleBasedChatData(trimmedMessage, datasetContext, history),
      message: trimmedMessage,
      datasetContext,
      keyHeader,
      runToolInternal,
    });
  }

  const fallbackData = buildRuleBasedChatData(trimmedMessage, datasetContext, history);

  try {
    const claudeData = await runClaudeChat({
      message: trimmedMessage,
      datasetContext,
      history,
      toolDefinitions: toolList(),
      toolRunner: (tool, input) => runToolInternal(tool, input, keyHeader, datasetContext),
    });
    return buildClaudeChatSuccessData(fallbackData, claudeData);
  } catch (claudeError) {
    const enrichedFallbackData = await enrichRuleBasedChatDataWithStatsRecommendation({
      data: fallbackData,
      message: trimmedMessage,
      datasetContext,
      keyHeader,
      runToolInternal,
    });
    return buildClaudeFallbackChatData(enrichedFallbackData, claudeError);
  }
}

async function runCallRequest(options = {}) {
  const {
    tool,
    input,
    keyHeader,
    datasetContext = {},
    runToolInternal,
  } = options;
  const trimmedTool = String(tool || '').trim();
  const result = await runToolInternal(trimmedTool, input, keyHeader, datasetContext);
  return {
    tool: trimmedTool,
    result,
  };
}

module.exports = {
  buildInfoData,
  buildToolsData,
  runChatRequest,
  runCallRequest,
};
