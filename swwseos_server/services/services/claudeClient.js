const ANTHROPIC_API_URL = 'https://api.anthropic.com/v1/messages';
const ANTHROPIC_VERSION = '2023-06-01';
const DEFAULT_MAX_TURNS = 4;

function getClaudeConfig() {
  return {
    apiKey: String(process.env.ANTHROPIC_API_KEY || '').trim(),
    model: String(process.env.CLAUDE_MODEL || 'claude-sonnet-4-20250514').trim(),
    maxTokens: Number(process.env.CLAUDE_MAX_TOKENS || 1200),
  };
}

function isClaudeConfigured() {
  return !!getClaudeConfig().apiKey;
}

function toTextBlocks(text) {
  return [{ type: 'text', text: String(text || '') }];
}

function normalizeHistory(history) {
  if (!Array.isArray(history)) return [];
  return history
    .filter((item) => item && (item.role === 'user' || item.role === 'assistant') && item.text)
    .slice(-8)
    .map((item) => ({
      role: item.role,
      content: toTextBlocks(item.text),
    }));
}

function buildSystemPrompt() {
  return [
    'You are an analytics planning assistant embedded in a data analysis product.',
    'Use available tools when needed.',
    'Prefer concise, practical answers.',
    'Do not claim to inspect data that was not provided.',
    'Use tool calls for dataset profile, flags, statistics, chart preparation, and ML capabilities.',
    'If the request can be answered from provided context alone, answer directly.',
    'When tools are used, summarize the result in plain language.',
  ].join(' ');
}

async function postAnthropic(body, fetchImpl) {
  const config = getClaudeConfig();
  const res = await fetchImpl(ANTHROPIC_API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': config.apiKey,
      'anthropic-version': ANTHROPIC_VERSION,
    },
    body: JSON.stringify(body),
  });

  const text = await res.text();
  let parsed = {};
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`Anthropic returned invalid JSON: ${text}`);
  }
  if (!res.ok) {
    const message = parsed?.error?.message || parsed?.message || `Anthropic request failed (${res.status})`;
    throw new Error(message);
  }
  return parsed;
}

function extractAssistantText(content) {
  if (!Array.isArray(content)) return '';
  return content
    .filter((block) => block && block.type === 'text' && block.text)
    .map((block) => block.text)
    .join('\n')
    .trim();
}

function extractToolUses(content) {
  if (!Array.isArray(content)) return [];
  return content.filter((block) => block && block.type === 'tool_use' && block.id && block.name);
}

function buildClaudeTools(toolDefinitions) {
  return (toolDefinitions || []).map((tool) => ({
    name: tool.name,
    description: tool.description || '',
    input_schema: tool.input_schema || {
      type: 'object',
      properties: {},
      additionalProperties: true,
    },
  }));
}

async function runClaudeChat(options) {
  const fetchImpl = options?.fetchImpl || global.fetch;
  if (typeof fetchImpl !== 'function') {
    throw new Error('Fetch is unavailable for Claude client.');
  }

  const config = getClaudeConfig();
  if (!config.apiKey) {
    throw new Error('Anthropic API key is not configured.');
  }

  const toolRunner = options?.toolRunner;
  if (typeof toolRunner !== 'function') {
    throw new Error('Claude tool runner is required.');
  }

  const toolDefinitions = Array.isArray(options?.toolDefinitions) ? options.toolDefinitions : [];
  const datasetContext = options?.datasetContext || {};
  const history = normalizeHistory(options?.history || []);
  const message = String(options?.message || '').trim();
  if (!message) {
    throw new Error('Claude chat message is required.');
  }

  const contextSummary = {
    datasetName: datasetContext.datasetName || 'untitled',
    datasetId: datasetContext.datasetId || '',
    rowCount: datasetContext.rowCount || 0,
    columnCount: datasetContext.columnCount || 0,
    columns: datasetContext.columns || [],
    profileSummary: datasetContext.profileSummary || {},
    sampleRows: Array.isArray(datasetContext.sampleRows) ? datasetContext.sampleRows : [],
  };

  const messages = history.slice();
  messages.push({
    role: 'user',
    content: toTextBlocks(
      [
        `User question: ${message}`,
        '',
        'Dataset context:',
        JSON.stringify(contextSummary),
      ].join('\n')
    ),
  });

  const tools = buildClaudeTools(toolDefinitions);
  const executedTools = [];

  for (let turn = 0; turn < DEFAULT_MAX_TURNS; turn += 1) {
    const response = await postAnthropic(
      {
        model: config.model,
        max_tokens: config.maxTokens,
        system: buildSystemPrompt(),
        messages,
        tools,
      },
      fetchImpl
    );

    const content = Array.isArray(response?.content) ? response.content : [];
    const toolUses = extractToolUses(content);
    const assistantText = extractAssistantText(content);

    if (!toolUses.length) {
      return {
        mode: 'claude',
        reply: assistantText || 'Claude returned an empty answer.',
        toolCalls: executedTools,
      };
    }

    messages.push({
      role: 'assistant',
      content,
    });

    const toolResults = [];
    for (const toolUse of toolUses) {
      const toolResult = await toolRunner(toolUse.name, toolUse.input || {});
      executedTools.push({
        tool: toolUse.name,
        input: toolUse.input || {},
      });
      toolResults.push({
        type: 'tool_result',
        tool_use_id: toolUse.id,
        content: JSON.stringify(toolResult || {}),
      });
    }

    messages.push({
      role: 'user',
      content: toolResults,
    });
  }

  throw new Error('Claude tool-use loop exceeded max turns.');
}

module.exports = {
  getClaudeConfig,
  isClaudeConfigured,
  runClaudeChat,
};
