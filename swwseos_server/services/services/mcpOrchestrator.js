const { buildFacts } = require('./datasetProfiler');

function cloneChatData(data, fallback = {}) {
  return {
    ...(fallback && typeof fallback === 'object' ? fallback : {}),
    ...(data && typeof data === 'object' ? data : {}),
    cards: Array.isArray(data?.cards) ? [...data.cards] : Array.isArray(fallback?.cards) ? [...fallback.cards] : [],
    suggestions: Array.isArray(data?.suggestions) ? [...data.suggestions] : Array.isArray(fallback?.suggestions) ? [...fallback.suggestions] : [],
    warnings: Array.isArray(data?.warnings) ? [...data.warnings] : Array.isArray(fallback?.warnings) ? [...fallback.warnings] : [],
    toolCalls: Array.isArray(data?.toolCalls) ? [...data.toolCalls] : Array.isArray(fallback?.toolCalls) ? [...fallback.toolCalls] : [],
  };
}

function historyText(history = []) {
  return Array.isArray(history)
    ? history
      .slice(-4)
      .map((entry) => String(entry?.text || entry?.content || '').trim())
      .filter(Boolean)
      .join('\n')
    : '';
}

function includesAny(text = '', terms = []) {
  const source = String(text || '').toLowerCase();
  return terms.some((term) => source.includes(String(term || '').toLowerCase()));
}

function isSummaryPrompt(message = '') {
  return includesAny(message, [
    'summary',
    'summarize',
    'summarise',
    'final report',
    'report',
    'wrap up',
    'watchpoint',
    'watchpoints',
    'checklist',
    '요약',
    '최종',
    '리포트',
    '관찰 지표',
    '체크리스트',
  ]);
}

function looksLikeOilContext(message = '', datasetContext = {}, history = []) {
  const columns = Array.isArray(datasetContext?.columns) ? datasetContext.columns.join(' ') : '';
  const corpus = [
    String(message || ''),
    String(datasetContext?.datasetName || ''),
    columns,
    historyText(history),
  ].join('\n');
  return includesAny(corpus, [
    'oil',
    'crude',
    'brent',
    'wti',
    'opec',
    'hormuz',
    'hormuz',
    'iran',
    'tanker',
    'refining',
    'korea',
    '원유',
    '이란',
    '호르무즈',
    '정유',
    '한국 경제',
  ]);
}

function pickColumns(datasetContext = {}, pattern) {
  const columns = Array.isArray(datasetContext?.columns) ? datasetContext.columns : [];
  return columns.filter((column) => pattern.test(String(column || ''))).slice(0, 4);
}

function buildOilSummaryData(datasetContext = {}, facts = {}, suggestions = []) {
  const priceColumns = pickColumns(datasetContext, /brent|wti|price|oil/i);
  const riskColumns = pickColumns(datasetContext, /risk|headline|news|supply|opec|iran|hormuz/i);
  const checklist = [
    priceColumns.length ? `${priceColumns.join(', ')} trend and daily change` : 'Brent and WTI price moves',
    riskColumns.length ? `${riskColumns.join(', ')} signal scan` : 'Supply disruption and headline risk',
    'Hormuz transit incidents, tanker insurance, and freight costs',
    'OPEC+ response, Asian import demand, and refinery margin pressure',
    'KRW/USD, Korea CPI, and energy-sensitive sector watchpoints',
  ];
  const trimmedSuggestions = suggestions
    .filter((item) => includesAny(item?.label, ['profile', 'descriptive', 'correlation', 'time-series', 'flags']))
    .slice(0, 5);

  return {
    reply: [
      `Final report: ${facts.datasetName}.`,
      `Price lens: ${priceColumns.length ? `track ${priceColumns.join(', ')}` : 'track the available oil price columns'} across the ${facts.rowCount}-row dataset and review date-based trend breaks first.`,
      `Supply-risk lens: ${riskColumns.length ? `use ${riskColumns.join(', ')}` : 'use the news and risk fields'} to monitor disruption signals around Iran, Hormuz, and OPEC+ response.`,
      'Korea watchpoints: imported energy costs, KRW/USD sensitivity, refining and petrochemical margins, shipping insurance, and freight-cost spillover.',
      'Scenario frame: mild disruption -> short-lived spike; prolonged disruption -> inflation and transport-cost pressure; escalation -> supply shock and broader risk-off move.',
      `Checklist: ${checklist.join(' | ')}.`,
      `Current dataset fit: ${facts.columnCount} columns with ${facts.numericColumns} numeric, ${facts.categoricalColumns} categorical, and ${facts.dateColumns} date-like fields${facts.topCorrCount > 0 ? ` / ${facts.topCorrCount} correlation candidate(s) already flagged` : ''}.`,
    ].join('\n'),
    cards: [
      {
        type: 'report',
        title: 'Final report',
        body: 'Price, supply risk, scenario framing, and watchpoints are summarized for a stakeholder-ready oil brief.',
      },
      {
        type: 'risk',
        title: 'Supply risk',
        body: riskColumns.length
          ? `Focus columns: ${riskColumns.join(', ')}.`
          : 'Focus on disruption headlines, transit signals, and producer-response evidence.',
      },
      {
        type: 'korea',
        title: 'Korea watchpoints',
        body: 'Track imported energy costs, KRW/USD, refinery margins, petrochemical exposure, and shipping/freight pass-through.',
      },
    ],
    suggestions: trimmedSuggestions.length ? trimmedSuggestions : suggestions.slice(0, 5),
  };
}

function buildGenericSummaryData(facts = {}, suggestions = []) {
  const trimmedSuggestions = suggestions
    .filter((item) => includesAny(item?.label, ['profile', 'descriptive', 'correlation', 'flags', 'chart']))
    .slice(0, 5);
  return {
    reply: [
      `Final report: ${facts.datasetName}.`,
      `Dataset scope: ${facts.rowCount} rows / ${facts.columnCount} columns / ${facts.numericColumns} numeric / ${facts.categoricalColumns} categorical / ${facts.dateColumns} date-like.`,
      facts.warningCount > 0
        ? `Risk notes: ${facts.warnings.slice(0, 2).join(' ')}`
        : 'Risk notes: no major profile warnings were provided in the current context.',
      facts.topCorrCount > 0
        ? `Evidence summary: ${facts.topCorrCount} correlation candidate(s) are already flagged for follow-up.`
        : 'Evidence summary: start with descriptive statistics, then confirm the strongest relationships visually.',
      'Next checks: profile quality, descriptive stats, and one chart or one formal test before expanding the scope.',
    ].join('\n'),
    cards: [
      {
        type: 'report',
        title: 'Final report',
        body: 'A concise summary is ready for the current dataset context and follow-up analysis planning.',
      },
    ],
    suggestions: trimmedSuggestions.length ? trimmedSuggestions : suggestions.slice(0, 5),
  };
}

function buildSummaryData(message = '', datasetContext = {}, history = [], facts = {}, suggestions = []) {
  if (!isSummaryPrompt(message)) return null;
  if (looksLikeOilContext(message, datasetContext, history)) {
    return buildOilSummaryData(datasetContext, facts, suggestions);
  }
  return buildGenericSummaryData(facts, suggestions);
}

function buildSuggestions(facts) {
  const suggestions = [];
  if (facts.workspaceCount > 1) {
    suggestions.push({
      label: 'Compare open datasets',
      tool: 'workspace.list_datasets',
      reason: 'Multiple datasets are open in the current workspace.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Compare descriptive stats across workspace',
      tool: 'workspace.compare_describe',
      reason: 'Run the same descriptive workflow on capped samples from the open datasets.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Plan comparison charts across workspace',
      tool: 'workspace.compare_chart_plan',
      reason: 'Build chart specs for comparing the open datasets.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Compare stat differences across workspace',
      tool: 'workspace.compare_stat_diff',
      reason: 'Highlight the strongest numeric and categorical differences across the open datasets.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Recommend workspace priorities',
      tool: 'workspace.recommend_analysis',
      reason: 'Prioritize which dataset and analysis to tackle first.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Plan formal compares across workspace',
      tool: 'workspace.formal_compare_plan',
      reason: 'Align comparable tests and cautions before deeper inference.',
      inputTemplate: {},
    });
  }
  suggestions.push({
    label: 'Inspect dataset profile',
    tool: 'dataset.profile',
    reason: 'Start by reading the compact schema and quality summary.',
    inputTemplate: {},
  });
  suggestions.push({
    label: 'Inspect dataset flags',
    tool: 'dataset.flags',
    reason: 'Review immediate warnings before deeper analysis.',
    inputTemplate: {},
  });
  suggestions.push({
    label: 'Run descriptive statistics',
    tool: 'stat.run',
    reason: 'Start with summary statistics and quality checks.',
    inputTemplate: { op: 'describe' },
  });
  if (facts.numericColumns >= 2) {
    suggestions.push({
      label: 'Run correlation analysis',
      tool: 'stat.run',
      reason: 'Multiple numeric columns are available.',
      inputTemplate: { op: 'corr' },
    });
  }
  if (facts.numericColumns >= 1 && facts.categoricalColumns >= 1) {
    suggestions.push({
      label: 'Ask for recommended tests',
      tool: 'stat.recommend',
      reason: 'Mixed numeric and categorical structure suggests group-comparison candidates.',
      inputTemplate: {},
    });
  }
  if (facts.numericColumns >= 2) {
    suggestions.push({
      label: 'Review visualization options',
      tool: 'viz.prepare',
      reason: 'Numeric relationships are present and charting is likely useful.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Aggregate chart-ready data',
      tool: 'viz.aggregate',
      reason: 'Build grouped or binned chart inputs before rendering.',
      inputTemplate: {},
    });
  }
  if (facts.rowCount >= 30) {
    suggestions.push({
      label: 'Check ML capabilities',
      tool: 'ml.capabilities',
      reason: 'Dataset size is large enough to consider ML workflows.',
      inputTemplate: {},
    });
    suggestions.push({
      label: 'Run anomaly detection starter',
      tool: 'ml.run',
      reason: 'Start with an unsupervised anomaly scan that does not require a target column.',
      inputTemplate: {
        task: 'anomaly',
        model: 'isolation_forest',
        options: {
          preset: 'balanced',
          contamination: 'auto',
        },
      },
    });
    if (facts.numericColumns >= 2) {
      suggestions.push({
        label: 'Run regression starter',
        tool: 'ml.run',
        reason: 'Use a numeric target candidate with the remaining columns as starter features.',
        inputTemplate: {
          task: 'regression',
          model: 'linear',
          options: {
            preset: 'balanced',
          },
        },
      });
    }
    if (facts.numericColumns >= 1 && facts.categoricalColumns >= 1) {
      suggestions.push({
        label: 'Run classification starter',
        tool: 'ml.run',
        reason: 'Use a categorical target candidate with mixed features as a first-pass classifier.',
        inputTemplate: {
          task: 'classification',
          model: 'forest',
          options: {
            preset: 'balanced',
          },
        },
      });
    }
    if (facts.dateColumns >= 1 && facts.numericColumns >= 1) {
      suggestions.push({
        label: 'Run time-series starter',
        tool: 'ml.run',
        reason: 'Use a date-like column and numeric measure for a first-pass forecast.',
        inputTemplate: {
          task: 'timeseries',
          model: 'moving_avg',
          options: {
            preset: 'balanced',
            horizon: 12,
          },
        },
      });
    }
  }
  return suggestions.slice(0, facts.workspaceCount > 1 ? 8 : 12);
}

function shouldUseStatsRecommender(message, facts) {
  const prompt = String(message || '').toLowerCase();
  if (!facts?.rowCount || !facts?.columnCount) return false;
  return (
    prompt.includes('stat')
    || prompt.includes('recommend')
    || prompt.includes('analy')
    || prompt.includes('what should')
    || prompt.includes('first')
    || prompt.includes('test')
  );
}

function mapStatRecommendSuggestions(result) {
  const recommendations = Array.isArray(result?.recommendations) ? result.recommendations : [];
  const mapped = [];
  for (const recommendation of recommendations.slice(0, 3)) {
    mapped.push({
      label: recommendation.label || `Run ${recommendation.op || 'recommended stats'}`,
      tool: 'stat.run',
      reason: recommendation.reason || 'Recommended by the statistics engine.',
      inputTemplate: {
        op: recommendation.op,
        args: recommendation.args || {},
      },
    });
    if (recommendation?.chart?.type) {
      mapped.push({
        label: `Prepare ${recommendation.chart.type} chart`,
        tool: 'viz.prepare',
        reason: `Chart hint for ${recommendation.label || recommendation.op || 'recommended stats'}.`,
        inputTemplate: {
          spec: {
            type: recommendation.chart.type,
            x: recommendation.chart.x,
            y: recommendation.chart.y,
            options: {
              title: recommendation.label || 'Recommended chart',
            },
          },
        },
      });
    }
  }
  return mapped;
}

function buildReply(message, facts, suggestions) {
  const prompt = String(message || '').toLowerCase();
  if (!facts.columnCount) {
    return 'No active dataset context was provided. Load a dataset first, then ask what to analyze.';
  }

  const lines = [
    `Active dataset: ${facts.datasetName}.`,
    `Rows: ${facts.rowCount}, columns: ${facts.columnCount}.`,
    `Column mix: ${facts.numericColumns} numeric, ${facts.categoricalColumns} categorical, ${facts.dateColumns} date-like.`,
  ];

  if (facts.missingColumns.length) {
    lines.push(`Missing-value hotspots: ${facts.missingColumns.map((item) => `${item.name} (${item.rate}%)`).join(', ')}.`);
  }
  if (facts.duplicateRows > 0) {
    lines.push(`Duplicate rows detected in the profile sample: ${facts.duplicateRows}.`);
  }
  if (facts.outlierColumns.length) {
    lines.push(`Outlier-heavy columns: ${facts.outlierColumns.map((item) => `${item.name} (${item.rate}%)`).join(', ')}.`);
  }
  if (facts.imbalancedColumns.length) {
    lines.push(`Categorical imbalance: ${facts.imbalancedColumns.map((item) => `${item.name}=${item.topValue} (${item.rate}%)`).join(', ')}.`);
  }
  if (facts.topCorrCount > 0) {
    lines.push(`Correlation candidates already detected: ${facts.topCorrCount}.`);
  }
  if (facts.warningCount > 0) {
    lines.push(`Profile warnings: ${facts.warnings.slice(0, 2).join(' ')}`);
  }
  if ((prompt.includes('compare') || prompt.includes('difference') || prompt.includes('workspace')) && facts.workspaceCount > 1) {
    lines.push(`Workspace datasets: ${facts.workspaceDatasets.map((dataset) => dataset.name).join(', ')}.`);
    if (facts.sharedColumns.length) {
      lines.push(`Shared columns across open datasets: ${facts.sharedColumns.join(', ')}.`);
    } else {
      lines.push('No shared column names were found across all open datasets.');
    }
    if (prompt.includes('stat') || prompt.includes('diff') || prompt.includes('difference')) {
      lines.push('Recommended next step: compute a statistical difference summary, then decide whether a deeper formal test or a comparison chart is needed.');
    } else if (prompt.includes('formal') || prompt.includes('test') || prompt.includes('infer')) {
      lines.push('Recommended next step: build a formal comparison plan so the same test family and cautions are aligned across datasets.');
    } else if (prompt.includes('priority') || prompt.includes('prioritize') || prompt.includes('recommend')) {
      lines.push('Recommended next step: ask for workspace analysis priorities so the highest-risk or highest-value dataset is handled first.');
    } else if (prompt.includes('chart') || prompt.includes('visual') || prompt.includes('graph')) {
      lines.push('Recommended next step: build a comparison chart plan using shared columns, then render the selected charts per dataset.');
    } else {
      lines.push('Recommended next step: inspect the workspace comparison summary, then run the same descriptive workflow or chart plan across the datasets you want to compare.');
    }
  }

  if (prompt.includes('chart') || prompt.includes('graph') || prompt.includes('visual')) {
    lines.push('Recommended next step: review chart-oriented suggestions first.');
  } else if (prompt.includes('ml') || prompt.includes('model') || prompt.includes('predict')) {
    lines.push('Recommended next step: validate stats and data quality first, then inspect ML-capability suggestions.');
  } else if (prompt.includes('anomaly') || prompt.includes('issue') || prompt.includes('problem') || prompt.includes('weird')) {
    lines.push('Recommended next step: inspect missing values, duplicates, and correlation-heavy columns before formal modeling.');
  } else {
    lines.push('Recommended next step: start with descriptive stats, then move to correlation or recommended tests.');
  }

  if (prompt.includes('claude')) {
    lines.push('Claude is not required for this phase. Rule-based MCP chat is active now; Claude becomes relevant when the server-side planner is added.');
  }

  if (suggestions.length) {
    lines.push(`Available suggestions: ${suggestions.map((item) => item.label).join(', ')}.`);
  }
  return lines.join('\n');
}

function buildRuleBasedChatData(message, datasetContext, history = []) {
  const facts = buildFacts(datasetContext);
  const suggestions = buildSuggestions(facts);
  const summaryData = buildSummaryData(message, datasetContext, history, facts, suggestions);
  const reply = summaryData?.reply || buildReply(message, facts, suggestions);
  const warnings = facts.warnings || [];
  const cards = [
    {
      type: 'dataset',
      title: facts.datasetName,
      body: `${facts.rowCount} rows / ${facts.columnCount} columns / ${facts.numericColumns} numeric / ${facts.categoricalColumns} categorical`,
    },
  ];
  if (facts.workspaceCount > 1) {
    cards.push({
      type: 'workspace',
      title: 'Workspace comparison',
      body: `${facts.workspaceCount} datasets open${facts.sharedColumns.length ? ` / shared columns: ${facts.sharedColumns.join(', ')}` : ''}`,
    });
  }
  if (Array.isArray(summaryData?.cards)) {
    cards.push(...summaryData.cards);
  }
  return {
    mode: 'rule-based',
    reply,
    cards,
    suggestions: Array.isArray(summaryData?.suggestions) ? summaryData.suggestions : suggestions,
    warnings,
  };
}

async function enrichRuleBasedChatDataWithStatsRecommendation(options = {}) {
  const {
    data,
    message,
    datasetContext,
    keyHeader,
    runToolInternal,
  } = options;
  const next = data && typeof data === 'object'
    ? cloneChatData(data)
    : buildRuleBasedChatData(message, datasetContext);
  const facts = buildFacts(datasetContext);
  if (!shouldUseStatsRecommender(message, facts) || typeof runToolInternal !== 'function') return next;

  try {
    const statRecommendResult = await runToolInternal('stat.recommend', {}, keyHeader, datasetContext);
    if (!statRecommendResult?.ok) return next;
    const summary = statRecommendResult.summary || {};
    next.cards.push({
      type: 'stats-recommendation',
      title: summary.title || 'Statistical Test Recommender',
      body: summary.conclusion || 'Statistics recommendations are available.',
    });
    const topLabels = (Array.isArray(statRecommendResult.recommendations) ? statRecommendResult.recommendations : [])
      .slice(0, 3)
      .map((item) => item?.label)
      .filter(Boolean);
    if (topLabels.length) {
      next.cards.push({
        type: 'stats-recommendation-top',
        title: 'Top recommended tests',
        body: topLabels.join(' | '),
      });
    }
    const mappedSuggestions = mapStatRecommendSuggestions(statRecommendResult);
    next.suggestions = [...mappedSuggestions, ...next.suggestions]
      .filter((item, index, list) => {
        const key = `${item?.tool || ''}:${item?.label || ''}`;
        return list.findIndex((candidate) => `${candidate?.tool || ''}:${candidate?.label || ''}` === key) === index;
      })
      .slice(0, facts.workspaceCount > 1 ? 10 : 14);
    next.warnings = [...next.warnings, ...(Array.isArray(statRecommendResult.warnings) ? statRecommendResult.warnings : [])];
    next.toolCalls.push({ tool: 'stat.recommend' });
  } catch {
    return next;
  }
  return next;
}

function buildClaudeChatSuccessData(baseData, claudeData) {
  const next = cloneChatData(baseData);
  return {
    ...next,
    mode: claudeData?.mode || 'claude',
    reply: claudeData?.reply || next.reply,
    toolCalls: Array.isArray(claudeData?.toolCalls) ? [...claudeData.toolCalls] : [],
  };
}

function buildClaudeFallbackChatData(baseData, claudeError) {
  const next = cloneChatData(baseData);
  return {
    ...next,
    warnings: [...next.warnings, `Claude fallback activated: ${String(claudeError?.message || claudeError || 'unknown error')}`],
  };
}

module.exports = {
  cloneChatData,
  buildSuggestions,
  shouldUseStatsRecommender,
  mapStatRecommendSuggestions,
  buildReply,
  buildRuleBasedChatData,
  enrichRuleBasedChatDataWithStatsRecommendation,
  buildClaudeChatSuccessData,
  buildClaudeFallbackChatData,
};
