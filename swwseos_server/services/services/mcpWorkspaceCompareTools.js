const {
  sampleRows,
  classifyColumns,
  inferColumnKinds,
  topMissingColumns,
  normalizeWorkspaceDatasets,
  sharedWorkspaceColumns,
} = require('./datasetProfiler');

const MCP_MAX_CONTEXT_ROWS = 120;

function sharedColumnKinds(workspaceDatasets, sharedColumns) {
  const numeric = [];
  const categorical = [];
  const dateLike = [];
  for (const column of sharedColumns || []) {
    let numericHits = 0;
    let dateHits = 0;
    let totalDatasets = 0;
    for (const dataset of workspaceDatasets || []) {
      const rows = sampleRows(dataset.sampleRows || [], 80);
      const values = rows.map((row) => row?.[column]);
      let nonNull = 0;
      let num = 0;
      let date = 0;
      for (const value of values) {
        if (value === null || value === undefined || value === '') continue;
        nonNull += 1;
        if (Number.isFinite(Number(value))) num += 1;
        else if (!Number.isNaN(new Date(String(value)).getTime())) date += 1;
      }
      totalDatasets += 1;
      if (nonNull > 0 && num / nonNull >= 0.7) numericHits += 1;
      else if (nonNull > 0 && date / nonNull >= 0.7) dateHits += 1;
    }
    if (totalDatasets > 0 && numericHits === totalDatasets) numeric.push(column);
    else if (totalDatasets > 0 && dateHits === totalDatasets) dateLike.push(column);
    else categorical.push(column);
  }
  return { numeric, categorical, dateLike };
}

function distinctValueCount(rows, column, limit = 20) {
  const seen = new Set();
  for (const row of rows || []) {
    const value = row?.[column];
    if (value === null || value === undefined || value === '') continue;
    seen.add(String(value));
    if (seen.size >= limit) return limit;
  }
  return seen.size;
}

function buildConcreteStatInput(columnKinds, rows, prefer = 'auto') {
  const numeric = Array.isArray(columnKinds?.numeric) ? columnKinds.numeric : [];
  const categorical = Array.isArray(columnKinds?.categorical) ? columnKinds.categorical : [];
  const dateLike = Array.isArray(columnKinds?.dateLike) ? columnKinds.dateLike : [];

  if ((prefer === 'group' || prefer === 'auto') && numeric[0] && categorical[0]) {
    const levels = distinctValueCount(rows, categorical[0]);
    if (levels === 2) {
      return { op: 'ttest', args: { value: numeric[0], group: categorical[0] } };
    }
    if (levels > 2) {
      return { op: 'anova', args: { value: numeric[0], group: categorical[0] } };
    }
  }
  if ((prefer === 'association' || prefer === 'auto') && categorical.length >= 2) {
    return { op: 'chisq', args: { a: categorical[0], b: categorical[1] } };
  }
  if ((prefer === 'correlation' || prefer === 'auto') && numeric.length >= 2) {
    return { op: 'corr', args: {} };
  }
  if ((prefer === 'trend' || prefer === 'auto') && dateLike[0] && numeric[0]) {
    return { op: 'describe', args: {} };
  }
  if ((prefer === 'describe' || prefer === 'auto') && (numeric.length || categorical.length || dateLike.length)) {
    return { op: 'describe', args: {} };
  }
  return null;
}

function buildStatRunAction(input, rows, label, reason) {
  if (!input?.op) return null;
  const inputTemplate = {
    op: input.op,
    rows: sampleRows(rows || [], MCP_MAX_CONTEXT_ROWS),
  };
  if (input.args && Object.keys(input.args).length) {
    inputTemplate.args = input.args;
  }
  return {
    label,
    tool: 'stat.run',
    reason,
    inputTemplate,
  };
}

function duplicateCount(rows) {
  const seen = new Set();
  let duplicates = 0;
  for (const row of rows || []) {
    const key = JSON.stringify(row || {});
    if (seen.has(key)) duplicates += 1;
    else seen.add(key);
  }
  return duplicates;
}

async function buildWorkspaceCompareDescribeResult(datasetContext, _keyHeader) {
  const workspaceDatasets = normalizeWorkspaceDatasets(datasetContext).slice(0, 4);
  if (workspaceDatasets.length < 2) {
    return {
      ok: true,
      data: {
        comparedDatasets: 0,
        datasets: [],
        sharedColumns: [],
        sharedNumericColumns: [],
        focusNumericColumn: '',
      },
    };
  }

  function computeMissing(columns, rows) {
    const items = [];
    for (const column of columns || []) {
      let missing = 0;
      for (const row of rows) {
        const value = row?.[column];
        if (value === null || value === undefined || value === '') missing += 1;
      }
      if (rows.length && missing > 0) {
        items.push({ column, nullRate: missing / rows.length });
      }
    }
    items.sort((a, b) => b.nullRate - a.nullRate);
    return items;
  }

  function computeNumericDescribe(columns, rows) {
    const output = {};
    for (const column of columns || []) {
      const values = rows
        .map((row) => row?.[column])
        .filter((value) => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)))
        .map((value) => Number(value))
        .sort((a, b) => a - b);
      if (!values.length) continue;
      const sum = values.reduce((acc, value) => acc + value, 0);
      const mean = sum / values.length;
      const middle = Math.floor(values.length / 2);
      const median = values.length % 2 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
      const variance = values.length > 1
        ? values.reduce((acc, value) => acc + ((value - mean) ** 2), 0) / (values.length - 1)
        : 0;
      output[column] = {
        mean: +mean.toFixed(4),
        median: +median.toFixed(4),
        std: +(Math.sqrt(variance)).toFixed(4),
      };
    }
    return output;
  }

  const compared = workspaceDatasets.map((dataset) => {
    const rows = Array.isArray(dataset.sampleRows) ? dataset.sampleRows : [];
    const missing = computeMissing(dataset.columns, rows);
    return {
      datasetId: dataset.datasetId,
      name: dataset.name,
      rowCount: dataset.rowCount || rows.length,
      columnCount: dataset.columnCount || dataset.columns.length,
      sampleRows: rows.length,
      dupRows: duplicateCount(rows),
      topMissing: missing[0]?.column ? `${missing[0].column} (${(missing[0].nullRate * 100).toFixed(1)}% null)` : '',
      numericDescribe: computeNumericDescribe(dataset.columns, rows),
    };
  });

  const sharedColumns = sharedWorkspaceColumns(workspaceDatasets);
  const sharedNumericColumns = compared
    .map((dataset) => Object.keys(dataset.numericDescribe || {}))
    .reduce((shared, columns, index) => {
      if (!index) return [...columns];
      return shared.filter((column) => columns.includes(column));
    }, [])
    .slice(0, 5);
  const focusNumericColumn = sharedNumericColumns[0] || '';

  return {
    ok: true,
    data: {
      comparedDatasets: compared.length,
      sharedColumns,
      sharedNumericColumns,
      focusNumericColumn,
      datasets: compared.map((dataset) => ({
        datasetId: dataset.datasetId,
        name: dataset.name,
        rowCount: dataset.rowCount,
        columnCount: dataset.columnCount,
        sampleRows: dataset.sampleRows,
        dupRows: dataset.dupRows,
        topMissing: dataset.topMissing,
        focusStats: focusNumericColumn && dataset.numericDescribe[focusNumericColumn]
          ? {
            mean: dataset.numericDescribe[focusNumericColumn].mean,
            median: dataset.numericDescribe[focusNumericColumn]['50%'] ?? dataset.numericDescribe[focusNumericColumn].median,
            std: dataset.numericDescribe[focusNumericColumn].std,
          }
          : null,
      })),
    },
  };
}

function buildWorkspaceCompareChartPlanResult(datasetContext) {
  const workspaceDatasets = normalizeWorkspaceDatasets(datasetContext).slice(0, 4);
  if (workspaceDatasets.length < 2) {
    return {
      ok: true,
      data: {
        chartType: '',
        reason: 'At least two workspace datasets are required.',
        sharedColumns: [],
        focus: {},
        datasets: [],
      },
    };
  }

  const sharedColumns = sharedWorkspaceColumns(workspaceDatasets);
  const kinds = sharedColumnKinds(workspaceDatasets, sharedColumns);
  let chartType = 'bar';
  let focus = {};
  let reason = 'Shared categorical structure is available, so count comparison is the safest default.';

  if (kinds.dateLike[0] && kinds.numeric[0]) {
    chartType = 'line';
    focus = { x: kinds.dateLike[0], y: kinds.numeric[0] };
    reason = 'Shared date-like and numeric columns exist, so line comparison is recommended.';
  } else if (kinds.categorical[0] && kinds.numeric[0]) {
    chartType = 'bar';
    focus = { x: kinds.categorical[0], y: kinds.numeric[0], agg: 'mean' };
    reason = 'Shared categorical and numeric columns exist, so grouped bar comparison is recommended.';
  } else if (kinds.numeric[0] && kinds.numeric[1]) {
    chartType = 'scatter';
    focus = { x: kinds.numeric[0], y: kinds.numeric[1] };
    reason = 'Two shared numeric columns exist, so scatter comparison is recommended.';
  } else if (kinds.numeric[0]) {
    chartType = 'histogram';
    focus = { x: kinds.numeric[0] };
    reason = 'A shared numeric column exists, so distribution comparison is recommended.';
  } else if (kinds.categorical[0]) {
    chartType = 'bar';
    focus = { x: kinds.categorical[0], agg: 'count' };
  }

  const datasets = workspaceDatasets.map((dataset) => ({
    datasetId: dataset.datasetId,
    name: dataset.name,
    spec: {
      type: chartType,
      x: focus.x,
      y: focus.y,
      options: {
        ...(focus.agg ? { agg: focus.agg } : {}),
        title: `${dataset.name}: ${chartType} comparison`,
        xLabel: focus.x || '',
        yLabel: focus.y || (focus.agg === 'count' ? 'Count' : ''),
      },
    },
  }));

  return {
    ok: true,
    data: {
      chartType,
      reason,
      sharedColumns,
      focus,
      datasets,
    },
  };
}

function buildWorkspaceCompareStatDiffResult(datasetContext) {
  const workspaceDatasets = normalizeWorkspaceDatasets(datasetContext).slice(0, 4);
  if (workspaceDatasets.length < 2) {
    return {
      ok: true,
      data: {
        comparedDatasets: 0,
        sharedColumns: [],
        focusNumericColumn: '',
        focusCategoricalColumn: '',
        numericDiffs: [],
        categoricalDiffs: [],
      },
    };
  }

  function numericSummary(rows, column) {
    const values = (rows || [])
      .map((row) => row?.[column])
      .filter((value) => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)))
      .map((value) => Number(value));
    if (!values.length) return null;
    const mean = values.reduce((acc, value) => acc + value, 0) / values.length;
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    const variance = sorted.length > 1
      ? sorted.reduce((acc, value) => acc + ((value - mean) ** 2), 0) / (sorted.length - 1)
      : 0;
    return {
      n: sorted.length,
      mean: +mean.toFixed(4),
      median: +median.toFixed(4),
      std: +(Math.sqrt(variance)).toFixed(4),
    };
  }

  function categoricalSummary(rows, column) {
    const counts = new Map();
    let total = 0;
    for (const row of rows || []) {
      const value = row?.[column];
      if (value === null || value === undefined || value === '') continue;
      const key = String(value);
      counts.set(key, (counts.get(key) || 0) + 1);
      total += 1;
    }
    if (!total) return null;
    const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
    const [topLabel, topCount] = ranked[0];
    return {
      topLabel,
      topCount,
      topRatio: +(topCount / total).toFixed(4),
    };
  }

  const sharedColumns = sharedWorkspaceColumns(workspaceDatasets);
  const kinds = sharedColumnKinds(workspaceDatasets, sharedColumns);
  const activeDataset = workspaceDatasets.find((dataset) => dataset.active) || workspaceDatasets[0];
  const activeRows = Array.isArray(activeDataset?.sampleRows) ? activeDataset.sampleRows : [];
  const activeSharedKinds = classifyColumns(sharedColumns, activeRows);

  function buildNumericDiffAction(column) {
    if (!column || !activeRows.length) return null;
    const valueColumn = activeSharedKinds.numeric.includes(column) ? column : activeSharedKinds.numeric[0];
    const groupColumn = activeSharedKinds.categorical[0];
    if (valueColumn && groupColumn) {
      const levels = distinctValueCount(activeRows, groupColumn);
      if (levels === 2) {
        return buildStatRunAction(
          { op: 'ttest', args: { value: valueColumn, group: groupColumn } },
          activeRows,
          `Run stat follow-up for ${column}`,
          `Check whether ${column} differs by ${groupColumn} on the active dataset.`
        );
      }
      if (levels > 2) {
        return buildStatRunAction(
          { op: 'anova', args: { value: valueColumn, group: groupColumn } },
          activeRows,
          `Run stat follow-up for ${column}`,
          `Check whether ${column} differs by ${groupColumn} on the active dataset.`
        );
      }
    }
    if (valueColumn) {
      return buildStatRunAction(
        { op: 'ci_mean', args: { column: valueColumn, confidence: 0.95 } },
        activeRows,
        `Run confidence interval for ${column}`,
        `Review a confidence interval for ${column} on the active dataset.`
      );
    }
    return buildStatRunAction(
      { op: 'describe', args: {} },
      activeRows,
      `Describe ${column}`,
      `Use a descriptive summary for ${column} on the active dataset.`
    );
  }

  function buildCategoricalDiffAction(column) {
    if (!column || !activeRows.length) return null;
    const categoricalColumns = activeSharedKinds.categorical.filter((item) => item !== column);
    if (activeSharedKinds.numeric[0]) {
      const levels = distinctValueCount(activeRows, column);
      if (levels === 2) {
        return buildStatRunAction(
          { op: 'ttest', args: { value: activeSharedKinds.numeric[0], group: column } },
          activeRows,
          `Run stat follow-up for ${column}`,
          `Check whether ${activeSharedKinds.numeric[0]} differs by ${column} on the active dataset.`
        );
      }
      if (levels > 2) {
        return buildStatRunAction(
          { op: 'anova', args: { value: activeSharedKinds.numeric[0], group: column } },
          activeRows,
          `Run stat follow-up for ${column}`,
          `Check whether ${activeSharedKinds.numeric[0]} differs by ${column} on the active dataset.`
        );
      }
    }
    if (categoricalColumns[0]) {
      return buildStatRunAction(
        { op: 'chisq', args: { a: column, b: categoricalColumns[0] } },
        activeRows,
        `Run categorical follow-up for ${column}`,
        `Check whether ${column} is associated with ${categoricalColumns[0]} on the active dataset.`
      );
    }
    return buildStatRunAction(
      { op: 'describe', args: {} },
      activeRows,
      `Describe ${column}`,
      `Use a descriptive summary for ${column} on the active dataset.`
    );
  }

  const numericDiffs = (kinds.numeric || []).map((column) => {
    const datasets = workspaceDatasets
      .map((dataset) => ({
        datasetId: dataset.datasetId,
        name: dataset.name,
        stats: numericSummary(dataset.sampleRows || [], column),
      }))
      .filter((dataset) => dataset.stats);
    if (datasets.length < 2) return null;
    const means = datasets.map((dataset) => dataset.stats.mean);
    const delta = Math.max(...means) - Math.min(...means);
    return {
      column,
      meanDelta: +delta.toFixed(4),
      datasets,
      datasetId: activeDataset?.datasetId || '',
      datasetName: activeDataset?.name || '',
      action: buildNumericDiffAction(column),
    };
  }).filter(Boolean).sort((a, b) => b.meanDelta - a.meanDelta).slice(0, 3);

  const categoricalDiffs = (kinds.categorical || []).map((column) => {
    const datasets = workspaceDatasets
      .map((dataset) => ({
        datasetId: dataset.datasetId,
        name: dataset.name,
        stats: categoricalSummary(dataset.sampleRows || [], column),
      }))
      .filter((dataset) => dataset.stats);
    if (datasets.length < 2) return null;
    const topRatios = datasets.map((dataset) => dataset.stats.topRatio);
    const dominanceGap = Math.max(...topRatios) - Math.min(...topRatios);
    return {
      column,
      dominanceGap: +dominanceGap.toFixed(4),
      datasets,
      datasetId: activeDataset?.datasetId || '',
      datasetName: activeDataset?.name || '',
      action: buildCategoricalDiffAction(column),
    };
  }).filter(Boolean).sort((a, b) => b.dominanceGap - a.dominanceGap).slice(0, 3);

  return {
    ok: true,
    data: {
      comparedDatasets: workspaceDatasets.length,
      sharedColumns,
      focusNumericColumn: numericDiffs[0]?.column || '',
      focusCategoricalColumn: categoricalDiffs[0]?.column || '',
      numericDiffs,
      categoricalDiffs,
    },
  };
}

function buildWorkspaceRecommendAnalysisResult(datasetContext) {
  const workspaceDatasets = normalizeWorkspaceDatasets(datasetContext).slice(0, 4);
  if (!workspaceDatasets.length) {
    return {
      ok: true,
      data: {
        sharedColumns: [],
        recommendedDatasets: [],
        workspaceAction: null,
      },
    };
  }

  const sharedColumns = sharedWorkspaceColumns(workspaceDatasets);

  const recommendedDatasets = workspaceDatasets.map((dataset) => {
    const rows = Array.isArray(dataset.sampleRows) ? dataset.sampleRows : [];
    const missing = topMissingColumns(dataset.columns, rows);
    const duplicates = duplicateCount(rows);
    const kindCounts = inferColumnKinds(dataset.columns, rows);
    const kindLists = classifyColumns(dataset.columns, rows);
    const reasons = [];
    let score = dataset.active ? 0.5 : 0;
    let action = {
      label: `Inspect ${dataset.name} profile`,
      tool: 'dataset.profile',
      reason: 'Start from a compact profile summary.',
      inputTemplate: {
        datasetName: dataset.name,
        columns: dataset.columns,
        sampleRows: rows,
        profileSummary: { duplicates, warnings: [], topCorrCount: 0, topAnovaCount: 0 },
      },
    };

    if (missing[0]?.rate >= 20) {
      score += 4;
      reasons.push(`high missingness in ${missing[0].name} (${missing[0].rate}%)`);
      action = {
        label: `Inspect ${dataset.name} data quality`,
        tool: 'dataset.flags',
        reason: 'Missingness is the strongest risk in this dataset.',
        inputTemplate: {
          datasetName: dataset.name,
          columns: dataset.columns,
          sampleRows: rows,
          profileSummary: { duplicates, warnings: [], topCorrCount: 0, topAnovaCount: 0 },
        },
      };
    }
    if (duplicates > 0) {
      score += 2;
      reasons.push(`${duplicates} duplicate row(s) in sample`);
      if (action.tool !== 'dataset.flags') {
        action = {
          label: `Inspect ${dataset.name} duplicates`,
          tool: 'dataset.flags',
          reason: 'Duplicate rows should be reviewed before modeling.',
          inputTemplate: {
            datasetName: dataset.name,
            columns: dataset.columns,
            sampleRows: rows,
            profileSummary: { duplicates, warnings: [], topCorrCount: 0, topAnovaCount: 0 },
          },
        };
      }
    }
    if (kindCounts.numeric >= 1 && kindCounts.categorical >= 1) {
      score += 2;
      reasons.push('mixed numeric/categorical structure');
      if (action.tool === 'dataset.profile') {
        action = buildStatRunAction(
          buildConcreteStatInput(kindLists, rows, 'group') || buildConcreteStatInput(kindLists, rows, 'auto'),
          rows,
          `Run recommended test for ${dataset.name}`,
          'Mixed structure suggests group-comparison analysis.'
        ) || action;
      }
    } else if (kindCounts.numeric >= 2) {
      score += 2;
      reasons.push('multiple numeric columns');
      if (action.tool === 'dataset.profile') {
        action = buildStatRunAction(
          buildConcreteStatInput(kindLists, rows, 'correlation'),
          rows,
          `Run correlation on ${dataset.name}`,
          'Multiple numeric columns are available.'
        ) || action;
      }
    } else if (kindCounts.categorical >= 2) {
      score += 1.5;
      reasons.push('multiple categorical columns');
      if (action.tool === 'dataset.profile') {
        action = buildStatRunAction(
          buildConcreteStatInput(kindLists, rows, 'association'),
          rows,
          `Run categorical association on ${dataset.name}`,
          'Multiple categorical columns are available.'
        ) || action;
      }
    } else if (kindCounts.numeric >= 1 || kindCounts.categorical >= 1) {
      score += 1;
      reasons.push('basic descriptive review available');
      if (action.tool === 'dataset.profile') {
        action = buildStatRunAction(
          buildConcreteStatInput(kindLists, rows, 'describe'),
          rows,
          `Describe ${dataset.name}`,
          'A descriptive summary is the safest first pass.'
        ) || action;
      }
    }

    const priority = score >= 5 ? 'high' : score >= 3 ? 'medium' : 'low';
    return {
      datasetId: dataset.datasetId,
      name: dataset.name,
      priority,
      score: +score.toFixed(1),
      reasons,
      sampleRows: rows.length,
      action,
    };
  }).sort((a, b) => b.score - a.score);

  const workspaceAction = sharedColumns.length
    ? {
      label: 'Compare stat differences across workspace',
      tool: 'workspace.compare_stat_diff',
      reason: 'Use a workspace-wide difference summary before deeper follow-up.',
      inputTemplate: {},
    }
    : {
      label: 'Compare open datasets',
      tool: 'workspace.list_datasets',
      reason: 'Start with the workspace structure because shared columns are limited.',
      inputTemplate: {},
    };

  return {
    ok: true,
    data: {
      sharedColumns,
      recommendedDatasets: recommendedDatasets.slice(0, 4),
      workspaceAction,
    },
  };
}

function buildWorkspaceFormalComparePlanResult(datasetContext) {
  const workspaceDatasets = normalizeWorkspaceDatasets(datasetContext).slice(0, 4);
  if (workspaceDatasets.length < 2) {
    return {
      ok: true,
      data: {
        sharedColumns: [],
        plans: [],
      },
    };
  }

  const sharedColumns = sharedWorkspaceColumns(workspaceDatasets);
  const kinds = sharedColumnKinds(workspaceDatasets, sharedColumns);
  const activeDataset = workspaceDatasets.find((dataset) => dataset.active) || workspaceDatasets[0];
  const activeRows = Array.isArray(activeDataset?.sampleRows) ? activeDataset.sampleRows : [];
  const activeSharedKinds = classifyColumns(sharedColumns, activeRows);
  const plans = [];
  const sampleSizes = workspaceDatasets.map((dataset) => ({
    name: dataset.name,
    sampleRows: Array.isArray(dataset.sampleRows) ? dataset.sampleRows.length : 0,
  }));

  if (kinds.numeric[0] && kinds.categorical[0]) {
    const action = buildStatRunAction(
      buildConcreteStatInput(activeSharedKinds, activeRows, 'group'),
      activeRows,
      'Run aligned group comparison',
      'Use the shared numeric and categorical columns on the active dataset first.'
    );
    plans.push({
      title: 'Align group-comparison tests',
      priority: 'high',
      reason: `Shared numeric column "${kinds.numeric[0]}" and categorical column "${kinds.categorical[0]}" exist across datasets.`,
      suggestedOps: ['stat.run recommend', 't-test / ANOVA or nonparametric alternative'],
      caution: 'Verify group balance and missing values in each dataset before comparing p-values.',
      action,
    });
  }
  if (kinds.numeric.length >= 2) {
    const action = buildStatRunAction(
      buildConcreteStatInput(activeSharedKinds, activeRows, 'correlation'),
      activeRows,
      'Run aligned correlation review',
      'Start with the shared numeric columns on the active dataset.'
    );
    plans.push({
      title: 'Align correlation review',
      priority: 'high',
      reason: `Multiple shared numeric columns exist (${kinds.numeric.slice(0, 3).join(', ')}).`,
      suggestedOps: ['stat.run corr', 'workspace.compare_stat_diff'],
      caution: 'Correlation strength can shift with sample size and outliers; compare direction and magnitude, not only rank.',
      action,
    });
  }
  if (kinds.dateLike[0] && kinds.numeric[0]) {
    const action = buildStatRunAction(
      buildConcreteStatInput(activeSharedKinds, activeRows, 'trend'),
      activeRows,
      'Run aligned descriptive trend review',
      'Start from a descriptive summary before comparing time trends.'
    );
    plans.push({
      title: 'Align trend comparison',
      priority: 'medium',
      reason: `Shared date-like column "${kinds.dateLike[0]}" and numeric column "${kinds.numeric[0]}" exist.`,
      suggestedOps: ['workspace.compare_chart_plan', 'stat.run describe'],
      caution: 'Check that date granularity and sampling windows are comparable before reading trend differences.',
      action,
    });
  }
  if (kinds.categorical.length >= 2) {
    const action = buildStatRunAction(
      buildConcreteStatInput(activeSharedKinds, activeRows, 'association'),
      activeRows,
      'Run aligned categorical association review',
      'Start with a categorical association test on the active dataset.'
    );
    plans.push({
      title: 'Align categorical association review',
      priority: 'medium',
      reason: `Shared categorical columns exist (${kinds.categorical.slice(0, 3).join(', ')}).`,
      suggestedOps: ['stat.run recommend', 'chi-square style association review'],
      caution: 'Sparse levels can make contingency comparisons unstable on capped samples.',
      action,
    });
  }
  if (!plans.length) {
    const action = buildStatRunAction(
      buildConcreteStatInput(activeSharedKinds, activeRows, 'describe'),
      activeRows,
      'Run descriptive alignment',
      'Start with a descriptive summary on the active dataset.'
    );
    plans.push({
      title: 'Start with descriptive alignment',
      priority: 'medium',
      reason: 'Shared structure is limited, so descriptive comparison is the safest first step.',
      suggestedOps: ['workspace.compare_describe', 'workspace.compare_chart_plan'],
      caution: 'Activate individual datasets for deeper inference once common comparison columns are confirmed.',
      action,
    });
  }

  return {
    ok: true,
    data: {
      sharedColumns,
      sampleSizes,
      plans: plans.slice(0, 4),
    },
  };
}

module.exports = {
  buildWorkspaceCompareDescribeResult,
  buildWorkspaceCompareChartPlanResult,
  buildWorkspaceCompareStatDiffResult,
  buildWorkspaceRecommendAnalysisResult,
  buildWorkspaceFormalComparePlanResult,
};
