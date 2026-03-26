const DEFAULT_CONTEXT_ROW_LIMIT = 120;
const DEFAULT_PROFILE_SAMPLE_LIMIT = 60;
const DEFAULT_WORKSPACE_DATASET_LIMIT = 4;
const DEFAULT_SHARED_COLUMN_LIMIT = 8;
const DEFAULT_TOP_LIMIT = 5;

function sampleRows(rows, limit = DEFAULT_PROFILE_SAMPLE_LIMIT) {
  return Array.isArray(rows) ? rows.slice(0, limit) : [];
}

function classifyColumns(columns, rows) {
  const numeric = [];
  const categorical = [];
  const dateLike = [];
  for (const column of columns || []) {
    const values = rows.map((row) => row?.[column]);
    let nonNull = 0;
    let numericHits = 0;
    let dateHits = 0;
    for (const value of values) {
      if (value === null || value === undefined || value === '') continue;
      nonNull += 1;
      if (Number.isFinite(Number(value))) numericHits += 1;
      else if (!Number.isNaN(new Date(String(value)).getTime())) dateHits += 1;
    }
    if (nonNull > 0 && numericHits / nonNull >= 0.7) numeric.push(column);
    else if (nonNull > 0 && dateHits / nonNull >= 0.7) dateLike.push(column);
    else categorical.push(column);
  }
  return { numeric, categorical, dateLike };
}

function inferColumnKinds(columns, rows) {
  const kinds = classifyColumns(columns, rows);
  return {
    numeric: kinds.numeric.length,
    categorical: kinds.categorical.length,
    dateLike: kinds.dateLike.length,
  };
}

function topMissingColumns(columns, rows, topLimit = DEFAULT_TOP_LIMIT) {
  const output = [];
  for (const column of columns || []) {
    let missing = 0;
    for (const row of rows) {
      const value = row?.[column];
      if (value === null || value === undefined || value === '') missing += 1;
    }
    if (rows.length && missing > 0) {
      output.push({
        name: column,
        rate: +((missing / rows.length) * 100).toFixed(1),
      });
    }
  }
  output.sort((a, b) => b.rate - a.rate);
  return output.slice(0, topLimit);
}

function percentile(sortedValues, p) {
  if (!sortedValues.length) return null;
  const index = (sortedValues.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sortedValues[lower];
  const weight = index - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function topOutlierColumns(columns, rows, topLimit = DEFAULT_TOP_LIMIT) {
  const output = [];
  for (const column of columns || []) {
    const values = rows
      .map((row) => row?.[column])
      .filter((value) => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)))
      .map((value) => Number(value))
      .sort((a, b) => a - b);
    if (values.length < 8) continue;
    const q1 = percentile(values, 0.25);
    const q3 = percentile(values, 0.75);
    const iqr = q3 - q1;
    if (!Number.isFinite(iqr) || iqr <= 0) continue;
    const low = q1 - (1.5 * iqr);
    const high = q3 + (1.5 * iqr);
    const count = values.filter((value) => value < low || value > high).length;
    if (count > 0) {
      output.push({
        name: column,
        count,
        rate: +((count / values.length) * 100).toFixed(1),
      });
    }
  }
  output.sort((a, b) => b.rate - a.rate);
  return output.slice(0, topLimit);
}

function topImbalancedCategoricals(columns, rows, topLimit = DEFAULT_TOP_LIMIT) {
  const output = [];
  for (const column of columns || []) {
    const values = rows.map((row) => row?.[column]);
    let nonNull = 0;
    let numericHits = 0;
    let dateHits = 0;
    const counts = new Map();
    for (const value of values) {
      if (value === null || value === undefined || value === '') continue;
      nonNull += 1;
      if (Number.isFinite(Number(value))) numericHits += 1;
      else if (!Number.isNaN(new Date(String(value)).getTime())) dateHits += 1;
      const key = String(value);
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    if (nonNull < 5) continue;
    if (numericHits / nonNull >= 0.7 || dateHits / nonNull >= 0.7) continue;
    const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
    if (ranked.length < 2) continue;
    const [topValue, topCount] = ranked[0];
    const rate = +((topCount / nonNull) * 100).toFixed(1);
    if (rate >= 75) {
      output.push({
        name: column,
        topValue,
        rate,
        uniqueCount: ranked.length,
      });
    }
  }
  output.sort((a, b) => b.rate - a.rate);
  return output.slice(0, topLimit);
}

function normalizeWorkspaceDatasets(datasetContext, options = {}) {
  const datasets = Array.isArray(datasetContext?.workspaceDatasets) ? datasetContext.workspaceDatasets : [];
  const workspaceLimit = Number(options.workspaceLimit || DEFAULT_WORKSPACE_DATASET_LIMIT);
  const contextRowLimit = Number(options.contextRowLimit || DEFAULT_CONTEXT_ROW_LIMIT);
  return datasets.slice(0, workspaceLimit).map((dataset) => ({
    datasetId: String(dataset?.datasetId || ''),
    name: String(dataset?.name || 'untitled'),
    rowCount: Number(dataset?.rowCount || 0),
    columnCount: Number(dataset?.columnCount || 0),
    columns: Array.isArray(dataset?.columns) ? dataset.columns.map((column) => String(column)) : [],
    sampleRows: sampleRows(dataset?.sampleRows || [], contextRowLimit),
    active: !!dataset?.active,
    dirty: !!dataset?.dirty,
  }));
}

function sharedWorkspaceColumns(workspaceDatasets, limit = DEFAULT_SHARED_COLUMN_LIMIT) {
  if (!Array.isArray(workspaceDatasets) || workspaceDatasets.length < 2) return [];
  return workspaceDatasets
    .map((dataset) => dataset.columns || [])
    .reduce((shared, columns, index) => {
      if (!index) return [...columns];
      return shared.filter((column) => columns.includes(column));
    }, [])
    .slice(0, limit);
}

function buildFacts(datasetContext, options = {}) {
  const columns = Array.isArray(datasetContext?.columns) ? datasetContext.columns : [];
  const profileSampleLimit = Number(options.profileSampleLimit || DEFAULT_PROFILE_SAMPLE_LIMIT);
  const workspaceLimit = Number(options.workspaceLimit || DEFAULT_WORKSPACE_DATASET_LIMIT);
  const contextRowLimit = Number(options.contextRowLimit || DEFAULT_CONTEXT_ROW_LIMIT);
  const sharedColumnLimit = Number(options.sharedColumnLimit || DEFAULT_SHARED_COLUMN_LIMIT);
  const topLimit = Number(options.topLimit || DEFAULT_TOP_LIMIT);
  const sampledRows = sampleRows(datasetContext?.sampleRows || datasetContext?.rows || [], profileSampleLimit);
  const kinds = inferColumnKinds(columns, sampledRows);
  const profile = datasetContext?.profileSummary || {};
  const workspaceDatasets = normalizeWorkspaceDatasets(datasetContext, { workspaceLimit, contextRowLimit });
  return {
    datasetId: String(datasetContext?.datasetId || ''),
    datasetName: String(datasetContext?.datasetName || 'untitled'),
    rowCount: Number(datasetContext?.rowCount || datasetContext?.rows?.length || 0),
    columnCount: Number(datasetContext?.columnCount || columns.length || 0),
    numericColumns: kinds.numeric,
    categoricalColumns: kinds.categorical,
    dateColumns: kinds.dateLike,
    missingColumns: topMissingColumns(columns, sampledRows, topLimit),
    outlierColumns: topOutlierColumns(columns, sampledRows, topLimit),
    imbalancedColumns: topImbalancedCategoricals(columns, sampledRows, topLimit),
    duplicateRows: Number(profile?.duplicates || 0),
    warningCount: Array.isArray(profile?.warnings) ? profile.warnings.length : 0,
    warnings: Array.isArray(profile?.warnings) ? profile.warnings : [],
    topCorrCount: Number(profile?.topCorrCount || 0),
    topAnovaCount: Number(profile?.topAnovaCount || 0),
    workspaceCount: workspaceDatasets.length,
    workspaceDatasets,
    sharedColumns: sharedWorkspaceColumns(workspaceDatasets, sharedColumnLimit),
  };
}

module.exports = {
  sampleRows,
  classifyColumns,
  inferColumnKinds,
  topMissingColumns,
  normalizeWorkspaceDatasets,
  sharedWorkspaceColumns,
  buildFacts,
};
