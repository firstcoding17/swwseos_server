const { buildFacts } = require('./datasetProfiler');

function buildProfileResult(datasetContext) {
  const facts = buildFacts(datasetContext);
  return {
    ok: true,
    data: {
      datasetId: facts.datasetId,
      datasetName: facts.datasetName,
      rowCount: facts.rowCount,
      columnCount: facts.columnCount,
      numericColumns: facts.numericColumns,
      categoricalColumns: facts.categoricalColumns,
      dateColumns: facts.dateColumns,
      missingColumns: facts.missingColumns,
      outlierColumns: facts.outlierColumns,
      imbalancedColumns: facts.imbalancedColumns,
      duplicateRows: facts.duplicateRows,
      topCorrCount: facts.topCorrCount,
      topAnovaCount: facts.topAnovaCount,
      warnings: facts.warnings,
    },
  };
}

function buildFlagsResult(datasetContext) {
  const facts = buildFacts(datasetContext);
  const flags = [];
  if (facts.missingColumns.length) {
    flags.push({
      kind: 'missing',
      severity: facts.missingColumns[0].rate >= 20 ? 'high' : 'medium',
      summary: `Missing-value hotspots: ${facts.missingColumns.map((item) => `${item.name} (${item.rate}%)`).join(', ')}`,
    });
  }
  if (facts.duplicateRows > 0) {
    flags.push({
      kind: 'duplicate',
      severity: 'medium',
      summary: `Duplicate rows detected in profile sample: ${facts.duplicateRows}`,
    });
  }
  if (facts.outlierColumns.length) {
    flags.push({
      kind: 'outlier',
      severity: facts.outlierColumns[0].rate >= 10 ? 'high' : 'medium',
      summary: `Possible outlier-heavy numeric columns: ${facts.outlierColumns.map((item) => `${item.name} (${item.rate}%)`).join(', ')}`,
    });
  }
  if (facts.imbalancedColumns.length) {
    flags.push({
      kind: 'imbalance',
      severity: facts.imbalancedColumns[0].rate >= 90 ? 'high' : 'medium',
      summary: `Categorical imbalance detected: ${facts.imbalancedColumns.map((item) => `${item.name}=${item.topValue} (${item.rate}%)`).join(', ')}`,
    });
  }
  if (facts.topCorrCount > 0) {
    flags.push({
      kind: 'correlation',
      severity: 'medium',
      summary: `Correlation-heavy relationships detected: ${facts.topCorrCount} candidate pairs`,
    });
  }
  if (facts.warningCount > 0) {
    flags.push({
      kind: 'warning',
      severity: 'medium',
      summary: facts.warnings.join(' '),
    });
  }
  if (!flags.length) {
    flags.push({
      kind: 'clean',
      severity: 'low',
      summary: 'No major flag was detected from the lightweight dataset context.',
    });
  }
  return {
    ok: true,
    data: {
      datasetName: facts.datasetName,
      flags,
    },
  };
}

function buildWorkspaceCurrentResult(datasetContext) {
  return buildProfileResult(datasetContext);
}

function buildWorkspaceListResult(datasetContext) {
  const facts = buildFacts(datasetContext);
  return {
    ok: true,
    data: {
      activeDatasetId: facts.datasetId,
      totalDatasets: facts.workspaceCount,
      datasets: facts.workspaceDatasets,
      sharedColumns: facts.sharedColumns,
    },
  };
}

module.exports = {
  buildProfileResult,
  buildFlagsResult,
  buildWorkspaceCurrentResult,
  buildWorkspaceListResult,
};
