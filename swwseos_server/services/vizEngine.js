function toNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function labelOf(value) {
  if (value === null || value === undefined || value === '') return '(blank)';
  return String(value);
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

function aggregateNumbers(values, agg) {
  const numbers = values.map(toNumber).filter((value) => value !== null);
  if (agg === 'count') return numbers.length || values.length;
  if (!numbers.length) return 0;
  if (agg === 'sum') return numbers.reduce((sum, value) => sum + value, 0);
  return numbers.reduce((sum, value) => sum + value, 0) / numbers.length;
}

function aggregateByCategory(rows, spec = {}) {
  const xKey = spec.x;
  const yKey = spec.y;
  const hueKey = spec.hue;
  const agg = spec?.options?.agg || 'count';
  const grouped = new Map();

  for (const row of rows || []) {
    const xValue = labelOf(row?.[xKey]);
    const hueValue = hueKey ? labelOf(row?.[hueKey]) : '__single__';
    const bucketKey = `${hueValue}:::${xValue}`;
    if (!grouped.has(bucketKey)) {
      grouped.set(bucketKey, { hue: hueValue, x: xValue, values: [] });
    }
    grouped.get(bucketKey).values.push(yKey ? row?.[yKey] : 1);
  }

  if (hueKey) {
    const series = {};
    for (const item of grouped.values()) {
      if (!series[item.hue]) series[item.hue] = { x: [], y: [] };
      series[item.hue].x.push(item.x);
      series[item.hue].y.push(aggregateNumbers(item.values, agg));
    }
    return { series };
  }

  const x = [];
  const y = [];
  for (const item of grouped.values()) {
    x.push(item.x);
    y.push(aggregateNumbers(item.values, agg));
  }
  return { x, y };
}

function histogramData(rows, spec = {}) {
  const key = spec.x || spec.y;
  const bins = Math.max(5, Math.min(Number(spec?.options?.bins || 30), 200));
  const values = (rows || [])
    .map((row) => toNumber(row?.[key]))
    .filter((value) => value !== null);
  if (!values.length) return { bins: [0, 1], counts: [0] };

  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = max === min ? 1 : (max - min) / bins;
  const edges = Array.from({ length: bins + 1 }, (_, index) => +(min + (width * index)).toFixed(6));
  const counts = Array.from({ length: bins }, () => 0);

  for (const value of values) {
    const index = Math.min(Math.floor((value - min) / width), bins - 1);
    counts[Math.max(0, index)] += 1;
  }

  return { bins: edges, counts };
}

function quantileData(rows, spec = {}) {
  const yKey = spec.y || spec.x;
  const xKey = spec.x && spec.y ? spec.x : null;
  const grouped = new Map();
  for (const row of rows || []) {
    const bucket = xKey ? labelOf(row?.[xKey]) : 'overall';
    const number = toNumber(row?.[yKey]);
    if (number === null) continue;
    if (!grouped.has(bucket)) grouped.set(bucket, []);
    grouped.get(bucket).push(number);
  }

  const quantiles = {};
  for (const [bucket, values] of grouped.entries()) {
    values.sort((a, b) => a - b);
    quantiles[bucket] = {
      q1: percentile(values, 0.25),
      median: percentile(values, 0.5),
      q3: percentile(values, 0.75),
    };
  }
  return { quantiles };
}

function heatmapData(rows, spec = {}) {
  const xKey = spec.x;
  const yKey = spec.y;
  const xNumbers = (rows || []).map((row) => toNumber(row?.[xKey])).filter((value) => value !== null);
  const yNumbers = (rows || []).map((row) => toNumber(row?.[yKey])).filter((value) => value !== null);
  const bothNumeric = xNumbers.length > 2 && yNumbers.length > 2;

  if (bothNumeric) {
    const bins = Math.max(5, Math.min(Number(spec?.options?.bins || 20), 40));
    const xMin = Math.min(...xNumbers);
    const xMax = Math.max(...xNumbers);
    const yMin = Math.min(...yNumbers);
    const yMax = Math.max(...yNumbers);
    const xWidth = xMax === xMin ? 1 : (xMax - xMin) / bins;
    const yWidth = yMax === yMin ? 1 : (yMax - yMin) / bins;
    const xBins = Array.from({ length: bins }, (_, index) => +(xMin + (xWidth * (index + 0.5))).toFixed(6));
    const yBins = Array.from({ length: bins }, (_, index) => +(yMin + (yWidth * (index + 0.5))).toFixed(6));
    const zCounts = Array.from({ length: bins }, () => Array.from({ length: bins }, () => 0));

    for (const row of rows || []) {
      const xValue = toNumber(row?.[xKey]);
      const yValue = toNumber(row?.[yKey]);
      if (xValue === null || yValue === null) continue;
      const xIndex = Math.min(Math.floor((xValue - xMin) / xWidth), bins - 1);
      const yIndex = Math.min(Math.floor((yValue - yMin) / yWidth), bins - 1);
      zCounts[Math.max(0, yIndex)][Math.max(0, xIndex)] += 1;
    }

    return { xBins, yBins, zCounts };
  }

  const xLabels = [...new Set((rows || []).map((row) => labelOf(row?.[xKey])))];
  const yLabels = [...new Set((rows || []).map((row) => labelOf(row?.[yKey])))];
  const zCounts = yLabels.map(() => xLabels.map(() => 0));

  for (const row of rows || []) {
    const xLabel = labelOf(row?.[xKey]);
    const yLabel = labelOf(row?.[yKey]);
    const xIndex = xLabels.indexOf(xLabel);
    const yIndex = yLabels.indexOf(yLabel);
    if (xIndex >= 0 && yIndex >= 0) zCounts[yIndex][xIndex] += 1;
  }

  return { xBins: xLabels, yBins: yLabels, zCounts };
}

function lineData(rows, spec = {}) {
  const xKey = spec.x;
  const yKey = spec.y;
  const agg = spec?.options?.agg || 'mean';
  const resample = String(spec?.options?.resample || '').trim();
  const grouped = new Map();

  function bucketOf(value) {
    if (!resample) return labelOf(value);
    const date = new Date(String(value || ''));
    if (Number.isNaN(date.getTime())) return labelOf(value);
    const year = date.getUTCFullYear();
    const month = `${date.getUTCMonth() + 1}`.padStart(2, '0');
    const day = `${date.getUTCDate()}`.padStart(2, '0');
    if (resample === 'M') return `${year}-${month}`;
    if (resample === 'W') {
      const weekBase = new Date(Date.UTC(year, date.getUTCMonth(), date.getUTCDate() - date.getUTCDay()));
      return weekBase.toISOString().slice(0, 10);
    }
    return `${year}-${month}-${day}`;
  }

  for (const row of rows || []) {
    const bucket = bucketOf(row?.[xKey]);
    if (!grouped.has(bucket)) grouped.set(bucket, []);
    grouped.get(bucket).push(row?.[yKey]);
  }

  const x = [...grouped.keys()].sort();
  const y = x.map((bucket) => aggregateNumbers(grouped.get(bucket) || [], agg));
  return { x, y };
}

function buildPreparedFigure(rows, spec = {}) {
  const type = spec.type || 'bar';
  const options = spec.options || {};
  const layout = {
    title: options.title || '',
    xaxis: { title: options.xLabel || spec.x || '' },
    yaxis: { title: options.yLabel || spec.y || '' },
  };

  let data = [];
  if (type === 'scatter' || type === 'bubble') {
    const x = [];
    const y = [];
    const sizes = [];
    for (const row of rows || []) {
      x.push(row?.[spec.x]);
      y.push(row?.[spec.y]);
      if (spec.size) sizes.push(toNumber(row?.[spec.size]) || 8);
    }
    data = [{
      type: 'scatter',
      mode: 'markers',
      x,
      y,
      marker: spec.size ? { size: sizes } : undefined,
    }];
  } else if (type === 'line' || type === 'area') {
    const result = lineData(rows, spec);
    data = [{
      type: 'scatter',
      mode: 'lines+markers',
      x: result.x,
      y: result.y,
      fill: type === 'area' ? 'tozeroy' : undefined,
      stackgroup: type === 'area' && options.stackedArea ? 'stack-1' : undefined,
    }];
  } else if (type === 'histogram') {
    const result = histogramData(rows, spec);
    const centers = [];
    for (let index = 0; index < result.counts.length; index += 1) {
      centers.push((result.bins[index] + result.bins[index + 1]) / 2);
    }
    data = [{ type: 'bar', x: centers, y: result.counts }];
  } else if (type === 'box' || type === 'violin') {
    const result = quantileData(rows, spec);
    data = Object.entries(result.quantiles).map(([name, value]) => ({
      type: 'box',
      name,
      q1: value.q1,
      median: value.median,
      q3: value.q3,
      y: [value.q1, value.median, value.q3],
      boxpoints: false,
    }));
  } else if (type === 'heatmap') {
    const result = heatmapData(rows, spec);
    data = [{ type: 'heatmap', x: result.xBins, y: result.yBins, z: result.zCounts }];
  } else if (type === 'pie' || type === 'donut') {
    const result = aggregateByCategory(rows, spec);
    data = [{
      type: 'pie',
      labels: result.x || [],
      values: result.y || [],
      hole: type === 'donut' ? 0.45 : 0,
    }];
  } else {
    const result = aggregateByCategory(rows, spec);
    if (result.series) {
      data = Object.entries(result.series).map(([name, series]) => ({
        type: 'bar',
        name,
        x: series.x,
        y: series.y,
      }));
      layout.barmode = 'group';
    } else {
      data = [{ type: 'bar', x: result.x || [], y: result.y || [] }];
    }
  }

  return {
    fig_json: JSON.stringify({ data, layout }),
  };
}

function aggregateForSpec(rows, spec = {}) {
  const type = spec.type || 'bar';
  if (type === 'histogram') return { result: histogramData(rows, spec), op: 'histogram' };
  if (type === 'box' || type === 'violin') return { result: quantileData(rows, spec), op: 'distribution' };
  if (type === 'heatmap') return { result: heatmapData(rows, spec), op: 'heatmap' };
  if (type === 'line') return { result: lineData(rows, spec), op: 'resample' };
  return { result: aggregateByCategory(rows, spec), op: 'groupby' };
}

module.exports = {
  aggregateForSpec,
  buildPreparedFigure,
};
