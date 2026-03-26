# `/stat/run` API Contract (Canonical)
Last updated: 2026-03-07

## Endpoint
- Method: `POST`
- Path: `/stat/run`
- Content-Type: `application/json`

## Request (Base)
```json
{
  "op": "describe",
  "rows": [ { "colA": 1, "colB": "x" } ],
  "args": {},
  "options": {}
}
```

Rules:
- `op`: required string
- `rows`: required array (can be empty; empty returns `ok:true` + empty summary)
- `args`: optional object (non-object is ignored as `{}`)
- `options`: optional object (non-object is ignored as `{}`)

## Response (Success)
```json
{
  "ok": true,
  "op": "describe",
  "summary": { "title": "...", "conclusion": "...", "stats": {} },
  "tables": [ { "name": "table_name", "columns": [], "rows": [] } ],
  "figures": [],
  "warnings": [],
  "meta": { "execMs": 0 }
}
```

## Response (Error)
```json
{
  "ok": false,
  "op": "describe",
  "code": "ERROR_CODE",
  "message": "human readable message",
  "details": {},
  "meta": { "execMs": 0 }
}
```

Current guardrail error codes:
- `INVALID_JSON`
- `INVALID_PAYLOAD`
- `INVALID_ROWS_TYPE`
- `UNSUPPORTED_OP`
- `STAT_RUN_EXCEPTION`

## Supported `op` Values
- `describe`
- `corr`
- `ttest`
- `chisq`
- `ols`
- `anova`
- `normality`
- `ci_mean`
- `quality`
- `quality_process`
- `mannwhitney`
- `wilcoxon`
- `kruskal`
- `tukey`
- `pairwise_adjusted`
- `recommend`

## `args` by op
- `ttest`: `{ "value": string, "group": string }`
- `chisq`: `{ "a": string, "b": string }`
- `ols`: `{ "y": string, "x": string[] }`
- `anova`: `{ "value": string, "group": string }`
- `normality`: `{ "column": string }`
- `ci_mean`: `{ "column": string, "confidence": number }`
- `quality`: `{ "columns": string[], "method": "iqr|zscore", "iqrK": number, "zThresh": number }`
- `quality_process`: `{ "strategy": "exclude|winsorize", "method": "iqr|zscore", "columns": string[], "iqrK": number, "zThresh": number, "dropMissing": boolean }`
- `mannwhitney`: `{ "value": string, "group": string }`
- `wilcoxon`: `{ "a": string, "b": string }`
- `kruskal`: `{ "value": string, "group": string }`
- `tukey`: `{ "value": string, "group": string, "alpha": number }`
- `pairwise_adjusted`: `{ "value": string, "group": string, "pAdjust": "holm|bonferroni|fdr_bh" }`
- `recommend`: `{ "goal": string, "value": string, "group": string, "a": string, "b": string }`

## Key Contract Notes (Current Implementation)
- Effect size CI fields:
  - `ttest.summary.stats`: `cohen_d_ci_low`, `cohen_d_ci_high`
  - `chisq.summary.stats`: `cramers_v_ci_low`, `cramers_v_ci_high`
  - `anova.summary.stats`: `eta_sq_ci_low`, `eta_sq_ci_high`
- OLS diagnostics:
  - `summary.stats`: `bp_lm`, `bp_lm_p`, `bp_f`, `bp_f_p`
  - `figures.type`: includes `residual_fitted`, `residual_qq` (when available)
- Quality processing deltas:
  - `summary.stats`: `rows_with_missing_before/after`, `outlier_rows_before/after`, `outlier_value_count_before/after`
  - `quality_process_summary` table columns include:
    - `outlier_count_before`
    - `outlier_count_after`
    - `outlier_count_delta`
