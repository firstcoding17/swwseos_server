#!/usr/bin/env python
import sys, json, time
import pandas as pd
import numpy as np

#pip install pandas numpy //should check
def j(obj):
    print(json.dumps(obj, ensure_ascii=False))

def to_table(df: pd.DataFrame, name: str):
    # DataFrame -> {name, columns, rows}
    return {
        "name": name,
        "columns": [str(c) for c in df.columns],
        "rows": [[None if (isinstance(x, float) and np.isnan(x)) else x for x in row] for row in df.values.tolist()]
    }

def safe_value_counts(s: pd.Series, topn=10):
    vc = s.astype("string").value_counts(dropna=False).head(topn)
    total = int(s.shape[0])
    out = []
    for k, v in vc.items():
        label = "NaN" if (k is pd.NA or str(k) == "<NA>") else str(k)
        out.append([label, int(v), float(v)/total if total else 0.0])
    return pd.DataFrame(out, columns=["value", "count", "ratio"])

def numeric_core_stats(df: pd.DataFrame, cols: list):
    rows = []
    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce").dropna()
        if s.empty:
            continue

        q1 = float(s.quantile(0.25))
        q3 = float(s.quantile(0.75))
        iqr = q3 - q1
        mode_vals = s.mode(dropna=True)
        mode_val = float(mode_vals.iloc[0]) if not mode_vals.empty else None

        rows.append([
            c,
            int(s.shape[0]),
            float(s.mean()),
            float(s.median()),
            mode_val,
            float(s.min()),
            float(s.max()),
            float(s.max() - s.min()),
            float(s.std(ddof=1)) if s.shape[0] > 1 else 0.0,
            float(s.var(ddof=1)) if s.shape[0] > 1 else 0.0,
            q1,
            q3,
            float(iqr),
            float(s.skew()) if s.shape[0] > 2 else 0.0,
            float(s.kurt()) if s.shape[0] > 3 else 0.0,
        ])

    return pd.DataFrame(rows, columns=[
        "column", "n", "mean", "median", "mode", "min", "max", "range",
        "std", "variance", "q1", "q3", "iqr", "skewness", "kurtosis"
    ])

def _finite_percentile_ci(values, alpha=0.05):
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.shape[0] < 20:
        return None, None
    lo = float(np.percentile(arr, 100.0 * (alpha / 2.0)))
    hi = float(np.percentile(arr, 100.0 * (1.0 - alpha / 2.0)))
    return lo, hi

def describe_report(df: pd.DataFrame, topn_cat=10):
    meta = {}
    meta["rows"] = int(df.shape[0])
    meta["cols"] = int(df.shape[1])

    # Basic dtype summary
    dtype_summary = df.dtypes.astype(str).value_counts().reset_index()
    dtype_summary.columns = ["dtype", "count"]

    # Top columns by missing rate
    na = df.isna().mean().sort_values(ascending=False)
    na_top = na.head(10).reset_index()
    na_top.columns = ["column", "null_rate"]

    # Duplicate rows
    dup = int(df.duplicated().sum())

    # Split numeric and categorical columns
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = [c for c in df.columns if c not in num_cols]

    # Numeric describe
    num_desc = None
    num_core = None
    if len(num_cols) > 0:
        num_desc = df[num_cols].describe().T  # count mean std min 25% 50% 75% max
        # Keep values unformatted; frontend handles display formatting.
        num_desc = num_desc.reset_index().rename(columns={"index":"column"})
        num_core = numeric_core_stats(df, num_cols)

    # Categorical describe + TopN frequency tables
    cat_desc = None
    cat_top_tables = []
    if len(cat_cols) > 0:
        # Build manually to handle mixed/object columns consistently.
        rows = []
        for c in cat_cols:
            s = df[c]
            s_nonnull = s.dropna()
            unique = int(s_nonnull.nunique()) if s_nonnull.shape[0] else 0
            if s_nonnull.shape[0]:
                top = s_nonnull.astype("string").value_counts().idxmax()
                freq = int(s_nonnull.astype("string").value_counts().max())
            else:
                top, freq = None, 0
            rows.append([c, unique, None if top is None else str(top), freq])
            cat_top_tables.append(to_table(safe_value_counts(s, topn_cat), f"top_{c}"))
        cat_desc = pd.DataFrame(rows, columns=["column","unique","top","freq"])

    result = {
        "ok": True,
        "op": "describe",
        "summary": {
            "title": "Descriptive Statistics Report",
            "conclusion": "Check top missing-value columns first, then review numeric distributions/outliers and categorical top frequencies.",
            "stats": {
                "rows": meta["rows"],
                "cols": meta["cols"],
                "dup_rows": dup
            }
        },
        "tables": [
            to_table(dtype_summary, "dtype_summary"),
            to_table(na_top, "null_rate_top"),
            *([to_table(num_desc, "numeric_describe")] if num_desc is not None else []),
            *([to_table(num_core, "numeric_core_stats")] if num_core is not None and not num_core.empty else []),
            *([to_table(cat_desc, "categorical_describe")] if cat_desc is not None else []),
            *cat_top_tables
        ],
        "warnings": [],
        "meta": meta
    }

    # Warnings
    if meta["rows"] < 20:
        result["warnings"].append("Sample size is very small (rows < 20). Use results for reference only.")
    if len(num_cols) == 0:
        result["warnings"].append("No numeric columns found.")
    if len(cat_cols) == 0:
        result["warnings"].append("No categorical columns found.")

    return result

def quality_report(df: pd.DataFrame, columns=None, method="iqr", iqr_k=1.5, z_thresh=3.0):
    rows_n = int(df.shape[0])
    cols_n = int(df.shape[1])

    def detect_numeric_like(frame: pd.DataFrame):
        out = []
        if frame.shape[0] == 0:
            return out
        min_non_null = max(3, int(frame.shape[0] * 0.6))
        for c in frame.columns:
            s = pd.to_numeric(frame[c], errors="coerce")
            if int(s.notna().sum()) >= min_non_null:
                out.append(str(c))
        return out

    numeric_like = detect_numeric_like(df)
    req_cols = [str(c) for c in (columns or []) if str(c) in df.columns]
    target_cols = [c for c in req_cols if c in numeric_like] if req_cols else list(numeric_like)

    miss_rows = []
    for c in df.columns:
        miss = int(df[c].isna().sum())
        miss_rows.append([str(c), miss, float(miss / rows_n) if rows_n else 0.0])
    missing_tbl = pd.DataFrame(miss_rows, columns=["column", "missing_count", "missing_rate"]).sort_values(
        ["missing_rate", "missing_count"], ascending=False
    )

    row_missing_count = df.isna().sum(axis=1)
    pattern = row_missing_count.value_counts().sort_index()
    pattern_tbl = pd.DataFrame({
        "missing_cols_in_row": pattern.index.astype(int),
        "row_count": pattern.values.astype(int),
    })
    pattern_tbl["ratio"] = pattern_tbl["row_count"] / max(rows_n, 1)

    out_rows = []
    for c in target_cols:
        s = pd.to_numeric(df[c], errors="coerce").dropna()
        n = int(s.shape[0])
        if n < 5:
            out_rows.append([c, n, None, None, 0, None, None, 0])
            continue
        q1 = float(s.quantile(0.25))
        q3 = float(s.quantile(0.75))
        iqr = q3 - q1
        i_lo = q1 - float(iqr_k) * iqr
        i_hi = q3 + float(iqr_k) * iqr
        i_mask = (s < i_lo) | (s > i_hi)
        i_cnt = int(i_mask.sum())

        mean = float(s.mean())
        std = float(s.std(ddof=1)) if n > 1 else 0.0
        if std > 0:
            z = np.abs((s - mean) / std)
            z_cnt = int((z > float(z_thresh)).sum())
            z_lo = mean - float(z_thresh) * std
            z_hi = mean + float(z_thresh) * std
        else:
            z_cnt = 0
            z_lo = None
            z_hi = None

        out_rows.append([c, n, float(i_lo), float(i_hi), i_cnt, z_lo, z_hi, z_cnt])

    outlier_tbl = pd.DataFrame(
        out_rows,
        columns=["column", "n", "iqr_low", "iqr_high", "iqr_outliers", "z_low", "z_high", "z_outliers"],
    )

    any_missing_rows = int((row_missing_count > 0).sum())
    complete_rows = rows_n - any_missing_rows
    selected_method = str(method or "iqr").lower()
    if selected_method not in ("iqr", "zscore"):
        selected_method = "iqr"

    return {
        "ok": True,
        "op": "quality",
        "inputs": {
            "columns": target_cols,
            "method": selected_method,
            "iqrK": float(iqr_k),
            "zThresh": float(z_thresh),
        },
        "summary": {
            "title": "Missing / Outlier Quality Report",
            "conclusion": "Review missing patterns and outlier counts before inferential analysis.",
            "stats": {
                "rows": rows_n,
                "cols": cols_n,
                "rows_with_missing": any_missing_rows,
                "complete_rows": complete_rows,
                "target_numeric_cols": int(len(target_cols)),
            },
        },
        "tables": [
            to_table(missing_tbl, "missing_by_column"),
            to_table(pattern_tbl, "missing_pattern"),
            to_table(outlier_tbl, "outlier_summary"),
        ],
        "warnings": [],
        "meta": {"rows": rows_n, "cols": cols_n},
    }

def quality_process_report(
    df: pd.DataFrame,
    strategy="exclude",
    method="iqr",
    columns=None,
    iqr_k=1.5,
    z_thresh=3.0,
    drop_missing=None,
):
    rows_n = int(df.shape[0])
    cols_n = int(df.shape[1])

    def detect_numeric_like(frame: pd.DataFrame):
        out = []
        if frame.shape[0] == 0:
            return out
        min_non_null = max(3, int(frame.shape[0] * 0.6))
        for c in frame.columns:
            s = pd.to_numeric(frame[c], errors="coerce")
            if int(s.notna().sum()) >= min_non_null:
                out.append(str(c))
        return out

    strategy = str(strategy or "exclude").lower()
    if strategy not in ("exclude", "winsorize"):
        strategy = "exclude"

    method = str(method or "iqr").lower()
    if method not in ("iqr", "zscore"):
        method = "iqr"

    if drop_missing is None:
        drop_missing = strategy == "exclude"
    drop_missing = bool(drop_missing)

    numeric_like = detect_numeric_like(df)
    req_cols = [str(c) for c in (columns or []) if str(c) in df.columns]
    target_cols = [c for c in req_cols if c in numeric_like] if req_cols else list(numeric_like)

    d = df.copy()
    missing_mask = d.isna().any(axis=1)
    outlier_mask = pd.Series(False, index=d.index)
    stat_rows = []
    bounds_by_col = {}
    outlier_before_by_col = {}

    for c in target_cols:
        s_num = pd.to_numeric(d[c], errors="coerce")
        s_valid = s_num.dropna()
        if s_valid.shape[0] < 5:
            stat_rows.append([c, int(s_valid.shape[0]), None, None, 0])
            continue

        if method == "zscore":
            mean = float(s_valid.mean())
            std = float(s_valid.std(ddof=1)) if s_valid.shape[0] > 1 else 0.0
            if std > 0:
                low = mean - float(z_thresh) * std
                high = mean + float(z_thresh) * std
            else:
                low = float(mean)
                high = float(mean)
        else:
            q1 = float(s_valid.quantile(0.25))
            q3 = float(s_valid.quantile(0.75))
            iqr = q3 - q1
            low = q1 - float(iqr_k) * iqr
            high = q3 + float(iqr_k) * iqr

        m = s_num.notna() & ((s_num < low) | (s_num > high))
        m_count = int(m.sum())
        outlier_mask = outlier_mask | m
        stat_rows.append([c, int(s_valid.shape[0]), float(low), float(high), m_count])
        bounds_by_col[c] = (float(low), float(high))
        outlier_before_by_col[c] = m_count

        if strategy == "winsorize":
            d[c] = s_num.clip(lower=low, upper=high)

    if strategy == "exclude":
        drop_mask = outlier_mask | (missing_mask if drop_missing else False)
        cleaned = d.loc[~drop_mask].copy()
    else:
        cleaned = d.copy()
        if drop_missing:
            cleaned = cleaned.loc[~missing_mask].copy()

    removed_rows = rows_n - int(cleaned.shape[0])
    cleaned_records = json.loads(cleaned.to_json(orient="records", force_ascii=False))
    summary_tbl = pd.DataFrame(stat_rows, columns=["column", "n", "low_bound", "high_bound", "outlier_count"])
    missing_rows_before = int(missing_mask.sum())
    missing_rows_after = int(cleaned.isna().any(axis=1).sum()) if cleaned.shape[0] else 0
    outlier_rows_before = int(outlier_mask.sum())

    outlier_rows_after_mask = pd.Series(False, index=cleaned.index)
    outlier_value_count_after = 0
    outlier_after_by_col = {}
    for c in target_cols:
        if c not in cleaned.columns:
            outlier_after_by_col[c] = 0
            continue
        bounds = bounds_by_col.get(c)
        if not bounds:
            outlier_after_by_col[c] = 0
            continue
        low, high = bounds
        s_after = pd.to_numeric(cleaned[c], errors="coerce")
        m_after = s_after.notna() & ((s_after < low) | (s_after > high))
        m_after_count = int(m_after.sum())
        outlier_rows_after_mask = outlier_rows_after_mask | m_after
        outlier_value_count_after += m_after_count
        outlier_after_by_col[c] = m_after_count

    outlier_rows_after = int(outlier_rows_after_mask.sum()) if cleaned.shape[0] else 0
    outlier_value_count_before = int(summary_tbl["outlier_count"].sum()) if not summary_tbl.empty else 0
    if not summary_tbl.empty:
        summary_tbl["outlier_count_before"] = summary_tbl["column"].map(lambda c: int(outlier_before_by_col.get(str(c), 0)))
        summary_tbl["outlier_count_after"] = summary_tbl["column"].map(lambda c: int(outlier_after_by_col.get(str(c), 0)))
        summary_tbl["outlier_count_delta"] = summary_tbl["outlier_count_after"] - summary_tbl["outlier_count_before"]

    return {
        "ok": True,
        "op": "quality_process",
        "inputs": {
            "strategy": strategy,
            "method": method,
            "columns": target_cols,
            "iqrK": float(iqr_k),
            "zThresh": float(z_thresh),
            "dropMissing": drop_missing,
        },
        "summary": {
            "title": "Quality Processing",
            "conclusion": "Dataset quality processing applied. Review row count changes before downstream analysis.",
            "stats": {
                "rows_before": rows_n,
                "rows_after": int(cleaned.shape[0]),
                "rows_removed": int(removed_rows),
                "target_numeric_cols": int(len(target_cols)),
                "rows_with_missing_before": missing_rows_before,
                "rows_with_missing_after": missing_rows_after,
                "outlier_rows_before": outlier_rows_before,
                "outlier_rows_after": outlier_rows_after,
                "outlier_value_count_before": outlier_value_count_before,
                "outlier_value_count_after": int(outlier_value_count_after),
            },
        },
        "tables": [to_table(summary_tbl, "quality_process_summary")],
        "data": {"columns": [str(c) for c in cleaned.columns], "rows": cleaned_records},
        "warnings": [],
        "meta": {"rows": int(cleaned.shape[0]), "cols": cols_n},
    }

def recommend_report(
    df: pd.DataFrame,
    goal: str = "auto",
    value_col: str = None,
    group_col: str = None,
    a_col: str = None,
    b_col: str = None,
):
    def detect_numeric_cols(frame: pd.DataFrame):
        if frame.shape[0] == 0:
            return []
        out = []
        min_non_null = max(3, int(frame.shape[0] * 0.6))
        for c in frame.columns:
            s = pd.to_numeric(frame[c], errors="coerce")
            if int(s.notna().sum()) >= min_non_null:
                out.append(str(c))
        return out

    rows_n = int(df.shape[0])
    cols_n = int(df.shape[1])
    numeric_cols = detect_numeric_cols(df)
    cat_cols = [str(c) for c in df.columns if str(c) not in set(numeric_cols)]

    def first_numeric():
        return numeric_cols[0] if numeric_cols else None

    def second_numeric(exclude=None):
        for c in numeric_cols:
            if c != exclude:
                return c
        return None

    def first_cat():
        return cat_cols[0] if cat_cols else None

    value = str(value_col) if value_col in df.columns else None
    group = str(group_col) if group_col in df.columns else None
    a = str(a_col) if a_col in df.columns else None
    b = str(b_col) if b_col in df.columns else None

    if value is None and first_numeric() is not None:
        value = first_numeric()
    if group is None and first_cat() is not None:
        group = first_cat()

    recs = []

    def add_rec(op, label, reason, args=None, chart_type=None, x_hint=None, y_hint=None, priority=50):
        recs.append({
            "priority": int(priority),
            "op": str(op),
            "label": str(label),
            "reason": str(reason),
            "args": args or {},
            "chart": {"type": chart_type, "x": x_hint, "y": y_hint},
        })

    # Always-safe baseline.
    add_rec(
        "describe",
        "Descriptive Statistics",
        "Start with basic quality checks (missing values, distributions, summary stats).",
        {},
        "histogram",
        value,
        None,
        90,
    )
    if len(numeric_cols) >= 2:
        add_rec(
            "corr",
            "Correlation Analysis",
            "At least two numeric columns are available for pair relationships.",
            {},
            "scatter",
            numeric_cols[0],
            numeric_cols[1],
            80,
        )

    goal_norm = str(goal or "auto").strip().lower()
    if goal_norm in ("group_compare", "compare_groups"):
        goal_norm = "compare_two_groups"
    if goal_norm in ("group_compare_multi",):
        goal_norm = "compare_multi_groups"
    if goal_norm in ("paired", "paired_test"):
        goal_norm = "paired_difference"

    n_levels = 0
    if group in df.columns:
        n_levels = int(df[group].astype("string").dropna().nunique())

    if goal_norm in ("auto", "compare_two_groups", "compare_multi_groups"):
        if value and group and n_levels >= 2:
            if n_levels == 2:
                add_rec(
                    "ttest",
                    "Independent t-test",
                    "Two-group numeric comparison with parametric assumptions.",
                    {"value": value, "group": group},
                    "box",
                    group,
                    value,
                    10,
                )
                add_rec(
                    "mannwhitney",
                    "Mann-Whitney U",
                    "Two-group nonparametric comparison when normality is uncertain.",
                    {"value": value, "group": group},
                    "box",
                    group,
                    value,
                    15,
                )
            else:
                add_rec(
                    "anova",
                    "One-way ANOVA",
                    "Multi-group numeric comparison with parametric assumptions.",
                    {"value": value, "group": group},
                    "box",
                    group,
                    value,
                    10,
                )
                add_rec(
                    "kruskal",
                    "Kruskal-Wallis",
                    "Multi-group nonparametric alternative to ANOVA.",
                    {"value": value, "group": group},
                    "box",
                    group,
                    value,
                    15,
                )
                add_rec(
                    "tukey",
                    "Tukey HSD Post-hoc",
                    "Identify which group pairs differ after ANOVA-style comparison.",
                    {"value": value, "group": group, "alpha": 0.05},
                    "box",
                    group,
                    value,
                    20,
                )
                add_rec(
                    "pairwise_adjusted",
                    "Pairwise Adjusted Comparisons",
                    "Pairwise tests with multiple-comparison correction.",
                    {"value": value, "group": group, "pAdjust": "holm"},
                    "box",
                    group,
                    value,
                    25,
                )

    if goal_norm in ("auto", "association", "categorical_association"):
        ca = a if (a in cat_cols) else (cat_cols[0] if len(cat_cols) >= 1 else None)
        cb = b if (b in cat_cols and b != ca) else (cat_cols[1] if len(cat_cols) >= 2 else None)
        if ca and cb:
            add_rec(
                "chisq",
                "Chi-square Independence",
                "Two categorical columns can be tested for association.",
                {"a": ca, "b": cb},
                "bar",
                ca,
                None,
                30,
            )

    if goal_norm in ("auto", "paired_difference"):
        pa = a if (a in numeric_cols) else first_numeric()
        pb = b if (b in numeric_cols and b != pa) else second_numeric(pa)
        if pa and pb:
            add_rec(
                "wilcoxon",
                "Wilcoxon Signed-Rank",
                "Paired numeric columns are available for within-subject difference testing.",
                {"a": pa, "b": pb},
                "scatter",
                pa,
                pb,
                30,
            )

    if goal_norm in ("auto", "regression", "prediction"):
        y = value if (value in numeric_cols) else first_numeric()
        x = [c for c in numeric_cols if c != y][:3]
        if y and x:
            add_rec(
                "ols",
                "OLS Linear Regression",
                "Numeric target and predictors are available.",
                {"y": y, "x": x},
                "scatter",
                x[0],
                y,
                35,
            )

    if goal_norm in ("auto", "distribution", "quality", "assumption"):
        c = value if (value in numeric_cols) else first_numeric()
        if c:
            add_rec(
                "normality",
                "Normality Check",
                "Check shape assumptions before choosing parametric tests.",
                {"column": c},
                "histogram",
                c,
                None,
                40,
            )
            add_rec(
                "ci_mean",
                "Mean Confidence Interval",
                "Estimate uncertainty of the mean for a numeric column.",
                {"column": c, "confidence": 0.95},
                "histogram",
                c,
                None,
                45,
            )

    # De-duplicate by (op,args JSON) and sort.
    seen = set()
    uniq = []
    for r in sorted(recs, key=lambda x: (x["priority"], x["op"])):
        key = (r["op"], json.dumps(r.get("args") or {}, ensure_ascii=False, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    tbl_rows = []
    for i, r in enumerate(uniq, start=1):
        chart = r.get("chart") or {}
        tbl_rows.append([
            int(i),
            r["op"],
            r["label"],
            r["reason"],
            chart.get("type"),
            chart.get("x"),
            chart.get("y"),
            json.dumps(r.get("args") or {}, ensure_ascii=False),
        ])
    rec_tbl = pd.DataFrame(tbl_rows, columns=["rank", "op", "label", "reason", "chart_type", "x_hint", "y_hint", "args_json"])

    conclusion = "Use top-ranked recommendation first, then validate assumptions and compare alternatives."
    if not uniq:
        conclusion = "No suitable recommendation was generated for the current dataset/goal."

    warnings = []
    if rows_n < 20:
        warnings.append("Sample size is small (rows < 20). Recommendations are heuristic.")
    if len(numeric_cols) == 0:
        warnings.append("No numeric-like columns were detected.")
    if len(cat_cols) == 0:
        warnings.append("No categorical-like columns were detected.")

    return {
        "ok": True,
        "op": "recommend",
        "inputs": {"goal": goal_norm, "value": value, "group": group, "a": a, "b": b},
        "summary": {
            "title": "Statistical Test Recommender",
            "conclusion": conclusion,
            "stats": {
                "rows": rows_n,
                "cols": cols_n,
                "numeric_like_cols": int(len(numeric_cols)),
                "categorical_like_cols": int(len(cat_cols)),
                "recommendations": int(len(uniq)),
            },
        },
        "tables": [to_table(rec_tbl, "recommended_tests")],
        "recommendations": uniq,
        "warnings": warnings,
        "meta": {"rows": rows_n, "cols": cols_n},
    }

def main():
    t0 = time.time()
    raw = sys.stdin.read()
    op = "unknown"

    def err_payload(code, message, details=None):
        out = {
            "ok": False,
            "op": op,
            "code": str(code),
            "message": str(message),
            "meta": {"execMs": int((time.time() - t0) * 1000)},
        }
        if details is not None:
            out["details"] = details
        return out

    try:
        try:
            payload = json.loads(raw or "{}")
        except Exception as e:
            j(err_payload("INVALID_JSON", "invalid json payload", str(e)))
            return

        if not isinstance(payload, dict):
            j(err_payload("INVALID_PAYLOAD", "payload must be a JSON object", {"type": type(payload).__name__}))
            return

        op = payload.get("op", "describe")
        rows = payload.get("rows", [])
        options = payload.get("options", {}) or {}
        if not isinstance(options, dict):
            options = {}
        if not isinstance(rows, list):
            j(err_payload("INVALID_ROWS_TYPE", "rows must be an array", {"type": type(rows).__name__}))
            return

        df = pd.DataFrame(rows)

        if df.shape[0] == 0:
            j({"ok": True, "op": op, "summary": {"title":"empty","conclusion":"No data was provided.","stats":{}},
               "tables": [], "warnings": ["No data was provided."], "meta": {"rows":0,"cols":0, "execMs": int((time.time()-t0)*1000)}})
            return

        if op == "describe":
            topn = int(options.get("topNCat", 10))
            out = describe_report(df, topn_cat=topn)
        elif op == "corr":
            topn = int(options.get("topNPairs", 20))
            out = corr_report(df, topn_pairs=topn)
        elif op == "ttest":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = ttest_report(df, args.get("value"), args.get("group"))
        elif op == "chisq":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = chisq_report(df, args.get("a"), args.get("b"))
        elif op == "ols":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            opts = payload.get("options", {}) or {}
            if not isinstance(opts, dict):
                opts = {}
            out = ols_report(df, args.get("y"), args.get("x") or [], opts)
        elif op == "anova":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = anova_report(df, args.get("value"), args.get("group"))
        elif op == "normality":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = normality_report(df, args.get("column"))
        elif op == "ci_mean":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = ci_mean_report(df, args.get("column"), args.get("confidence", 0.95))
        elif op == "quality":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = quality_report(
                df,
                args.get("columns"),
                args.get("method", "iqr"),
                args.get("iqrK", 1.5),
                args.get("zThresh", 3.0),
            )
        elif op == "quality_process":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = quality_process_report(
                df,
                args.get("strategy", "exclude"),
                args.get("method", "iqr"),
                args.get("columns"),
                args.get("iqrK", 1.5),
                args.get("zThresh", 3.0),
                args.get("dropMissing"),
            )
        elif op == "mannwhitney":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = mannwhitney_report(df, args.get("value"), args.get("group"))
        elif op == "wilcoxon":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = wilcoxon_report(df, args.get("a"), args.get("b"))
        elif op == "kruskal":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = kruskal_report(df, args.get("value"), args.get("group"))
        elif op == "tukey":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = tukey_report(df, args.get("value"), args.get("group"), args.get("alpha", 0.05))
        elif op == "pairwise_adjusted":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = pairwise_adjusted_report(
                df,
                args.get("value"),
                args.get("group"),
                args.get("pAdjust", "holm"),
            )
        elif op == "recommend":
            args = payload.get("args", {}) or {}
            if not isinstance(args, dict):
                args = {}
            out = recommend_report(
                df,
                args.get("goal", "auto"),
                args.get("value"),
                args.get("group"),
                args.get("a"),
                args.get("b"),
            )
        else:
            out = {"ok": False, "op": op, "code": "UNSUPPORTED_OP", "message": "unsupported op in PASS1"}

        out.setdefault("meta", {})
        out["meta"]["execMs"] = int((time.time() - t0) * 1000)
        j(out)
    except Exception as e:
        j(err_payload("STAT_RUN_EXCEPTION", "unhandled exception in stat_run", str(e)))
def corr_report(df: pd.DataFrame, topn_pairs=20):
    num = df.select_dtypes(include=[np.number])
    cols = num.columns.tolist()

    if len(cols) < 2:
        return {
            "ok": True,
            "op": "corr",
            "summary": {"title":"Correlation Analysis", "conclusion":"At least two numeric columns are required.", "stats":{}},
            "tables": [],
            "warnings": ["Fewer than two numeric columns are available."],
            "meta": {"rows": int(df.shape[0]), "cols": int(df.shape[1])}
        }

    corr = num.corr(method="pearson").fillna(0.0)

    # TOP pairs
    pairs = []
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            v = float(corr.iloc[i, j])
            pairs.append([cols[i], cols[j], v, abs(v)])
    pairs.sort(key=lambda x: x[3], reverse=True)
    top = pairs[:topn_pairs]
    top_df = pd.DataFrame([[a,b,v] for a,b,v,_ in top], columns=["col_a","col_b","corr"])

    # Heatmap matrix payload used by GraphPanel.
    fig = {
        "type": "heatmap_matrix",
        "x": cols,
        "y": cols,
        "z": corr.values.tolist()
    }

    return {
        "ok": True,
        "op": "corr",
        "summary": {
            "title": "Correlation Analysis (Pearson)",
            "conclusion": "Review top absolute correlation pairs and inspect the heatmap for overall structure.",
            "stats": {"num_cols": len(cols)}
        },
        "tables": [
            to_table(corr.reset_index().rename(columns={"index":"column"}), "corr_matrix"),
            to_table(top_df, "top_pairs")
        ],
        "figures": [fig],
        "warnings": [],
        "meta": {"rows": int(df.shape[0]), "cols": int(df.shape[1])}
    }

def ttest_report(df: pd.DataFrame, value_col: str, group_col: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "ttest",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for ttest",
            "details": str(e),
        }

    if value_col not in df.columns or group_col not in df.columns:
        return {"ok":False,"op":"ttest","error":"invalid columns"}

    d = df[[value_col, group_col]].dropna()
    # Cast test value column to numeric.
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])

    groups = d[group_col].astype("string").unique().tolist()
    if len(groups) != 2:
        return {
            "ok": True, "op":"ttest",
            "summary":{"title":"t-test (Welch)","conclusion":"The group column must contain exactly 2 levels.","stats":{}},
            "tables": [],
            "warnings":[f"Group level count={len(groups)} (requires 2)"],
            "meta":{"n": int(d.shape[0])}
        }

    g1, g2 = groups[0], groups[1]
    a = d.loc[d[group_col].astype("string")==g1, value_col].to_numpy()
    b = d.loc[d[group_col].astype("string")==g2, value_col].to_numpy()

    # Minimum sample-size check.
    if len(a) < 2 or len(b) < 2:
        return {
            "ok": True, "op":"ttest",
            "summary":{"title":"t-test (Welch)","conclusion":"Insufficient sample size.","stats":{}},
            "tables": [],
            "warnings":[f"Insufficient sample size: n1={len(a)}, n2={len(b)} (each must be >= 2)"],
            "meta":{"n1": int(len(a)), "n2": int(len(b))}
        }

    # Welch t-test
    tstat, p = stats.ttest_ind(a, b, equal_var=False, nan_policy="omit")

    # Effect size: Cohen's d (simple pooled-SD version).
    mean1, mean2 = float(np.mean(a)), float(np.mean(b))
    sd1, sd2 = float(np.std(a, ddof=1)), float(np.std(b, ddof=1))
    pooled = np.sqrt((sd1**2 + sd2**2)/2) if (sd1>0 or sd2>0) else 0.0
    d_eff = (mean1-mean2)/pooled if pooled>0 else 0.0
    # Approximate normal CI for Cohen's d.
    n1 = int(len(a))
    n2 = int(len(b))
    if n1 > 1 and n2 > 1:
        se_d = np.sqrt(((n1 + n2) / (n1 * n2)) + ((d_eff ** 2) / (2.0 * max(n1 + n2 - 2, 1))))
        z = 1.959963984540054
        d_ci_low = float(d_eff - z * se_d)
        d_ci_high = float(d_eff + z * se_d)
    else:
        d_ci_low, d_ci_high = None, None

    table = pd.DataFrame([
        [g1, int(len(a)), mean1, sd1],
        [g2, int(len(b)), mean2, sd2],
    ], columns=["group","n","mean","std"])

    conclusion = "Statistically significant difference detected (p < 0.05)." if (p is not None and p < 0.05) else "No statistically significant difference (p >= 0.05)."

    return {
        "ok": True,
        "op": "ttest",
        "inputs": {"value": value_col, "group": group_col, "method": "welch"},
        "summary": {
            "title": "Two-Group Mean Comparison (Welch t-test)",
            "conclusion": conclusion,
            "stats": {
                "t": float(tstat),
                "p": float(p),
                "cohen_d": float(d_eff),
                "cohen_d_ci_low": d_ci_low,
                "cohen_d_ci_high": d_ci_high,
            }
        },
        "tables": [to_table(table, "group_summary")],
        "warnings": [],
        "meta": {"n1": int(len(a)), "n2": int(len(b))}
    }

def chisq_report(df: pd.DataFrame, a_col: str, b_col: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "chisq",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for chisq",
            "details": str(e),
        }

    if a_col not in df.columns or b_col not in df.columns:
        return {"ok":False,"op":"chisq","error":"invalid columns"}

    d = df[[a_col, b_col]].dropna()
    if d.shape[0] < 5:
        return {
            "ok": True, "op":"chisq",
            "summary":{"title":"Chi-square Test","conclusion":"Sample size is too small.","stats":{}},
            "tables": [],
            "warnings":[f"Insufficient sample size: n={int(d.shape[0])}"],
            "meta":{"n": int(d.shape[0])}
        }

    ct = pd.crosstab(d[a_col].astype("string"), d[b_col].astype("string"))
    chi2, p, dof, expected = stats.chi2_contingency(ct.values)

    # Cramer's V
    n = ct.values.sum()
    r, k = ct.shape
    v = np.sqrt((chi2 / n) / (min(r-1, k-1))) if n>0 and min(r-1,k-1)>0 else 0.0
    v_ci_low, v_ci_high = None, None
    if int(d.shape[0]) >= 20 and min(r - 1, k - 1) > 0:
        boots = []
        rng = np.random.default_rng(42)
        for _ in range(250):
            idx = rng.integers(0, int(d.shape[0]), int(d.shape[0]))
            sb = d.iloc[idx]
            ct_b = pd.crosstab(sb[a_col].astype("string"), sb[b_col].astype("string"))
            if ct_b.shape[0] < 2 or ct_b.shape[1] < 2:
                continue
            chi2_b, _, _, _ = stats.chi2_contingency(ct_b.values)
            n_b = ct_b.values.sum()
            denom = min(ct_b.shape[0] - 1, ct_b.shape[1] - 1)
            if n_b <= 0 or denom <= 0:
                continue
            v_b = np.sqrt((chi2_b / n_b) / denom)
            if np.isfinite(v_b):
                boots.append(float(v_b))
        v_ci_low, v_ci_high = _finite_percentile_ci(boots, alpha=0.05)

    # Expected frequencies table can be added later; omitted for MVP.
    ct_df = ct.reset_index().rename(columns={a_col:"row"})
    conclusion = "A statistically significant association exists (p < 0.05)." if p < 0.05 else "No statistically significant association (p >= 0.05)."

    return {
        "ok": True,
        "op": "chisq",
        "inputs": {"a": a_col, "b": b_col},
        "summary": {
            "title": "Categorical-Categorical Association (Chi-square Test)",
            "conclusion": conclusion,
            "stats": {
                "chi2": float(chi2),
                "p": float(p),
                "dof": int(dof),
                "cramers_v": float(v),
                "cramers_v_ci_low": v_ci_low,
                "cramers_v_ci_high": v_ci_high,
            }
        },
        "tables": [to_table(ct_df, "contingency_table")],
        "warnings": [],
        "meta": {"n": int(n)}
    }

def anova_report(df: pd.DataFrame, value_col: str, group_col: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "anova",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for anova",
            "details": str(e),
        }

    if value_col not in df.columns or group_col not in df.columns:
        return {"ok": False, "op": "anova", "error": "invalid columns"}

    d = df[[value_col, group_col]].dropna()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])

    if d.shape[0] < 6:
        return {
            "ok": True,
            "op": "anova",
            "summary": {"title": "One-way ANOVA", "conclusion": "Insufficient sample size.", "stats": {}},
            "tables": [],
            "warnings": [f"Insufficient sample size: n={int(d.shape[0])} (recommended >= 6)"],
            "meta": {"n": int(d.shape[0])},
        }

    grouped = []
    group_rows = []
    for g, sub in d.groupby(group_col, dropna=False):
        arr = sub[value_col].to_numpy(dtype=float)
        if arr.shape[0] < 2:
            continue
        grouped.append(arr)
        group_rows.append([str(g), int(arr.shape[0]), float(np.mean(arr)), float(np.std(arr, ddof=1)) if arr.shape[0] > 1 else 0.0])

    if len(grouped) < 2:
        return {
            "ok": True,
            "op": "anova",
            "summary": {"title": "One-way ANOVA", "conclusion": "At least two valid groups are required.", "stats": {}},
            "tables": [],
            "warnings": ["Need at least two groups with n >= 2."],
            "meta": {"n": int(d.shape[0])},
        }

    f_stat, p_val = stats.f_oneway(*grouped)
    grand_mean = float(d[value_col].mean())
    ss_between = sum(len(g) * ((float(np.mean(g)) - grand_mean) ** 2) for g in grouped)
    ss_within = sum(float(np.sum((g - np.mean(g)) ** 2)) for g in grouped)
    ss_total = ss_between + ss_within
    eta_sq = (ss_between / ss_total) if ss_total > 0 else 0.0
    eta_ci_low, eta_ci_high = None, None
    if int(d.shape[0]) >= 20:
        boots = []
        rng = np.random.default_rng(42)
        for _ in range(250):
            idx = rng.integers(0, int(d.shape[0]), int(d.shape[0]))
            sb = d.iloc[idx]
            grouped_b = []
            for _, sub_b in sb.groupby(group_col, dropna=False):
                arr_b = sub_b[value_col].to_numpy(dtype=float)
                if arr_b.shape[0] < 2:
                    continue
                grouped_b.append(arr_b)
            if len(grouped_b) < 2:
                continue
            gm_b = float(sb[value_col].mean())
            ss_b = sum(len(gb) * ((float(np.mean(gb)) - gm_b) ** 2) for gb in grouped_b)
            ss_w = sum(float(np.sum((gb - np.mean(gb)) ** 2)) for gb in grouped_b)
            ss_t = ss_b + ss_w
            if ss_t <= 0:
                continue
            e_b = ss_b / ss_t
            if np.isfinite(e_b):
                boots.append(float(e_b))
        eta_ci_low, eta_ci_high = _finite_percentile_ci(boots, alpha=0.05)

    anova_tbl = pd.DataFrame([
        ["between", float(ss_between), int(len(grouped) - 1), float(ss_between / max(len(grouped) - 1, 1))],
        ["within", float(ss_within), int(int(d.shape[0]) - len(grouped)), float(ss_within / max(int(d.shape[0]) - len(grouped), 1))],
    ], columns=["source", "ss", "df", "ms"])

    group_tbl = pd.DataFrame(group_rows, columns=["group", "n", "mean", "std"])
    conclusion = "Group means differ significantly (p < 0.05)." if float(p_val) < 0.05 else "No significant group mean difference (p >= 0.05)."

    return {
        "ok": True,
        "op": "anova",
        "inputs": {"value": value_col, "group": group_col},
        "summary": {
            "title": "One-way ANOVA",
            "conclusion": conclusion,
            "stats": {
                "f": float(f_stat),
                "p": float(p_val),
                "eta_sq": float(eta_sq),
                "eta_sq_ci_low": eta_ci_low,
                "eta_sq_ci_high": eta_ci_high,
                "groups": int(len(grouped)),
            }
        },
        "tables": [to_table(group_tbl, "group_summary"), to_table(anova_tbl, "anova_table")],
        "warnings": [],
        "meta": {"n": int(d.shape[0])},
    }

def normality_report(df: pd.DataFrame, column: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "normality",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for normality test",
            "details": str(e),
        }

    if column not in df.columns:
        return {"ok": False, "op": "normality", "error": "invalid column"}

    s = pd.to_numeric(df[column], errors="coerce").dropna()
    n = int(s.shape[0])
    if n < 8:
        return {
            "ok": True,
            "op": "normality",
            "summary": {"title": "Normality Test", "conclusion": "Insufficient sample size.", "stats": {"n": n}},
            "tables": [],
            "warnings": [f"Need at least 8 valid numeric values; got {n}."],
            "meta": {"n": n},
        }

    vals = s.to_numpy(dtype=float)
    jb_stat, jb_p = stats.jarque_bera(vals)
    sh_p = None
    if n <= 5000:
        try:
            _, sh_p = stats.shapiro(vals)
            sh_p = float(sh_p)
        except Exception:
            sh_p = None

    mean_v = float(np.mean(vals))
    std_v = float(np.std(vals, ddof=1)) if n > 1 else 0.0

    probs = np.linspace(0.01, 0.99, min(100, n))
    theo = stats.norm.ppf(probs, loc=mean_v, scale=std_v if std_v > 0 else 1.0)
    obs = np.quantile(vals, probs)

    hist_counts, hist_edges = np.histogram(vals, bins=min(30, max(8, int(np.sqrt(n)))))
    stat_tbl = pd.DataFrame([[
        n,
        mean_v,
        std_v,
        float(s.skew()) if n > 2 else 0.0,
        float(s.kurt()) if n > 3 else 0.0,
        float(jb_stat),
        float(jb_p),
        sh_p,
    ]], columns=["n", "mean", "std", "skewness", "kurtosis", "jb_stat", "jb_p", "shapiro_p"])

    conclusion = "Distribution appears non-normal (p < 0.05)." if float(jb_p) < 0.05 else "No strong evidence against normality (p >= 0.05)."

    return {
        "ok": True,
        "op": "normality",
        "inputs": {"column": column},
        "summary": {
            "title": "Normality Check (Jarque-Bera / Shapiro)",
            "conclusion": conclusion,
            "stats": {"n": n, "jb_p": float(jb_p), "shapiro_p": sh_p},
        },
        "tables": [to_table(stat_tbl, "normality_stats")],
        "figures": [
            {"type": "qq_plot", "x": theo.tolist(), "y": obs.tolist()},
            {"type": "histogram", "bins": hist_edges.tolist(), "counts": hist_counts.astype(int).tolist()},
        ],
        "warnings": [],
        "meta": {"n": n},
    }

def ci_mean_report(df: pd.DataFrame, column: str, confidence: float):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "ci_mean",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for confidence interval",
            "details": str(e),
        }

    if column not in df.columns:
        return {"ok": False, "op": "ci_mean", "error": "invalid column"}

    conf = float(confidence if confidence is not None else 0.95)
    conf = min(max(conf, 0.5), 0.999)

    s = pd.to_numeric(df[column], errors="coerce").dropna()
    n = int(s.shape[0])
    if n < 2:
        return {
            "ok": True,
            "op": "ci_mean",
            "summary": {"title": "Mean Confidence Interval", "conclusion": "Insufficient sample size.", "stats": {"n": n}},
            "tables": [],
            "warnings": [f"Need at least 2 valid numeric values; got {n}."],
            "meta": {"n": n},
        }

    vals = s.to_numpy(dtype=float)
    mean_v = float(np.mean(vals))
    std_v = float(np.std(vals, ddof=1))
    se = std_v / np.sqrt(n) if n > 0 else 0.0
    alpha = 1.0 - conf
    tcrit = float(stats.t.ppf(1 - alpha / 2, df=n - 1))
    margin = tcrit * se
    lo = mean_v - margin
    hi = mean_v + margin

    ci_tbl = pd.DataFrame([[
        n, mean_v, std_v, se, conf, lo, hi
    ]], columns=["n", "mean", "std", "se", "confidence", "ci_low", "ci_high"])

    return {
        "ok": True,
        "op": "ci_mean",
        "inputs": {"column": column, "confidence": conf},
        "summary": {
            "title": "Mean Confidence Interval",
            "conclusion": f"{int(conf*100)}% CI estimated for population mean.",
            "stats": {"n": n, "mean": mean_v, "ci_low": lo, "ci_high": hi},
        },
        "tables": [to_table(ci_tbl, "mean_ci")],
        "warnings": [],
        "meta": {"n": n},
    }

def mannwhitney_report(df: pd.DataFrame, value_col: str, group_col: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "mannwhitney",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for mannwhitney",
            "details": str(e),
        }

    if value_col not in df.columns or group_col not in df.columns:
        return {"ok": False, "op": "mannwhitney", "error": "invalid columns"}

    d = df[[value_col, group_col]].dropna()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])

    groups = d[group_col].astype("string").unique().tolist()
    if len(groups) != 2:
        return {
            "ok": True,
            "op": "mannwhitney",
            "summary": {"title": "Mann-Whitney U Test", "conclusion": "Group column must have exactly two levels.", "stats": {}},
            "tables": [],
            "warnings": [f"Group level count={len(groups)} (requires 2)"],
            "meta": {"n": int(d.shape[0])},
        }

    g1, g2 = groups[0], groups[1]
    a = d.loc[d[group_col].astype("string") == g1, value_col].to_numpy(dtype=float)
    b = d.loc[d[group_col].astype("string") == g2, value_col].to_numpy(dtype=float)
    if len(a) < 2 or len(b) < 2:
        return {
            "ok": True,
            "op": "mannwhitney",
            "summary": {"title": "Mann-Whitney U Test", "conclusion": "Insufficient sample size.", "stats": {}},
            "tables": [],
            "warnings": [f"Insufficient sample size: n1={len(a)}, n2={len(b)} (each must be >= 2)"],
            "meta": {"n1": int(len(a)), "n2": int(len(b))},
        }

    u_stat, p_val = stats.mannwhitneyu(a, b, alternative="two-sided")
    cles = float(u_stat) / float(len(a) * len(b))
    grp_tbl = pd.DataFrame([
        [g1, int(len(a)), float(np.mean(a)), float(np.median(a))],
        [g2, int(len(b)), float(np.mean(b)), float(np.median(b))],
    ], columns=["group", "n", "mean", "median"])

    conclusion = "Two groups differ significantly (p < 0.05)." if float(p_val) < 0.05 else "No significant difference (p >= 0.05)."
    return {
        "ok": True,
        "op": "mannwhitney",
        "inputs": {"value": value_col, "group": group_col},
        "summary": {
            "title": "Mann-Whitney U Test",
            "conclusion": conclusion,
            "stats": {"u": float(u_stat), "p": float(p_val), "cles": float(cles)},
        },
        "tables": [to_table(grp_tbl, "group_summary")],
        "warnings": [],
        "meta": {"n1": int(len(a)), "n2": int(len(b))},
    }

def wilcoxon_report(df: pd.DataFrame, a_col: str, b_col: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "wilcoxon",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for wilcoxon",
            "details": str(e),
        }

    if a_col not in df.columns or b_col not in df.columns:
        return {"ok": False, "op": "wilcoxon", "error": "invalid columns"}

    d = df[[a_col, b_col]].dropna()
    d[a_col] = pd.to_numeric(d[a_col], errors="coerce")
    d[b_col] = pd.to_numeric(d[b_col], errors="coerce")
    d = d.dropna(subset=[a_col, b_col])

    if d.shape[0] < 5:
        return {
            "ok": True,
            "op": "wilcoxon",
            "summary": {"title": "Wilcoxon Signed-Rank Test", "conclusion": "Insufficient paired sample size.", "stats": {}},
            "tables": [],
            "warnings": [f"Need at least 5 paired observations; got {int(d.shape[0])}."],
            "meta": {"n": int(d.shape[0])},
        }

    a = d[a_col].to_numpy(dtype=float)
    b = d[b_col].to_numpy(dtype=float)
    diff = a - b
    if np.allclose(diff, 0):
        return {
            "ok": True,
            "op": "wilcoxon",
            "summary": {"title": "Wilcoxon Signed-Rank Test", "conclusion": "All paired differences are zero.", "stats": {"n": int(d.shape[0])}},
            "tables": [],
            "warnings": ["All paired differences are zero."],
            "meta": {"n": int(d.shape[0])},
        }

    w_stat, p_val = stats.wilcoxon(a, b, zero_method="wilcox", alternative="two-sided")
    n_pairs = int(d.shape[0])
    expected_w = n_pairs * (n_pairs + 1) / 4.0
    var_w = n_pairs * (n_pairs + 1) * (2 * n_pairs + 1) / 24.0
    if var_w > 0:
        z_approx = (float(w_stat) - expected_w) / np.sqrt(var_w)
        r_approx = abs(float(z_approx)) / np.sqrt(n_pairs)
    else:
        r_approx = 0.0
    pair_tbl = pd.DataFrame([[
        int(d.shape[0]),
        float(np.mean(a)),
        float(np.mean(b)),
        float(np.median(diff)),
        float(np.mean(diff)),
    ]], columns=["n", "mean_a", "mean_b", "median_diff", "mean_diff"])

    conclusion = "Paired distributions differ significantly (p < 0.05)." if float(p_val) < 0.05 else "No significant paired difference (p >= 0.05)."
    pair_fig = {
        "type": "paired_points",
        "x": a.tolist(),
        "y": b.tolist(),
    }
    return {
        "ok": True,
        "op": "wilcoxon",
        "inputs": {"a": a_col, "b": b_col},
        "summary": {
            "title": "Wilcoxon Signed-Rank Test",
            "conclusion": conclusion,
            "stats": {"w": float(w_stat), "p": float(p_val), "n": int(d.shape[0]), "r_approx": float(r_approx)},
        },
        "tables": [to_table(pair_tbl, "pair_summary")],
        "figures": [pair_fig],
        "warnings": [],
        "meta": {"n": int(d.shape[0])},
    }

def kruskal_report(df: pd.DataFrame, value_col: str, group_col: str):
    try:
        from scipy import stats
    except Exception as e:
        return {
            "ok": False,
            "op": "kruskal",
            "code": "SCIPY_NOT_INSTALLED",
            "message": "scipy is required for kruskal",
            "details": str(e),
        }

    if value_col not in df.columns or group_col not in df.columns:
        return {"ok": False, "op": "kruskal", "error": "invalid columns"}

    d = df[[value_col, group_col]].dropna()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])

    grouped = []
    group_rows = []
    for g, sub in d.groupby(group_col, dropna=False):
        arr = sub[value_col].to_numpy(dtype=float)
        if arr.shape[0] < 2:
            continue
        grouped.append(arr)
        group_rows.append([str(g), int(arr.shape[0]), float(np.mean(arr)), float(np.median(arr))])

    if len(grouped) < 2:
        return {
            "ok": True,
            "op": "kruskal",
            "summary": {"title": "Kruskal-Wallis H Test", "conclusion": "Need at least two groups with n >= 2.", "stats": {}},
            "tables": [],
            "warnings": ["Need at least two groups with n >= 2."],
            "meta": {"n": int(d.shape[0])},
        }

    h_stat, p_val = stats.kruskal(*grouped)
    n = int(sum(len(g) for g in grouped))
    k = int(len(grouped))
    eps_sq = (float(h_stat) - k + 1.0) / (n - k) if (n - k) > 0 else 0.0
    eps_sq = float(max(eps_sq, 0.0))

    grp_tbl = pd.DataFrame(group_rows, columns=["group", "n", "mean", "median"])
    conclusion = "At least one group differs significantly (p < 0.05)." if float(p_val) < 0.05 else "No significant group distribution difference (p >= 0.05)."

    return {
        "ok": True,
        "op": "kruskal",
        "inputs": {"value": value_col, "group": group_col},
        "summary": {
            "title": "Kruskal-Wallis H Test",
            "conclusion": conclusion,
            "stats": {"h": float(h_stat), "p": float(p_val), "epsilon_sq": eps_sq, "groups": k},
        },
        "tables": [to_table(grp_tbl, "group_summary")],
        "warnings": [],
        "meta": {"n": n, "groups": k},
    }

def tukey_report(df: pd.DataFrame, value_col: str, group_col: str, alpha=0.05):
    try:
        from statsmodels.stats.multicomp import pairwise_tukeyhsd
    except Exception as e:
        return {
            "ok": False,
            "op": "tukey",
            "code": "STATSMODELS_NOT_INSTALLED",
            "message": "statsmodels is required for tukey",
            "details": str(e),
        }

    if value_col not in df.columns or group_col not in df.columns:
        return {"ok": False, "op": "tukey", "error": "invalid columns"}

    d = df[[value_col, group_col]].dropna()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])
    if d.shape[0] < 6:
        return {
            "ok": True,
            "op": "tukey",
            "summary": {"title": "Tukey HSD", "conclusion": "Insufficient sample size.", "stats": {}},
            "tables": [],
            "warnings": [f"Need more rows for post-hoc testing; got n={int(d.shape[0])}."],
            "meta": {"n": int(d.shape[0])},
        }

    counts = d[group_col].astype("string").value_counts()
    valid_groups = counts[counts >= 2].index.tolist()
    d = d[d[group_col].astype("string").isin(valid_groups)]
    if len(valid_groups) < 2:
        return {
            "ok": True,
            "op": "tukey",
            "summary": {"title": "Tukey HSD", "conclusion": "Need at least two groups with n >= 2.", "stats": {}},
            "tables": [],
            "warnings": ["Need at least two valid groups (n >= 2)."],
            "meta": {"n": int(d.shape[0])},
        }

    alpha = float(alpha if alpha is not None else 0.05)
    alpha = min(max(alpha, 0.001), 0.2)

    tukey = pairwise_tukeyhsd(
        endog=d[value_col].to_numpy(dtype=float),
        groups=d[group_col].astype("string").to_numpy(),
        alpha=alpha,
    )
    rows = tukey.summary().data[1:]
    tbl = pd.DataFrame(rows, columns=["group1", "group2", "mean_diff", "p_adj", "ci_low", "ci_high", "reject"])

    # ensure numeric types for charting and downstream use
    for c in ["mean_diff", "p_adj", "ci_low", "ci_high"]:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce")
    tbl["reject"] = tbl["reject"].astype(str).map(lambda s: str(s).lower() in ("true", "1", "yes"))
    tbl["abs_mean_diff"] = tbl["mean_diff"].abs()
    tbl["pair"] = tbl["group1"].astype(str) + " vs " + tbl["group2"].astype(str)

    pair_fig = {
        "type": "posthoc_pairs",
        "x": tbl["pair"].tolist(),
        "y": tbl["abs_mean_diff"].fillna(0.0).tolist(),
        "p": tbl["p_adj"].fillna(1.0).tolist(),
        "reject": tbl["reject"].tolist(),
    }
    sig_n = int(tbl["reject"].sum())
    conclusion = f"Tukey HSD completed. Significant pairs: {sig_n}/{int(tbl.shape[0])} (alpha={alpha})."

    return {
        "ok": True,
        "op": "tukey",
        "inputs": {"value": value_col, "group": group_col, "alpha": alpha},
        "summary": {"title": "Tukey HSD Post-hoc", "conclusion": conclusion, "stats": {"pairs": int(tbl.shape[0]), "significant_pairs": sig_n}},
        "tables": [to_table(tbl, "tukey_pairs")],
        "figures": [pair_fig],
        "warnings": [],
        "meta": {"n": int(d.shape[0]), "groups": int(len(valid_groups))},
    }

def pairwise_adjusted_report(df: pd.DataFrame, value_col: str, group_col: str, p_adjust="holm"):
    try:
        from scipy import stats
        from statsmodels.stats.multitest import multipletests
    except Exception as e:
        return {
            "ok": False,
            "op": "pairwise_adjusted",
            "code": "SCIPY_OR_STATSMODELS_MISSING",
            "message": "scipy and statsmodels are required for pairwise adjusted tests",
            "details": str(e),
        }

    if value_col not in df.columns or group_col not in df.columns:
        return {"ok": False, "op": "pairwise_adjusted", "error": "invalid columns"}

    d = df[[value_col, group_col]].dropna()
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])

    counts = d[group_col].astype("string").value_counts()
    valid_groups = counts[counts >= 2].index.tolist()
    d = d[d[group_col].astype("string").isin(valid_groups)]
    if len(valid_groups) < 2:
        return {
            "ok": True,
            "op": "pairwise_adjusted",
            "summary": {"title": "Pairwise Adjusted Comparisons", "conclusion": "Need at least two groups with n >= 2.", "stats": {}},
            "tables": [],
            "warnings": ["Need at least two valid groups (n >= 2)."],
            "meta": {"n": int(d.shape[0])},
        }

    method_map = {
        "bonferroni": "bonferroni",
        "holm": "holm",
        "fdr": "fdr_bh",
        "fdr_bh": "fdr_bh",
        "bh": "fdr_bh",
    }
    method = method_map.get(str(p_adjust or "holm").lower(), "holm")

    groups = sorted(valid_groups)
    rows = []
    pvals = []
    for i in range(len(groups)):
        for j in range(i + 1, len(groups)):
            g1 = groups[i]
            g2 = groups[j]
            a = d.loc[d[group_col].astype("string") == g1, value_col].to_numpy(dtype=float)
            b = d.loc[d[group_col].astype("string") == g2, value_col].to_numpy(dtype=float)
            if len(a) < 2 or len(b) < 2:
                continue
            t_stat, p_val = stats.ttest_ind(a, b, equal_var=False, nan_policy="omit")
            mean_diff = float(np.mean(a) - np.mean(b))
            rows.append([g1, g2, int(len(a)), int(len(b)), mean_diff, float(t_stat), float(p_val)])
            pvals.append(float(p_val))

    if not rows:
        return {
            "ok": True,
            "op": "pairwise_adjusted",
            "summary": {"title": "Pairwise Adjusted Comparisons", "conclusion": "No valid pairwise comparisons.", "stats": {}},
            "tables": [],
            "warnings": ["No valid pairwise group comparisons were available."],
            "meta": {"n": int(d.shape[0]), "groups": int(len(valid_groups))},
        }

    reject, p_adj, _, _ = multipletests(pvals, alpha=0.05, method=method)
    tbl = pd.DataFrame(rows, columns=["group1", "group2", "n1", "n2", "mean_diff", "t", "p_raw"])
    tbl["p_adj"] = [float(v) for v in p_adj]
    tbl["reject"] = [bool(v) for v in reject]
    tbl["abs_mean_diff"] = tbl["mean_diff"].abs()
    tbl["pair"] = tbl["group1"].astype(str) + " vs " + tbl["group2"].astype(str)
    tbl = tbl.sort_values(["p_adj", "p_raw"]).reset_index(drop=True)

    fig = {
        "type": "pairwise_adjusted_pairs",
        "x": tbl["pair"].tolist(),
        "y": tbl["abs_mean_diff"].fillna(0.0).tolist(),
        "p": tbl["p_adj"].fillna(1.0).tolist(),
        "reject": tbl["reject"].tolist(),
    }
    sig_n = int(tbl["reject"].sum())
    conclusion = f"Pairwise Welch comparisons with {method} correction completed. Significant pairs: {sig_n}/{int(tbl.shape[0])}."

    return {
        "ok": True,
        "op": "pairwise_adjusted",
        "inputs": {"value": value_col, "group": group_col, "pAdjust": method},
        "summary": {"title": "Pairwise Comparisons (Adjusted)", "conclusion": conclusion, "stats": {"pairs": int(tbl.shape[0]), "significant_pairs": sig_n}},
        "tables": [to_table(tbl, "pairwise_adjusted")],
        "figures": [fig],
        "warnings": [],
        "meta": {"n": int(d.shape[0]), "groups": int(len(valid_groups))},
    }
def ols_report(df: pd.DataFrame, y_col: str, x_cols: list, options: dict):
    try:
        import statsmodels.api as sm
    except Exception as e:
        return {
            "ok": False,
            "op": "ols",
            "code": "STATSMODELS_NOT_INSTALLED",
            "message": "statsmodels is required for ols",
            "details": str(e),
        }

    if y_col not in df.columns:
        return {"ok": False, "op": "ols", "error": "invalid y"}

    if not x_cols or any(c not in df.columns for c in x_cols):
        return {
            "ok": True, "op": "ols",
            "summary": {"title":"OLS Regression", "conclusion":"Select at least one predictor (X).", "stats":{}},
            "tables": [],
            "warnings": ["X columns are empty or invalid."],
            "meta": {"n": int(df.shape[0])}
        }

    add_intercept = bool(options.get("addIntercept", True))
    dummy = bool(options.get("dummy", True))
    drop_first = bool(options.get("dropFirst", True))
    robust = options.get("robust", None)  # e.g. "HC3" or None

    use_cols = [y_col] + list(dict.fromkeys(x_cols))
    d = df[use_cols].copy()

    # Force y to numeric.
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce")

    # Process X with mixed numeric/categorical handling.
    X = d[x_cols].copy()
    for c in x_cols:
        if pd.api.types.is_numeric_dtype(X[c]):
            X[c] = pd.to_numeric(X[c], errors="coerce")
        else:
            X[c] = X[c].astype("string")

    # Drop rows with missing values.
    before = int(d.shape[0])
    dd = pd.concat([d[[y_col]], X], axis=1).dropna()
    dropped = before - int(dd.shape[0])

    if dd.shape[0] < max(20, len(x_cols)*5):
        # Small-sample warning (still allow model fitting).
        warn_small = f"Sample size is small (n={int(dd.shape[0])}). Use results for reference only."
    else:
        warn_small = None

    y = dd[y_col].astype(float)

    X2 = dd[x_cols].copy()
    if dummy:
        # Dummy-encode categorical predictors.
        cat_cols = [c for c in x_cols if not pd.api.types.is_numeric_dtype(X2[c])]
        if cat_cols:
            X2 = pd.get_dummies(X2, columns=cat_cols, drop_first=drop_first)

    # Add intercept.
    if add_intercept:
        X2 = sm.add_constant(X2, has_constant="add")

    # Remove constant (zero-variance) columns.
    nunique = X2.nunique(dropna=False)
    bad_cols = [
        c for c in nunique[nunique <= 1].index.tolist()
        if str(c).lower() not in ("const", "intercept")
    ]
    if bad_cols:
        X2 = X2.drop(columns=bad_cols, errors="ignore")

    # Empty design-matrix check.
    if X2.shape[1] == 0:
        return {
            "ok": True, "op":"ols",
            "summary":{"title":"OLS Regression","conclusion":"No valid predictors remain after constant/missing/dummy processing.","stats":{}},
            "tables": [],
            "warnings":["No valid X columns remain."],
            "meta":{"n": int(dd.shape[0]), "droppedNA": dropped}
        }

    # Fit model.
    model = sm.OLS(y, X2).fit()

    # Optional robust covariance adjustment.
    if robust:
        try:
            model = model.get_robustcov_results(cov_type=robust)
        except Exception:
            pass

    # coefficients (normalize ndarray/Series shapes)
    raw_params = np.asarray(model.params)
    term_names = list(getattr(getattr(model, "model", None), "exog_names", []) or [])
    if len(term_names) != len(raw_params):
        term_names = [f"x{i}" for i in range(len(raw_params))]

    params = pd.Series(raw_params, index=term_names)
    bse = pd.Series(np.asarray(model.bse), index=params.index)
    tvals = pd.Series(np.asarray(model.tvalues), index=params.index)
    pvals = pd.Series(np.asarray(model.pvalues), index=params.index)

    # CI
    try:
        raw_ci = model.conf_int()
        if isinstance(raw_ci, np.ndarray):
            ci = pd.DataFrame(raw_ci, columns=["ci_low", "ci_high"], index=params.index)
        else:
            ci = pd.DataFrame(raw_ci).iloc[:, :2]
            ci.columns = ["ci_low", "ci_high"]
            ci.index = params.index
    except Exception:
        ci = pd.DataFrame({"ci_low":[None]*len(params), "ci_high":[None]*len(params)}, index=params.index)

    coef_df = pd.DataFrame({
        "term": params.index.astype(str),
        "coef": params.astype(float).values,
        "std_err": bse.astype(float).values,
        "t": tvals.astype(float).values,
        "p": pvals.astype(float).values,
        "ci_low": ci["ci_low"].astype(float).values,
        "ci_high": ci["ci_high"].astype(float).values
    })

    # Diagnostic figures for interactive charting.
    fitted = np.asarray(model.fittedvalues, dtype=float)
    resid = np.asarray(model.resid, dtype=float)
    diag_warning = None

    try:
        influence = model.get_influence()
        std_resid = np.asarray(influence.resid_studentized_internal, dtype=float)
        cooks_d = np.asarray(influence.cooks_distance[0], dtype=float)
    except Exception:
        std = np.std(resid, ddof=1) if resid.shape[0] > 1 else 0.0
        std_resid = (resid / std) if std > 0 else np.zeros_like(resid)
        cooks_d = np.zeros_like(resid)
        diag_warning = "Some regression diagnostics could not be computed via influence API."

    def compact_xy(xv, yv, max_points=1200):
        x = np.asarray(xv, dtype=float)
        y = np.asarray(yv, dtype=float)
        mask = np.isfinite(x) & np.isfinite(y)
        x = x[mask]
        y = y[mask]
        n = x.shape[0]
        if n > max_points:
            idx = np.linspace(0, n - 1, max_points).astype(int)
            x = x[idx]
            y = y[idx]
        return x, y

    rf_x, rf_y = compact_xy(fitted, resid)

    std_resid_clean = std_resid[np.isfinite(std_resid)]
    qq_x = np.array([])
    qq_y = np.array([])
    if std_resid_clean.shape[0] >= 5:
        try:
            pp = sm.ProbPlot(std_resid_clean)
            qq_x = np.asarray(pp.theoretical_quantiles, dtype=float)
            qq_y = np.asarray(pp.sample_quantiles, dtype=float)
            qq_x, qq_y = compact_xy(qq_x, qq_y)
        except Exception:
            pass

    sl_x, sl_y = compact_xy(fitted, np.sqrt(np.abs(std_resid)))
    cd_x, cd_y = compact_xy(np.arange(cooks_d.shape[0]), cooks_d)

    bp_lm = None
    bp_lm_p = None
    bp_f = None
    bp_f_p = None
    try:
        from statsmodels.stats.diagnostic import het_breuschpagan
        bp_lm, bp_lm_p, bp_f, bp_f_p = het_breuschpagan(resid, np.asarray(model.model.exog, dtype=float))
        bp_lm = float(bp_lm)
        bp_lm_p = float(bp_lm_p)
        bp_f = float(bp_f)
        bp_f_p = float(bp_f_p)
    except Exception:
        pass

    figures = [
        {"type": "residual_fitted", "x": rf_x.tolist(), "y": rf_y.tolist()},
        {"type": "scale_location", "x": sl_x.tolist(), "y": sl_y.tolist()},
        {"type": "cooks_distance", "x": cd_x.tolist(), "y": cd_y.tolist()},
    ]
    if qq_x.shape[0] and qq_y.shape[0]:
        figures.append({"type": "residual_qq", "x": qq_x.tolist(), "y": qq_y.tolist()})

    # Model summary metrics.
    stats_obj = {
        "nobs": int(model.nobs),
        "df_model": float(getattr(model, "df_model", None)) if getattr(model, "df_model", None) is not None else None,
        "df_resid": float(getattr(model, "df_resid", None)) if getattr(model, "df_resid", None) is not None else None,
        "r2": float(getattr(model, "rsquared", None)) if getattr(model, "rsquared", None) is not None else None,
        "adj_r2": float(getattr(model, "rsquared_adj", None)) if getattr(model, "rsquared_adj", None) is not None else None,
        "aic": float(getattr(model, "aic", None)) if getattr(model, "aic", None) is not None else None,
        "bic": float(getattr(model, "bic", None)) if getattr(model, "bic", None) is not None else None,
        "f": float(getattr(model, "fvalue", None)) if getattr(model, "fvalue", None) is not None else None,
        "f_p": float(getattr(model, "f_pvalue", None)) if getattr(model, "f_pvalue", None) is not None else None,
        "bp_lm": bp_lm,
        "bp_lm_p": bp_lm_p,
        "bp_f": bp_f,
        "bp_f_p": bp_f_p,
        "robust": robust
    }

    warnings = []
    if dropped > 0:
        warnings.append(f"Excluded {dropped} rows due to missing values.")
    if warn_small:
        warnings.append(warn_small)
    if bad_cols:
        warnings.append(f"Removed constant (no variance) columns: {', ' .join(bad_cols[:8])}" + ("..." if len(bad_cols)>8 else ""))
    if diag_warning:
        warnings.append(diag_warning)
    if bp_lm_p is not None and bp_lm_p < 0.05:
        warnings.append("Breusch-Pagan suggests heteroskedasticity (p < 0.05). Prefer robust covariance.")

    conclusion = "Model fit completed. Review coefficients (p-values) together with R-squared."

    return {
        "ok": True,
        "op": "ols",
        "inputs": {
            "y": y_col,
            "x": x_cols,
            "options": {"addIntercept": add_intercept, "dummy": dummy, "dropFirst": drop_first, "robust": robust}
        },
        "summary": {"title":"OLS Linear Regression", "conclusion": conclusion, "stats": stats_obj},
        "tables": [to_table(coef_df, "coef_table")],
        "figures": figures,
        "warnings": warnings,
        "meta": {"n": int(dd.shape[0]), "droppedNA": dropped, "xColsUsed": int(X2.shape[1])}
    }


if __name__ == "__main__":
    main()
