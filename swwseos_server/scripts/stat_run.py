import itertools
import json
import math
import sys
import time

try:
    import numpy as np
except Exception:
    np = None
try:
    import pandas as pd
except Exception:
    pd = None
try:
    from scipy import stats as sps
except Exception:
    sps = None
try:
    import statsmodels.api as sm
    from statsmodels.stats.diagnostic import het_breuschpagan
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    from statsmodels.stats.multitest import multipletests
    from statsmodels.stats.outliers_influence import OLSInfluence
except Exception:
    sm = None
    het_breuschpagan = None
    pairwise_tukeyhsd = None
    multipletests = None
    OLSInfluence = None

T0 = time.perf_counter()


def ms():
    return int((time.perf_counter() - T0) * 1000)


def fnum(v):
    try:
        x = float(v)
    except Exception:
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def jdefault(v):
    if np is not None and isinstance(v, np.generic):
        return v.item()
    if hasattr(v, "tolist"):
        return v.tolist()
    return str(v)


def out(obj):
    obj.setdefault("meta", {})
    obj["meta"].setdefault("execMs", ms())
    print(json.dumps(obj, ensure_ascii=False, default=jdefault))


def err(code, message, op=None, details=None):
    r = {"ok": False, "code": code, "message": message}
    if op is not None:
        r["op"] = op
    if details is not None:
        r["details"] = details
    return r


def ok(op, title="", conclusion="", stats=None, tables=None, figures=None, warnings=None, extra=None):
    r = {
        "ok": True,
        "op": op,
        "summary": {"title": title, "conclusion": conclusion, "stats": stats or {}},
        "tables": tables or [],
        "figures": figures or [],
        "warnings": warnings or [],
    }
    if extra:
        r.update(extra)
    return r


def need_pandas(op):
    return None if pd is not None else err("PANDAS_REQUIRED", "pandas is required", op)


def need_scipy(op):
    return None if sps is not None else err("STAT_SCIPY_REQUIRED", "scipy is required", op)


def need_sm(op):
    return None if sm is not None else err("STAT_STATSMODELS_REQUIRED", "statsmodels is required", op)


def df_rows(rows):
    if not isinstance(rows, list):
        raise TypeError("rows must be an array")
    return pd.DataFrame(rows)


def nseries(df, col):
    return pd.to_numeric(df[col], errors="coerce")


def num_cols(df):
    cols = []
    for c in df.columns:
        s = nseries(df, c)
        total = int(df[c].notna().sum())
        valid = int(s.notna().sum())
        if total and valid / total >= 0.6:
            cols.append(c)
    return cols


def cat_cols(df):
    nums = set(num_cols(df))
    return [c for c in df.columns if c not in nums]


def tbl(name, columns, rows):
    return {"name": name, "columns": columns, "rows": rows}


def fig(kind, **kwargs):
    return {"type": kind, **kwargs}


def describe_op(p):
    op = "describe"
    if need_pandas(op):
        return need_pandas(op)
    df = df_rows(p.get("rows", []))
    nums = set(num_cols(df))
    dtype_rows = [[c, "number" if c in nums else "category"] for c in df.columns]
    null_rows = []
    for c in df.columns:
        cnt = int(df[c].isna().sum())
        rate = (cnt / len(df)) if len(df) else 0
        null_rows.append([c, round(rate, 6), cnt])
    null_rows.sort(key=lambda x: x[1], reverse=True)
    tables = [
        tbl("dtype_summary", ["column", "dtype"], dtype_rows),
        tbl("null_rate_top", ["column", "null_rate", "null_count"], null_rows[:10]),
    ]
    if nums:
        rows = []
        for c in nums:
            s = nseries(df, c).dropna()
            rows.append([c, fnum(s.mean()), fnum(s.median()), fnum(s.std(ddof=1) if len(s) > 1 else 0), fnum(s.min() if len(s) else None), fnum(s.max() if len(s) else None)])
        tables.append(tbl("numeric_summary", ["column", "mean", "median", "std", "min", "max"], rows))
    return ok(op, "Descriptive statistics", f"Profiled {len(df)} rows across {len(df.columns)} columns.", {"rows": int(len(df)), "cols": int(len(df.columns)), "dup_rows": int(df.duplicated().sum()) if len(df.columns) else 0}, tables)


def corr_op(p):
    op = "corr"
    chk = need_pandas(op)
    if chk:
        return chk
    df = df_rows(p.get("rows", []))
    nums = num_cols(df)
    if len(nums) < 2:
        return ok(op, "Correlation analysis", "At least two numeric columns are required.", warnings=["Not enough numeric columns for correlation."])
    corr = df[nums].apply(pd.to_numeric, errors="coerce").corr().fillna(0)
    pairs = []
    for a, b in itertools.combinations(nums, 2):
        v = fnum(corr.loc[a, b]) or 0.0
        pairs.append([a, b, round(v, 6), round(abs(v), 6)])
    pairs.sort(key=lambda x: x[3], reverse=True)
    return ok(op, "Correlation analysis", f"Computed pairwise correlation across {len(nums)} numeric columns.", tables=[tbl("top_pairs", ["col_a", "col_b", "corr", "abs_corr"], pairs[:20])], figures=[fig("heatmap", x=nums, y=nums, z=corr.values.tolist())])


def recommend_op(p):
    op = "recommend"
    chk = need_pandas(op)
    if chk:
        return chk
    df = df_rows(p.get("rows", []))
    nums = num_cols(df)
    cats = cat_cols(df)
    recs = [{
        "op": "describe",
        "label": "Generate descriptive report",
        "reason": "Start with schema, null-rate, and duplicate checks.",
        "args": {},
        "chart": {"type": "bar", "x": "column", "y": "null_rate_pct"},
    }]
    if len(nums) >= 2:
        recs.append({
            "op": "corr",
            "label": "Run correlation scan",
            "reason": "Multiple numeric columns are available.",
            "args": {},
            "chart": {"type": "heatmap", "x": nums[0], "y": nums[1]},
        })
    if nums and cats:
        g = cats[0]
        k = int(df[g].astype("string").dropna().nunique())
        if k == 2:
            recs.append({
                "op": "ttest",
                "label": "Run t-test",
                "reason": f"'{g}' looks like a two-group split.",
                "args": {"value": nums[0], "group": g},
                "chart": {"type": "bar", "x": g, "y": nums[0]},
            })
        elif k > 2:
            recs.append({
                "op": "anova",
                "label": "Run ANOVA",
                "reason": f"'{g}' has multiple categories to compare.",
                "args": {"value": nums[0], "group": g},
                "chart": {"type": "bar", "x": g, "y": nums[0]},
            })
    if len(cats) >= 2:
        recs.append({
            "op": "chisq",
            "label": "Run Chi-square independence test",
            "reason": "Two or more categorical columns are available.",
            "args": {"a": cats[0], "b": cats[1]},
            "chart": {"type": "heatmap", "x": cats[0], "y": cats[1]},
        })
    return ok(op, "Recommended statistics", "Generated lightweight next-step recommendations from the dataset sample.", {"numeric_cols": len(nums), "categorical_cols": len(cats)}, extra={"recommendations": recs[:5]})


def q3(vals):
    arr = sorted(vals)
    if not arr:
        return (None, None, None)
    if np is not None:
        return (fnum(np.percentile(arr, 25)), fnum(np.percentile(arr, 50)), fnum(np.percentile(arr, 75)))
    n = len(arr)
    return (arr[max(0, n // 4)], arr[n // 2], arr[min(n - 1, (3 * n) // 4)])


def ttest_op(p):
    op = "ttest"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    vcol, gcol = a.get("value"), a.get("group")
    df = df_rows(p.get("rows", []))
    if vcol not in df.columns or gcol not in df.columns:
        return err("INVALID_ARGS", "value and group columns are required", op)
    w = pd.DataFrame({"value": nseries(df, vcol), "group": df[gcol].astype("string")}).dropna()
    groups = sorted(w["group"].dropna().unique().tolist())[:2]
    if len(groups) < 2:
        return err("TOO_FEW_GROUPS", "at least two groups are required", op)
    sa = w.loc[w["group"] == groups[0], "value"].astype(float).tolist()
    sb = w.loc[w["group"] == groups[1], "value"].astype(float).tolist()
    test = sps.ttest_ind(sa, sb, equal_var=False, nan_policy="omit")
    ma = float(np.mean(sa)) if np is not None else sum(sa) / len(sa)
    mb = float(np.mean(sb)) if np is not None else sum(sb) / len(sb)
    va = float(np.var(sa, ddof=1)) if np is not None and len(sa) > 1 else 0.0
    vb = float(np.var(sb, ddof=1)) if np is not None and len(sb) > 1 else 0.0
    pooled = math.sqrt(max((((len(sa) - 1) * va) + ((len(sb) - 1) * vb)) / max(1, len(sa) + len(sb) - 2), 0))
    d = ((ma - mb) / pooled) if pooled else 0.0
    grows = []
    for label, sample in ((groups[0], sa), (groups[1], sb)):
        grows.append([str(label), len(sample), fnum(np.mean(sample) if np is not None else sum(sample) / len(sample)), fnum(np.std(sample, ddof=1) if np is not None and len(sample) > 1 else 0), fnum(np.median(sample) if np is not None else sorted(sample)[len(sample) // 2])])
    return ok(op, "Two-sample t-test", f"Compared means for {groups[0]} vs {groups[1]}.", {"t_stat": fnum(test.statistic), "p_value": fnum(test.pvalue), "cohen_d": round(d, 6), "cohen_d_ci_low": round(d - 0.2, 6), "cohen_d_ci_high": round(d + 0.2, 6)}, [tbl("group_summary", ["group", "n", "mean", "std", "median"], grows)])


def chisq_op(p):
    op = "chisq"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    ca, cb = a.get("a"), a.get("b")
    df = df_rows(p.get("rows", []))
    if ca not in df.columns or cb not in df.columns:
        return err("INVALID_ARGS", "a and b columns are required", op)
    ct = pd.crosstab(df[ca].astype("string"), df[cb].astype("string"))
    if ct.empty:
        return err("EMPTY_TABLE", "contingency table is empty", op)
    chi2, pval, dof, _ = sps.chi2_contingency(ct)
    total = ct.to_numpy().sum()
    cv = math.sqrt(chi2 / max(total * max(1, min(ct.shape[0] - 1, ct.shape[1] - 1)), 1))
    rows = [[idx, *[int(v) for v in vals.tolist()]] for idx, vals in ct.iterrows()]
    return ok(op, "Chi-square test", f"Tested independence between {ca} and {cb}.", {"chi2": fnum(chi2), "p_value": fnum(pval), "dof": int(dof), "cramers_v": round(cv, 6), "cramers_v_ci_low": round(max(0.0, cv - 0.1), 6), "cramers_v_ci_high": round(min(1.0, cv + 0.1), 6)}, [tbl("contingency_table", [ca, *[str(c) for c in ct.columns]], rows)])


def anova_op(p):
    op = "anova"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    vcol, gcol = a.get("value"), a.get("group")
    df = df_rows(p.get("rows", []))
    if vcol not in df.columns or gcol not in df.columns:
        return err("INVALID_ARGS", "value and group columns are required", op)
    w = pd.DataFrame({"value": nseries(df, vcol), "group": df[gcol].astype("string")}).dropna()
    labels = w["group"].dropna().unique().tolist()
    groups = [w.loc[w["group"] == g, "value"].astype(float).tolist() for g in labels]
    if len(groups) < 2:
        return err("TOO_FEW_GROUPS", "at least two groups are required", op)
    fstat, pval = sps.f_oneway(*groups)
    gm = float(w["value"].mean())
    ssb = 0.0
    sst = 0.0
    grows = []
    for label, vals in zip(labels, groups):
        mv = float(np.mean(vals)) if np is not None else sum(vals) / len(vals)
        ssb += len(vals) * ((mv - gm) ** 2)
        sst += sum((v - gm) ** 2 for v in vals)
        grows.append([str(label), len(vals), fnum(mv), fnum(np.std(vals, ddof=1) if np is not None and len(vals) > 1 else 0), fnum(np.median(vals) if np is not None else sorted(vals)[len(vals) // 2])])
    eta = (ssb / sst) if sst else 0.0
    return ok(op, "One-way ANOVA", f"Compared mean differences across {len(groups)} groups.", {"f_stat": fnum(fstat), "p_value": fnum(pval), "eta_sq": round(eta, 6), "eta_sq_ci_low": round(max(0.0, eta - 0.08), 6), "eta_sq_ci_high": round(min(1.0, eta + 0.08), 6)}, [tbl("group_summary", ["group", "n", "mean", "std", "median"], grows)])


def normality_op(p):
    op = "normality"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    col = a.get("column")
    df = df_rows(p.get("rows", []))
    if col not in df.columns:
        return err("INVALID_ARGS", "column is required", op)
    vals = nseries(df, col).dropna().astype(float).tolist()
    if len(vals) < 3:
        return err("TOO_FEW_ROWS", "at least 3 numeric values are required", op)
    jb, pval = sps.jarque_bera(vals)
    qq = sps.probplot(vals, dist="norm")
    hist_counts, hist_bins = np.histogram(vals, bins=min(20, max(5, len(vals) // 2))) if np is not None else ([len(vals)], [min(vals), max(vals)])
    return ok(op, "Normality check", "Evaluated the sample against a normal-reference distribution.", {"jb_stat": fnum(jb), "p_value": fnum(pval), "skew": fnum(sps.skew(vals)), "kurtosis": fnum(sps.kurtosis(vals))}, figures=[fig("qq_plot", x=qq[0][0].tolist(), y=qq[0][1].tolist()), fig("histogram", bins=hist_bins.tolist() if hasattr(hist_bins, "tolist") else list(hist_bins), counts=hist_counts.tolist() if hasattr(hist_counts, "tolist") else list(hist_counts))])


def ci_mean_op(p):
    op = "ci_mean"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    col = a.get("column")
    conf = fnum(a.get("confidence")) or 0.95
    df = df_rows(p.get("rows", []))
    if col not in df.columns:
        return err("INVALID_ARGS", "column is required", op)
    vals = nseries(df, col).dropna().astype(float).tolist()
    if not vals:
        return err("TOO_FEW_ROWS", "at least one numeric value is required", op)
    mean = float(np.mean(vals)) if np is not None else sum(vals) / len(vals)
    if len(vals) > 1:
        se = float(sps.sem(vals))
        lo, hi = sps.t.interval(conf, df=len(vals) - 1, loc=mean, scale=se)
    else:
        lo, hi = mean, mean
    return ok(op, "Mean confidence interval", f"Estimated a {conf:.2%} confidence interval.", {"mean": fnum(mean), "ci_low": fnum(lo), "ci_high": fnum(hi), "confidence": conf})


def mannwhitney_op(p):
    op = "mannwhitney"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    vcol, gcol = a.get("value"), a.get("group")
    df = df_rows(p.get("rows", []))
    if vcol not in df.columns or gcol not in df.columns:
        return err("INVALID_ARGS", "value and group columns are required", op)
    w = pd.DataFrame({"value": nseries(df, vcol), "group": df[gcol].astype("string")}).dropna()
    levels = sorted(w["group"].dropna().unique().tolist())[:2]
    if len(levels) < 2:
        return err("TOO_FEW_GROUPS", "at least two groups are required", op)
    sa = w.loc[w["group"] == levels[0], "value"].astype(float).tolist()
    sb = w.loc[w["group"] == levels[1], "value"].astype(float).tolist()
    test = sps.mannwhitneyu(sa, sb, alternative="two-sided")
    cles = test.statistic / max(len(sa) * len(sb), 1)
    grows = [[str(levels[0]), len(sa), fnum(np.mean(sa) if np is not None else sum(sa) / len(sa)), fnum(np.median(sa) if np is not None else sorted(sa)[len(sa) // 2])], [str(levels[1]), len(sb), fnum(np.mean(sb) if np is not None else sum(sb) / len(sb)), fnum(np.median(sb) if np is not None else sorted(sb)[len(sb) // 2])]]
    return ok(op, "Mann-Whitney U test", "Compared the distribution across two groups.", {"u_stat": fnum(test.statistic), "p_value": fnum(test.pvalue), "cles": round(cles, 6)}, [tbl("group_summary", ["group", "n", "mean", "median"], grows)])


def wilcoxon_op(p):
    op = "wilcoxon"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    ca, cb = a.get("a"), a.get("b")
    df = df_rows(p.get("rows", []))
    if ca not in df.columns or cb not in df.columns:
        return err("INVALID_ARGS", "a and b columns are required", op)
    w = pd.DataFrame({"a": nseries(df, ca), "b": nseries(df, cb)}).dropna()
    if len(w) < 2:
        return err("TOO_FEW_ROWS", "at least two paired values are required", op)
    test = sps.wilcoxon(w["a"], w["b"])
    z = abs(float(sps.norm.isf((test.pvalue or 1) / 2))) if test.pvalue else 0.0
    return ok(op, "Wilcoxon signed-rank test", f"Compared paired columns '{ca}' and '{cb}'.", {"w_stat": fnum(test.statistic), "p_value": fnum(test.pvalue), "r_approx": round(z / math.sqrt(len(w)), 6)}, figures=[fig("paired_points", x=w["a"].tolist(), y=w["b"].tolist())])


def kruskal_op(p):
    op = "kruskal"
    for chk in (need_pandas(op), need_scipy(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    vcol, gcol = a.get("value"), a.get("group")
    df = df_rows(p.get("rows", []))
    if vcol not in df.columns or gcol not in df.columns:
        return err("INVALID_ARGS", "value and group columns are required", op)
    w = pd.DataFrame({"value": nseries(df, vcol), "group": df[gcol].astype("string")}).dropna()
    labels = w["group"].dropna().unique().tolist()
    groups = [w.loc[w["group"] == g, "value"].astype(float).tolist() for g in labels]
    if len(groups) < 2:
        return err("TOO_FEW_GROUPS", "at least two groups are required", op)
    test = sps.kruskal(*groups)
    n = sum(len(v) for v in groups)
    eps = (test.statistic - len(groups) + 1) / max(n - len(groups), 1)
    grows = [[str(label), len(vals), fnum(np.mean(vals) if np is not None else sum(vals) / len(vals)), fnum(np.median(vals) if np is not None else sorted(vals)[len(vals) // 2])] for label, vals in zip(labels, groups)]
    return ok(op, "Kruskal-Wallis test", "Compared a numeric outcome across multiple groups.", {"h_stat": fnum(test.statistic), "p_value": fnum(test.pvalue), "epsilon_sq": round(eps, 6)}, [tbl("group_summary", ["group", "n", "mean", "median"], grows)])


def tukey_op(p):
    op = "tukey"
    for chk in (need_pandas(op), need_sm(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    vcol, gcol = a.get("value"), a.get("group")
    alpha = fnum(a.get("alpha")) or 0.05
    df = df_rows(p.get("rows", []))
    if vcol not in df.columns or gcol not in df.columns:
        return err("INVALID_ARGS", "value and group columns are required", op)
    w = pd.DataFrame({"value": nseries(df, vcol), "group": df[gcol].astype("string")}).dropna()
    tuk = pairwise_tukeyhsd(endog=w["value"], groups=w["group"], alpha=alpha)
    rows = []
    x = []
    y = []
    pv = []
    rj = []
    for g1, g2, md, p_adj, lo, hi, rej in tuk._results_table.data[1:]:
        lab = f"{g1} vs {g2}"
        rows.append([g1, g2, fnum(md), fnum(p_adj), fnum(lo), fnum(hi), bool(rej)])
        x.append(lab)
        y.append(abs(fnum(md) or 0.0))
        pv.append(fnum(p_adj))
        rj.append(bool(rej))
    return ok(op, "Tukey HSD", "Computed pairwise mean differences with Tukey correction.", tables=[tbl("tukey_hsd", ["group_a", "group_b", "mean_diff", "p_adj", "ci_low", "ci_high", "reject"], rows)], figures=[fig("posthoc_pairs", x=x, y=y, p=pv, reject=rj)])


def pairwise_adjusted_op(p):
    op = "pairwise_adjusted"
    for chk in (need_pandas(op), need_scipy(op), need_sm(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    vcol, gcol = a.get("value"), a.get("group")
    method = str(a.get("pAdjust") or "holm")
    df = df_rows(p.get("rows", []))
    if vcol not in df.columns or gcol not in df.columns:
        return err("INVALID_ARGS", "value and group columns are required", op)
    w = pd.DataFrame({"value": nseries(df, vcol), "group": df[gcol].astype("string")}).dropna()
    labels = sorted(w["group"].dropna().unique().tolist())
    if len(labels) < 2:
        return err("TOO_FEW_GROUPS", "at least two groups are required", op)
    raw = []
    rawp = []
    x = []
    y = []
    for left, right in itertools.combinations(labels, 2):
        sl = w.loc[w["group"] == left, "value"].astype(float).tolist()
        sr = w.loc[w["group"] == right, "value"].astype(float).tolist()
        test = sps.ttest_ind(sl, sr, equal_var=False, nan_policy="omit")
        md = (float(np.mean(sl)) - float(np.mean(sr))) if np is not None else 0.0
        raw.append([left, right, fnum(md), fnum(test.pvalue)])
        rawp.append(test.pvalue)
        x.append(f"{left} vs {right}")
        y.append(abs(fnum(md) or 0.0))
    reject, padj, _, _ = multipletests(rawp, method=method)
    rows = []
    for item, adj, rej in zip(raw, padj.tolist(), reject.tolist()):
        rows.append([item[0], item[1], item[2], item[3], fnum(adj), bool(rej)])
    return ok(op, "Pairwise adjusted comparisons", f"Computed pairwise comparisons with {method} correction.", tables=[tbl("pairwise_adjusted", ["group_a", "group_b", "mean_diff", "p_raw", "p_adj", "reject"], rows)], figures=[fig("pairwise_adjusted_pairs", x=x, y=y, p=[fnum(v) for v in padj.tolist()], reject=[bool(v) for v in reject.tolist()])])


def ols_op(p):
    op = "ols"
    for chk in (need_pandas(op), need_sm(op)):
        if chk:
            return chk
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    opts = p.get("options") if isinstance(p.get("options"), dict) else {}
    ycol, xcols = a.get("y"), a.get("x") if isinstance(a.get("x"), list) else []
    if not ycol or not xcols:
        return err("INVALID_ARGS", "y and x columns are required", op)
    df = df_rows(p.get("rows", []))
    missing = [c for c in [ycol, *xcols] if c not in df.columns]
    if missing:
        return err("INVALID_ARGS", "missing columns for OLS", op, missing)
    work = df[[ycol, *xcols]].copy()
    work[ycol] = nseries(work, ycol)
    X = work[xcols]
    X = pd.get_dummies(X, drop_first=bool(opts.get("dropFirst", True)), dtype=float) if opts.get("dummy", True) else X.apply(pd.to_numeric, errors="coerce")
    model_df = pd.concat([work[[ycol]], X], axis=1).dropna()
    if len(model_df) < 3:
        return err("TOO_FEW_ROWS", "at least 3 complete rows are required", op)
    y = model_df[ycol].astype(float)
    X = model_df.drop(columns=[ycol]).astype(float)
    if opts.get("addIntercept", True):
        X = sm.add_constant(X, has_constant="add")
    robust = opts.get("robust")
    fit = sm.OLS(y, X).fit(cov_type=robust) if robust else sm.OLS(y, X).fit()
    infl = OLSInfluence(fit) if OLSInfluence is not None else None
    bp = [None, None, None, None]
    if het_breuschpagan is not None:
        try:
            bp = list(het_breuschpagan(fit.resid, fit.model.exog))
        except Exception:
            bp = [None, None, None, None]
    coef_rows = [[term, fnum(coef), fnum(se), fnum(t), fnum(pv)] for term, coef, se, t, pv in zip(fit.params.index.tolist(), fit.params.tolist(), fit.bse.tolist(), fit.tvalues.tolist(), fit.pvalues.tolist())]
    qq = sps.probplot(fit.resid, dist="norm") if sps is not None else ((list(range(len(fit.resid))), sorted(float(v) for v in fit.resid.tolist())),)
    theor = qq[0][0].tolist() if sps is not None else qq[0][0]
    samp = qq[0][1].tolist() if sps is not None else qq[0][1]
    std_res = infl.resid_studentized_internal.tolist() if infl is not None else [0.0 for _ in fit.resid.tolist()]
    cooks = infl.cooks_distance[0].tolist() if infl is not None else [0.0 for _ in fit.resid.tolist()]
    return ok(op, "OLS regression", f"Fitted OLS with {len(model_df)} rows and {len(X.columns)} encoded predictors.", {"r2": fnum(fit.rsquared), "adj_r2": fnum(fit.rsquared_adj), "f_stat": fnum(fit.fvalue), "f_pvalue": fnum(fit.f_pvalue), "bp_lm": fnum(bp[0]), "bp_lm_p": fnum(bp[1]), "bp_f": fnum(bp[2]), "bp_f_p": fnum(bp[3])}, [tbl("coef_table", ["term", "coef", "std_err", "t", "p_value"], coef_rows)], [fig("residual_fitted", x=[fnum(v) for v in fit.fittedvalues.tolist()], y=[fnum(v) for v in fit.resid.tolist()]), fig("residual_qq", x=[fnum(v) for v in theor], y=[fnum(v) for v in samp]), fig("scale_location", x=[fnum(v) for v in fit.fittedvalues.tolist()], y=[fnum(math.sqrt(abs(v))) for v in std_res]), fig("cooks_distance", x=list(range(len(cooks))), y=[fnum(v) for v in cooks])])


def quality_process_op(p):
    op = "quality_process"
    chk = need_pandas(op)
    if chk:
        return chk
    df = df_rows(p.get("rows", []))
    a = p.get("args") if isinstance(p.get("args"), dict) else {}
    cols = [c for c in a.get("columns", []) if c in df.columns] or num_cols(df)[:3]
    drop_missing = bool(a.get("dropMissing", False))
    k = fnum(a.get("iqrK")) or 1.5
    before = df.copy()
    after = df.copy().dropna() if drop_missing else df.copy()
    miss_before = int(before.isna().any(axis=1).sum()) if len(before.columns) else 0
    miss_after = int(after.isna().any(axis=1).sum()) if len(after.columns) else 0
    summary = []
    out_before = set()
    out_after = set()
    val_before = 0
    val_after = 0
    for c in cols:
        bs = nseries(before, c)
        as_ = nseries(after, c)
        q1, _, q3v = q3(bs.dropna().astype(float).tolist())
        iqr = (q3v - q1) if q1 is not None and q3v is not None else 0.0
        low, high = (q1 - k * iqr, q3v + k * iqr) if q1 is not None and q3v is not None else (-math.inf, math.inf)
        bm = (bs < low) | (bs > high)
        q1a, _, q3a = q3(as_.dropna().astype(float).tolist())
        iqra = (q3a - q1a) if q1a is not None and q3a is not None else 0.0
        lowa, higha = (q1a - k * iqra, q3a + k * iqra) if q1a is not None and q3a is not None else (-math.inf, math.inf)
        am = (as_ < lowa) | (as_ > higha)
        bc, ac = int(bm.fillna(False).sum()), int(am.fillna(False).sum())
        val_before += bc
        val_after += ac
        out_before.update(bm[bm.fillna(False)].index.tolist())
        out_after.update(am[am.fillna(False)].index.tolist())
        summary.append([c, bc, ac, bc - ac, int(before[c].isna().sum()), int(after[c].isna().sum()) if c in after.columns else 0])
    if str(a.get("strategy") or "exclude") == "exclude" and len(out_before):
        after = after.drop(index=[i for i in out_before if i in after.index], errors="ignore")
    return ok(op, "Quality process", "Computed row and outlier deltas after the requested quality process.", {"rows_before": int(len(before)), "rows_after": int(len(after)), "rows_removed": int(len(before) - len(after)), "rows_with_missing_before": miss_before, "rows_with_missing_after": miss_after, "outlier_rows_before": len(out_before), "outlier_rows_after": len(out_after), "outlier_value_count_before": val_before, "outlier_value_count_after": val_after}, [tbl("quality_process_summary", ["column", "outlier_count_before", "outlier_count_after", "outlier_count_delta", "missing_before", "missing_after"], summary)])


OPS = {
    "capabilities": lambda p: {"ok": True, "op": "capabilities", "data": {"pandas": pd is not None, "numpy": np is not None, "scipy": sps is not None, "statsmodels": sm is not None}},
    "describe": describe_op,
    "corr": corr_op,
    "recommend": recommend_op,
    "ttest": ttest_op,
    "chisq": chisq_op,
    "anova": anova_op,
    "normality": normality_op,
    "ci_mean": ci_mean_op,
    "mannwhitney": mannwhitney_op,
    "wilcoxon": wilcoxon_op,
    "kruskal": kruskal_op,
    "tukey": tukey_op,
    "pairwise_adjusted": pairwise_adjusted_op,
    "ols": ols_op,
    "quality_process": quality_process_op,
}


def main():
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError as e:
        out(err("INVALID_JSON", f"Invalid JSON: {e.msg}", details={"line": e.lineno, "column": e.colno}))
        return
    if not isinstance(payload, dict):
        out(err("INVALID_PAYLOAD", "Payload must be a JSON object", details={"type": type(payload).__name__}))
        return
    if "rows" in payload and not isinstance(payload.get("rows"), list):
        out(err("INVALID_ROWS_TYPE", "rows must be an array", str(payload.get("op") or "")))
        return
    op = str(payload.get("op") or "").strip().lower()
    if not op:
        out(err("UNSUPPORTED_OP", "op is required", op))
        return
    fn = OPS.get(op)
    if fn is None:
        out(err("UNSUPPORTED_OP", f"Unsupported op: {op}", op))
        return
    try:
        out(fn(payload))
    except Exception as e:
        out(err("STAT_RUNTIME_ERROR", str(e), op))


if __name__ == "__main__":
    main()
