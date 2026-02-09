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

def describe_report(df: pd.DataFrame, topn_cat=10):
    meta = {}
    meta["rows"] = int(df.shape[0])
    meta["cols"] = int(df.shape[1])

    # ????붿빟
    dtype_summary = df.dtypes.astype(str).value_counts().reset_index()
    dtype_summary.columns = ["dtype", "count"]

    # 寃곗륫瑜?TOP
    na = df.isna().mean().sort_values(ascending=False)
    na_top = na.head(10).reset_index()
    na_top.columns = ["column", "null_rate"]

    # 以묐났??
    dup = int(df.duplicated().sum())

    # ?섏튂??踰붿＜??遺꾨━
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = [c for c in df.columns if c not in num_cols]

    # ?섏튂 describe
    num_desc = None
    if len(num_cols) > 0:
        num_desc = df[num_cols].describe().T  # count mean std min 25% 50% 75% max
        # 蹂닿린 醫뗪쾶 ?뺣젹/諛섏삱由??먰븯硫??꾨줎?몄뿉??
        num_desc = num_desc.reset_index().rename(columns={"index":"column"})

    # 踰붿＜ describe(?좊땲??top/freq) + TopN 遺꾪룷
    cat_desc = None
    cat_top_tables = []
    if len(cat_cols) > 0:
        # pandas describe(include=['object'])??mixed?먯꽌 ?좊ℓ?섎땲 吏곸젒
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
            *([to_table(cat_desc, "categorical_describe")] if cat_desc is not None else []),
            *cat_top_tables
        ],
        "warnings": [],
        "meta": meta
    }

    # 寃쎄퀬
    if meta["rows"] < 20:
        result["warnings"].append("Sample size is very small (rows < 20). Use results for reference only.")
    if len(num_cols) == 0:
        result["warnings"].append("No numeric columns found.")
    if len(cat_cols) == 0:
        result["warnings"].append("No categorical columns found.")

    return result

def main():
    t0 = time.time()
    raw = sys.stdin.read()
    payload = json.loads(raw or "{}")

    op = payload.get("op", "describe")
    rows = payload.get("rows", [])
    options = payload.get("options", {}) or {}

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
        out = ttest_report(df, args.get("value"), args.get("group"))
    elif op == "chisq":
        args = payload.get("args", {}) or {}
        out = chisq_report(df, args.get("a"), args.get("b"))
    elif op == "ols":
        args = payload.get("args", {}) or {}
        opts = payload.get("options", {}) or {}
        out = ols_report(df, args.get("y"), args.get("x") or [], opts)
    else:
        out = {"ok": False, "op": op, "error": "unsupported op in PASS1"}

    out.setdefault("meta", {})
    out["meta"]["execMs"] = int((time.time() - t0) * 1000)
    j(out)
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

    # heatmap figure data (GraphPanel?먯꽌 諛붾줈 ?곌린 醫뗭? ?뺥깭)
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
    # ?섏튂 蹂??
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

    # ?쒕낯 ??泥댄겕
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

    # ?④낵?ш린 Cohen's d (Welch?⑹쑝濡?pooled sd 媛꾨떒踰꾩쟾)
    mean1, mean2 = float(np.mean(a)), float(np.mean(b))
    sd1, sd2 = float(np.std(a, ddof=1)), float(np.std(b, ddof=1))
    pooled = np.sqrt((sd1**2 + sd2**2)/2) if (sd1>0 or sd2>0) else 0.0
    d_eff = (mean1-mean2)/pooled if pooled>0 else 0.0

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
            "stats": {"t": float(tstat), "p": float(p), "cohen_d": float(d_eff)}
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

    # Cram챕r?셲 V
    n = ct.values.sum()
    r, k = ct.shape
    v = np.sqrt((chi2 / n) / (min(r-1, k-1))) if n>0 and min(r-1,k-1)>0 else 0.0

    # expected???덈Т ?щ㈃ ?뚯씠釉붾줈 ???대룄 ??MVP???앸왂 媛??
    ct_df = ct.reset_index().rename(columns={a_col:"row"})
    conclusion = "A statistically significant association exists (p < 0.05)." if p < 0.05 else "No statistically significant association (p >= 0.05)."

    return {
        "ok": True,
        "op": "chisq",
        "inputs": {"a": a_col, "b": b_col},
        "summary": {
            "title": "Categorical-Categorical Association (Chi-square Test)",
            "conclusion": conclusion,
            "stats": {"chi2": float(chi2), "p": float(p), "dof": int(dof), "cramers_v": float(v)}
        },
        "tables": [to_table(ct_df, "contingency_table")],
        "warnings": [],
        "meta": {"n": int(n)}
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
    robust = options.get("robust", None)  # ?? "HC3" ?먮뒗 None

    use_cols = [y_col] + list(dict.fromkeys(x_cols))
    d = df[use_cols].copy()

    # y???섏튂濡?媛뺤젣
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce")

    # X 泥섎━: ?섏튂/踰붿＜ ?쇳빀 媛??
    X = d[x_cols].copy()
    for c in x_cols:
        if pd.api.types.is_numeric_dtype(X[c]):
            X[c] = pd.to_numeric(X[c], errors="coerce")
        else:
            X[c] = X[c].astype("string")

    # 寃곗륫 ?쒓굅
    before = int(d.shape[0])
    dd = pd.concat([d[[y_col]], X], axis=1).dropna()
    dropped = before - int(dd.shape[0])

    if dd.shape[0] < max(20, len(x_cols)*5):
        # ?덈Т ?묒? ?섑뵆?대㈃ 寃쎄퀬(洹몃옒???ㅽ뻾? 媛?ν븯寃?
        warn_small = f"Sample size is small (n={int(dd.shape[0])}). Use results for reference only."
    else:
        warn_small = None

    y = dd[y_col].astype(float)

    X2 = dd[x_cols].copy()
    if dummy:
        # 踰붿＜?뺤? ?붾?濡?蹂??
        cat_cols = [c for c in x_cols if not pd.api.types.is_numeric_dtype(X2[c])]
        if cat_cols:
            X2 = pd.get_dummies(X2, columns=cat_cols, drop_first=drop_first)

    # ?곸닔??
    if add_intercept:
        X2 = sm.add_constant(X2, has_constant="add")

    # ?곸닔??遺꾩궛0 而щ읆 ?쒓굅(紐⑤뜽 ??컻 諛⑹?)
    nunique = X2.nunique(dropna=False)
    bad_cols = nunique[nunique <= 1].index.tolist()
    if bad_cols:
        X2 = X2.drop(columns=bad_cols, errors="ignore")

    # ????泥댄겕
    if X2.shape[1] == 0:
        return {
            "ok": True, "op":"ols",
            "summary":{"title":"OLS Regression","conclusion":"No valid predictors remain after constant/missing/dummy processing.","stats":{}},
            "tables": [],
            "warnings":["No valid X columns remain."],
            "meta":{"n": int(dd.shape[0]), "droppedNA": dropped}
        }

    # 紐⑤뜽 ?곹빀
    model = sm.OLS(y, X2).fit()

    # 濡쒕쾭?ㅽ듃 ?쒖??ㅼ감 ?듭뀡
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

    # 紐⑤뜽 ?붿빟
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
        "robust": robust
    }

    warnings = []
    if dropped > 0:
        warnings.append(f"Excluded {dropped} rows due to missing values.")
    if warn_small:
        warnings.append(warn_small)
    if bad_cols:
        warnings.append(f"Removed constant (no variance) columns: {', ' .join(bad_cols[:8])}" + ("..." if len(bad_cols)>8 else ""))

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
        "warnings": warnings,
        "meta": {"n": int(dd.shape[0]), "droppedNA": dropped, "xColsUsed": int(X2.shape[1])}
    }


if __name__ == "__main__":
    main()
