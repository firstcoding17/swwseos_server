#!/usr/bin/env python
import sys, json
import pandas as pd
import numpy as np

def json_out(obj): print(json.dumps(obj, ensure_ascii=False))

def groupby_agg(df, x, y, hue, agg):
    keys = [c for c in [x, hue] if c]
    if not keys:
        return {"result": {"x": [], "y": [], "hue": None}, "meta": {"op":"groupby","rowsUsed":int(df.shape[0])}}
    if y is None or agg == "count":
        g = df.groupby(keys, dropna=False).size().reset_index(name="value")
        ycol = "value"
    else:
        if agg == "sum":
            g = df.groupby(keys, dropna=False)[y].sum().reset_index()
            ycol = y
        elif agg == "mean":
            g = df.groupby(keys, dropna=False)[y].mean().reset_index()
            ycol = y
        else:  # default count
            g = df.groupby(keys, dropna=False).size().reset_index(name="value")
            ycol = "value"
    if hue:
        # 여러 시리즈로 반환
        series = {}
        for k, sub in g.groupby(hue, dropna=False):
            series[str(k)] = {
                "x": sub[x].astype(str).tolist(),
                "y": sub[ycol].astype(float).tolist()
            }
        return {"result": {"series": series, "hue": hue}, "meta": {"op":"groupby","rowsUsed":int(df.shape[0])}}
    else:
        return {"result": {"x": g[x].astype(str).tolist(), "y": g[ycol].astype(float).tolist(), "hue": None},
                "meta": {"op":"groupby","rowsUsed":int(df.shape[0])}}

def histogram(df, x, bins):
    vals = pd.to_numeric(df[x], errors='coerce').dropna().to_numpy()
    cnt, edges = np.histogram(vals, bins=int(bins))
    return {"result": {"bins": edges.tolist(), "counts": cnt.astype(int).tolist()},
            "meta": {"op":"hist","rowsUsed":int(len(vals))}}

def quantiles_by_group(df, y, hue):
    vals = pd.to_numeric(df[y], errors='coerce')
    df2 = df.copy()
    df2['_y'] = vals
    df2 = df2.dropna(subset=['_y'])
    out = {}
    if hue and hue in df2.columns:
        for k, sub in df2.groupby(hue, dropna=False):
            q = sub['_y'].quantile([0.25,0.5,0.75])
            out[str(k)] = {"q1": float(q.loc[0.25]), "median": float(q.loc[0.5]), "q3": float(q.loc[0.75])}
    else:
        q = df2['_y'].quantile([0.25,0.5,0.75])
        out["__all__"] = {"q1": float(q.loc[0.25]), "median": float(q.loc[0.5]), "q3": float(q.loc[0.75])}
    return {"result": {"quantiles": out, "hue": hue or None}, "meta":{"op":"quantiles","rowsUsed":int(df2.shape[0])}}

def heatmap_2d_bin(df, x, y, bins=30):
    xv = pd.to_numeric(df[x], errors='coerce').dropna()
    yv = pd.to_numeric(df[y], errors='coerce').dropna()
    # 공통 인덱스 맞추기보다 간단히 dropna 함께
    d = pd.DataFrame({x:xv, y:yv}).dropna()
    H, xedges, yedges = np.histogram2d(d[x].to_numpy(), d[y].to_numpy(), bins=int(bins))
    return {"result":{"xBins": xedges.tolist(), "yBins": yedges.tolist(), "zCounts": H.astype(int).tolist()},
            "meta":{"op":"2dbin","rowsUsed":int(d.shape[0])}}

def resample_line(df, x, y, rule, agg):
    # x: datetime 파싱
    s = pd.to_datetime(df[x], errors='coerce')
    df2 = df.copy()
    df2['_dt'] = s
    df2 = df2.dropna(subset=['_dt'])
    df2 = df2.set_index('_dt').sort_index()
    if agg == 'sum':
        s = pd.to_numeric(df2[y], errors='coerce').resample(rule).sum()
    elif agg == 'mean':
        s = pd.to_numeric(df2[y], errors='coerce').resample(rule).mean()
    else:  # count
        s = pd.to_numeric(df2[y], errors='coerce').resample(rule).count()
    s = s.dropna()
    return {"result":{"x": [ts.isoformat() for ts in s.index.to_pydatetime()], "y": s.astype(float).tolist()},
            "meta":{"op":"resample","rowsUsed":int(df2.shape[0]), "rule":rule, "agg":agg}}

def main():
    raw = sys.stdin.read()
    payload = json.loads(raw or "{}")
    rows = payload.get("rows", [])
    spec = payload.get("spec", {}) or {}
    opts = (spec.get("options") or {})
    df = pd.DataFrame(rows)

    t = (spec.get("type") or "").lower()
    x = spec.get("x"); y = spec.get("y"); hue = spec.get("hue")

    if df.shape[0] == 0:
        return json_out({"result": {}, "meta": {"rowsUsed": 0}})

    if t == 'bar':
        agg = (opts.get("agg") or "count")
        return json_out(groupby_agg(df, x, y, hue, agg))

    if t == 'histogram':
        bins = int(opts.get("bins", 30))
        return json_out(histogram(df, x, bins))

    if t in ('box','violin'):
        return json_out(quantiles_by_group(df, y, hue))

    if t == 'heatmap':
        # 숫자형 x/y 가정 (상관 히트맵이 아니라 2D bin)
        bins = int(opts.get("bins", 30))
        return json_out(heatmap_2d_bin(df, x, y, bins))

    if t == 'line' and opts.get('resample'):
        rule = opts.get('resample')  # 'D','W','M' 등
        agg = (opts.get('agg') or 'sum')
        return json_out(resample_line(df, x, y, rule, agg))

    # 기본: 미지원 → 프런트 집계로 폴백
    return json_out({"result": {}, "meta": {"op": "noop"}})

if __name__ == "__main__":
    main()
