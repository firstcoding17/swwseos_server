#!/usr/bin/env python
import sys, json
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

def palette_seq(name: str):
    if name == "pastel":
        return px.colors.qualitative.Pastel
    if name == "vivid":
        return px.colors.qualitative.Vivid
    if name == "mono":
        return ["#5f6c7b","#778396","#9aa8b7","#c2ccd9","#e1e6ef","#f4f6fb"]
    return px.colors.qualitative.Plotly

def main():
    raw = sys.stdin.read()
    payload = json.loads(raw or "{}")
    rows = payload.get("rows", [])
    spec = payload.get("spec", {}) or {}
    opts = (spec.get("options") or {})

    df = pd.DataFrame(rows)
    if df.shape[0] == 0:
        fig = go.Figure(); fig.update_layout(title="(empty)")
        print(json.dumps({"fig_json": fig.to_json()}))
        return

    t = (spec.get("type") or "").lower()
    x = spec.get("x"); y = spec.get("y"); hue = spec.get("hue"); size = spec.get("size")
    pal = palette_seq(opts.get("palette", "default"))

    def agg_by_x(frame: pd.DataFrame, x_col: str, y_col: str, agg_mode: str):
        if not x_col or x_col not in frame.columns:
            return pd.DataFrame(columns=["label", "value"])
        g = frame[[x_col]].copy()
        g["_y"] = pd.to_numeric(frame[y_col], errors="coerce") if (y_col and y_col in frame.columns) else np.nan
        if (not y_col) or (agg_mode == "count"):
            out = g.groupby(x_col, dropna=False).size().reset_index(name="value")
        elif agg_mode == "sum":
            out = g.groupby(x_col, dropna=False)["_y"].sum().reset_index(name="value")
        elif agg_mode == "mean":
            out = g.groupby(x_col, dropna=False)["_y"].mean().reset_index(name="value")
        else:
            out = g.groupby(x_col, dropna=False).size().reset_index(name="value")
        out = out.rename(columns={x_col: "label"})
        out["label"] = out["label"].astype(str)
        out["value"] = pd.to_numeric(out["value"], errors="coerce").fillna(0.0)
        return out

    def label_layout(fig):
        title = opts.get("title") or ""
        xlab = opts.get("xLabel") or (x or "")
        ylab = opts.get("yLabel") or (y or "")
        if title: fig.update_layout(title=title)
        if xlab: fig.update_xaxes(title=xlab)
        if ylab: fig.update_yaxes(title=ylab)
        return fig

    if t == "bar":
        agg = (opts.get("agg") or "count")
        if y is None:
            g = df.groupby(x, dropna=False).size().reset_index(name="value")
            fig = px.bar(g, x=x, y="value", color=hue, color_discrete_sequence=pal)
        else:
            if agg == "sum":
                g = df.groupby([c for c in [x, hue] if c], dropna=False)[y].sum().reset_index()
                fig = px.bar(g, x=x, y=y, color=hue, barmode="group", color_discrete_sequence=pal)
            elif agg == "mean":
                g = df.groupby([c for c in [x, hue] if c], dropna=False)[y].mean().reset_index()
                fig = px.bar(g, x=x, y=y, color=hue, barmode="group", color_discrete_sequence=pal)
            else:
                g = df.groupby([c for c in [x, hue] if c], dropna=False).size().reset_index(name="value")
                fig = px.bar(g, x=x, y="value", color=hue, barmode="group", color_discrete_sequence=pal)
    elif t == "line":
        fig = px.line(df.sort_values(by=x), x=x, y=y, color=hue, markers=True, color_discrete_sequence=pal)
    elif t == "area":
        fig = px.area(df.sort_values(by=x), x=x, y=y, color=hue, color_discrete_sequence=pal)
        if opts.get("stackedArea") is False:
            fig.update_traces(stackgroup=None, fill="tozeroy")
    elif t == "scatter":
        fig = px.scatter(df, x=x, y=y, color=hue, color_discrete_sequence=pal)
    elif t == "bubble":
        size_col = size if (size and size in df.columns) else None
        fig = px.scatter(df, x=x, y=y, color=hue, size=size_col, color_discrete_sequence=pal)
    elif t == "histogram":
        bins = int(opts.get("bins", 30))
        fig = px.histogram(df, x=x, nbins=bins, color=hue, color_discrete_sequence=pal)
    elif t == "box":
        fig = px.box(df, x=hue, y=y, color=hue, color_discrete_sequence=pal)
    elif t == "violin":
        fig = px.violin(df, x=hue, y=y, color=hue, box=True, points="outliers", color_discrete_sequence=pal)
    elif t == "treemap":
        fig = px.treemap(df, path=[hue, x] if hue else [x], values=y if y else None)
    elif t == "pie":
        agg = (opts.get("agg") or "count")
        g = agg_by_x(df, x, y, agg)
        fig = px.pie(g, names="label", values="value", color="label", color_discrete_sequence=pal)
    elif t == "donut":
        agg = (opts.get("agg") or "count")
        g = agg_by_x(df, x, y, agg)
        fig = px.pie(g, names="label", values="value", color="label", hole=0.45, color_discrete_sequence=pal)
    elif t == "funnel":
        agg = (opts.get("agg") or "count")
        g = agg_by_x(df, x, y, agg)
        fig = px.funnel(g, y="label", x="value")
    elif t == "waterfall":
        agg = (opts.get("agg") or "count")
        g = agg_by_x(df, x, y, agg)
        fig = go.Figure(go.Waterfall(
            x=g["label"].tolist(),
            y=g["value"].astype(float).tolist(),
            measure=["relative"] * len(g),
        ))
    elif t == "heatmap":
        if hue and hue in df.columns:
            g = df.groupby([x, y], dropna=False)[hue].sum().reset_index()
            pivot = g.pivot(index=y, columns=x, values=hue).fillna(0)
        else:
            g = df.groupby([x, y], dropna=False).size().reset_index(name="value")
            pivot = g.pivot(index=y, columns=x, values="value").fillna(0)
        fig = px.imshow(pivot.values, x=list(pivot.columns), y=list(pivot.index), color_continuous_scale="Viridis")
    elif t == "radar":
        if not (x and y):
            fig = go.Figure(); fig.update_layout(title="Radar: set x,y")
            print(json.dumps({"fig_json": fig.to_json()})); return
        axes = df[x].astype(str).unique().tolist()
        fig = go.Figure()
        if hue and hue in df.columns:
            for k, sub in df.groupby(hue):
                m = {str(r[x]): r[y] for _, r in sub.iterrows()}
                r = [m.get(a, 0) for a in axes] + [m.get(axes[0], 0)]
                fig.add_trace(go.Scatterpolar(r=r, theta=axes+[axes[0]], fill="toself", name=str(k)))
        else:
            m = {str(r[x]): r[y] for _, r in df.iterrows()}
            r = [m.get(a, 0) for a in axes] + [m.get(axes[0], 0)]
            fig.add_trace(go.Scatterpolar(r=r, theta=axes+[axes[0]], fill="toself", name="series"))
        fig.update_layout(polar=dict(radialaxis=dict(visible=True)))
    elif t == "sankey":
        src = df[x].astype(str).tolist()
        tgt = df[y].astype(str).tolist()
        if hue and hue in df.columns:
            val = df[hue].fillna(1).astype(float).tolist()
        else:
            val = [1.0]*len(df)
        labels = list(pd.Index(src + tgt).unique())
        index = {s:i for i,s in enumerate(labels)}
        sources = [index[s] for s in src]
        targets = [index[t] for t in tgt]
        fig = go.Figure(data=[go.Sankey(node=dict(label=labels), link=dict(source=sources, target=targets, value=val))])
    else:
        fig = go.Figure(); fig.update_layout(title=f"Unsupported type: {t}")

    fig = label_layout(fig)
    print(json.dumps({"fig_json": fig.to_json()}))

if __name__ == "__main__":
    main()
