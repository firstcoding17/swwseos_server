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
    x = spec.get("x"); y = spec.get("y"); hue = spec.get("hue")
    pal = palette_seq(opts.get("palette", "default"))

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
    elif t == "scatter":
        fig = px.scatter(df, x=x, y=y, color=hue, color_discrete_sequence=pal)
    elif t == "histogram":
        bins = int(opts.get("bins", 30))
        fig = px.histogram(df, x=x, nbins=bins, color=hue, color_discrete_sequence=pal)
    elif t == "box":
        fig = px.box(df, x=hue, y=y, color=hue, color_discrete_sequence=pal)
    elif t == "violin":
        fig = px.violin(df, x=hue, y=y, color=hue, box=True, points="outliers", color_discrete_sequence=pal)
    elif t == "treemap":
        fig = px.treemap(df, path=[hue, x] if hue else [x], values=y if y else None)
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
