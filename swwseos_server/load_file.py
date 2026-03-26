import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from io import BytesIO
sys.stdout.reconfigure(encoding='utf-8')
# Keep stdout binary-only for image response.

try:
    data_json = sys.argv[1]
    x_column = sys.argv[2]
    y_column = sys.argv[3]
    graph_type = (sys.argv[4] if len(sys.argv) > 4 else "scatter").lower()

    # Keep stdout clean: no text logs before image bytes.

    data = json.loads(data_json)
    df = pd.DataFrame(data['rows'], columns=data['columns'])

    plt.figure(figsize=(10, 6))
    if graph_type == "line":
        sns.lineplot(data=df, x=x_column, y=y_column)
        plt.title(f"{x_column} vs {y_column} (line)")
        plt.xlabel(x_column)
        plt.ylabel(y_column)
    elif graph_type == "bar":
        if y_column:
            bar_df = (
                df[[x_column, y_column]]
                .dropna()
                .groupby(x_column, as_index=False)[y_column]
                .mean()
            )
            sns.barplot(data=bar_df, x=x_column, y=y_column)
        else:
            bar_df = df[[x_column]].dropna().value_counts().reset_index(name='count')
            sns.barplot(data=bar_df, x=x_column, y='count')
        plt.title(f"{x_column} bar chart")
        plt.xlabel(x_column)
        plt.ylabel(y_column or "count")
    elif graph_type == "box":
        if not y_column:
            raise ValueError("box graph requires y column")
        box_df = df[[x_column, y_column]].dropna()
        sns.boxplot(data=box_df, x=x_column, y=y_column)
        plt.title(f"{y_column} by {x_column} (box)")
        plt.xlabel(x_column)
        plt.ylabel(y_column)
    elif graph_type == "histogram":
        sns.histplot(data=df, x=x_column, bins=30)
        plt.title(f"{x_column} distribution")
        plt.xlabel(x_column)
        plt.ylabel("count")
    else:
        sns.scatterplot(data=df, x=x_column, y=y_column)
        plt.title(f"{x_column} vs {y_column}")
        plt.xlabel(x_column)
        plt.ylabel(y_column)

    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    sys.stdout.buffer.write(buffer.read())

except Exception as e:
    print(f"Graph generation failed: {e}", file=sys.stderr)
    sys.exit(1)
