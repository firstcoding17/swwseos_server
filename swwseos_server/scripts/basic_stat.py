import sys, json, pandas as pd

if len(sys.argv) < 3:
    print(json.dumps({"error": "usage: basic_stat.py <filePath> <column>"})); sys.exit(1)

file_path = sys.argv[1]
column = sys.argv[2]

df = pd.read_csv(file_path)
col = df[column].dropna()

result = {
    "mean": float(col.mean()),
    "median": float(col.median()),
    "std": float(col.std()),
    "min": float(col.min()),
    "max": float(col.max()),
    "q1": float(col.quantile(0.25)),
    "q3": float(col.quantile(0.75)),
    "count": int(col.shape[0]),
}
print(json.dumps(result, ensure_ascii=False))
