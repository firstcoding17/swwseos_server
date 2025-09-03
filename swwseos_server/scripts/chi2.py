# swwseos_server/scripts/chi2.py
import sys, json, pandas as pd
from scipy.stats import chi2_contingency

if len(sys.argv) < 4:
    print(json.dumps({"error":"usage: chi2.py <filePath> <colA> <colB>"})); sys.exit(1)

file_path = sys.argv[1]
col_a = sys.argv[2]
col_b = sys.argv[3]

df = pd.read_csv(file_path)
table = pd.crosstab(df[col_a], df[col_b])

if table.empty:
    print(json.dumps({"error":"contingency table is empty"})); sys.exit(1)

chi2, p, dof, expected = chi2_contingency(table)

out = {
    "chi2": float(chi2),
    "p": float(p),
    "dof": int(dof),
    "observed": table.astype(int).to_dict(),
    "expected": pd.DataFrame(expected, index=table.index, columns=table.columns).round(6).to_dict()
}
print(json.dumps(out, ensure_ascii=False))
