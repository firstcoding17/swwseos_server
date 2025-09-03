# swwseos_server/scripts/ttest.py
import sys, json, pandas as pd
from scipy import stats

if len(sys.argv) < 6:
    print(json.dumps({
        "error": "usage: ttest.py <filePath> <valueCol> <groupCol> <groupA> <groupB> [equal_var]"
    })); sys.exit(1)

file_path = sys.argv[1]
val_col  = sys.argv[2]
grp_col  = sys.argv[3]
grp_a    = sys.argv[4]
grp_b    = sys.argv[5]
equal_var = (len(sys.argv) >= 7 and sys.argv[6].lower() == "true")

df = pd.read_csv(file_path)

a = df[df[grp_col].astype(str) == str(grp_a)][val_col].dropna()
b = df[df[grp_col].astype(str) == str(grp_b)][val_col].dropna()

if a.empty or b.empty:
    print(json.dumps({"error":"one of the groups has no data"})); sys.exit(1)

t, p = stats.ttest_ind(a, b, equal_var=equal_var)

out = {
    "t": float(t),
    "p": float(p),
    "equal_var": bool(equal_var),
    "nA": int(a.shape[0]),
    "nB": int(b.shape[0]),
    "meanA": float(a.mean()),
    "meanB": float(b.mean()),
    "stdA": float(a.std()),
    "stdB": float(b.std())
}
print(json.dumps(out, ensure_ascii=False))
