# swwseos_server/scripts/linreg.py
import sys, json, pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

# usage: linreg.py <filePath> <target> <feat1,feat2,...>
if len(sys.argv) < 4:
    print(json.dumps({"error":"usage: linreg.py <filePath> <target> <featuresCSV>"})); sys.exit(1)

file_path = sys.argv[1]
target = sys.argv[2]
features = [s for s in sys.argv[3].split(",") if s]

df = pd.read_csv(file_path)
data = df[features + [target]].dropna()

if data.empty:
    print(json.dumps({"error":"no rows after dropna on features/target"})); sys.exit(1)

X = data[features].values
y = data[target].values

model = LinearRegression()
model.fit(X, y)
pred = model.predict(X)

out = {
    "coef": { f: float(w) for f, w in zip(features, model.coef_) },
    "intercept": float(model.intercept_),
    "r2": float(r2_score(y, pred)),
    "n": int(len(data))
}
print(json.dumps(out, ensure_ascii=False))
