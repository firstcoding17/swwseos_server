import sys, json, pandas as pd, numpy as np

if len(sys.argv) < 3:
    print(json.dumps({"error": "usage: correlation.py <filePath> <columnOrAll>"})); sys.exit(1)

file_path = sys.argv[1]
# 두 번째 인자는 안 써도 되게 해두지만, 프론트 구조 유지용으로 받음
_ = sys.argv[2]

df = pd.read_csv(file_path)
num_df = df.select_dtypes(include=[np.number])
corr = num_df.corr(method="pearson").round(6).fillna(0).to_dict()
print(json.dumps({"columns": list(num_df.columns), "corr": corr}, ensure_ascii=False))
