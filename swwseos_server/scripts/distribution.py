import sys, json, pandas as pd, os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime

if len(sys.argv) < 3:
    print(json.dumps({"error": "usage: distribution.py <filePath> <column>"})); sys.exit(1)

file_path = sys.argv[1]
column = sys.argv[2]

df = pd.read_csv(file_path)
col = df[column].dropna()

os.makedirs(os.path.join(os.path.dirname(__file__), '..', 'outputs'), exist_ok=True)
fname = f"hist_{column}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
out_dir = os.path.join(os.path.dirname(__file__), '..', 'outputs')
out_path = os.path.join(out_dir, fname)

plt.figure()
plt.hist(col, bins=30)
plt.title(f"Distribution of {column}")
plt.xlabel(column)
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig(out_path)
plt.close()

print(json.dumps({"image": fname}, ensure_ascii=False))
