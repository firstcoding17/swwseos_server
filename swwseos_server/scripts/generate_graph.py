import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from io import BytesIO
sys.stdout.reconfigure(encoding='utf-8')
print("✅ Python 스크립트 실행됨!")  # ✅ 로그 추가

try:
    data_json = sys.argv[1]
    x_column = sys.argv[2]
    y_column = sys.argv[3]

    print(f"📂 X: {x_column}, Y: {y_column}")  # ✅ 로그 추가

    data = json.loads(data_json)
    df = pd.DataFrame(data['rows'], columns=data['columns'])

    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x=x_column, y=y_column)
    plt.title(f"{x_column} vs {y_column}")
    plt.xlabel(x_column)
    plt.ylabel(y_column)

    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    sys.stdout.buffer.write(buffer.read())

except Exception as e:
    print(f"❌ 오류 발생: {e}")
    sys.exit(1)