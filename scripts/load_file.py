
import sys
import pandas as pd
import json


print("✅ Python 스크립트 실행됨!")
# ✅ 파일 경로 확인
if len(sys.argv) < 2:
    print(json.dumps({"error": "❌ Error: 파일 경로가 전달되지 않았습니다."}))
    sys.exit(1)
file_path = sys.argv[1]
print(f"📂 파일 경로: {file_path}")  # ✅ 파일 경로 출력


try:
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path, delimiter=';')  # ✅ CSV 읽기
    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("지원하지 않는 파일 형식")

    data = {
        "columns": df.columns.tolist(),
        "rows": df.fillna("").values.tolist(),
    }

    print(json.dumps(data))  # ✅ JSON 반환
except Exception as e:
    print(json.dumps({"error": str(e)}))  # ✅ 오류 메시지 출력
    sys.exit(1)
