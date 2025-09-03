import sys
import pandas as pd
import json

# ✅ Windows에서 UTF-8 인코딩 강제 적용
sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print(json.dumps({"error": "파일 경로가 전달되지 않았습니다."}))
    sys.exit(1)

file_path = sys.argv[1]

try:
    df = pd.read_csv(file_path)
    
    # ✅ JSON 형식으로 변환하여 출력
    data = {
        "columns": df.columns.tolist(),
        "rows": df.fillna("").values.tolist(),
    }
    
    print(json.dumps(data))  # ✅ JSON 데이터만 출력

except Exception as e:
    print(json.dumps({"error": str(e)}))  # ✅ 오류 메시지도 JSON 형식으로 변환
    sys.exit(1)
