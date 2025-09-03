import sys
import json

# Node.js에서 전달받은 인자를 처리
args = sys.argv[1:]

# 데이터 처리 예제
result = {
    "message": "Python executed successfully!",
    "received_args": args,
    "sum": sum(map(int, args)) if args else 0
}

# 결과 반환
print(json.dumps(result))
