import sys
import pandas as pd
import json

# Force UTF-8 output on Windows terminals.
sys.stdout.reconfigure(encoding='utf-8')

if len(sys.argv) < 2:
    print(json.dumps({"error": "File path was not provided."}))
    sys.exit(1)

file_path = sys.argv[1]

try:
    df = pd.read_csv(file_path)
    
    # Return normalized JSON payload.
    data = {
        "columns": df.columns.tolist(),
        "rows": df.fillna("").values.tolist(),
    }
    
    print(json.dumps(data))

except Exception as e:
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
