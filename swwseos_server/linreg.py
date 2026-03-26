import sys
import json

# Handle arguments passed from Node.js.
args = sys.argv[1:]

# Example data processing
result = {
    "message": "Python executed successfully!",
    "received_args": args,
    "sum": sum(map(int, args)) if args else 0
}

# Return result
print(json.dumps(result))
