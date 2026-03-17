import sys
import json
import numpy as np
import pandas as pd

def main(args):
    numbers = list(map(int, args))
    # Compute stats using NumPy
    mean = np.mean(numbers)
    std_dev = np.std(numbers)
    # Build a DataFrame with Pandas
    df = pd.DataFrame({'Numbers': numbers})

    result = {
        'mean': mean,
        'std_dev': std_dev,
        'dataframe': df.to_dict()  # Convert DataFrame to JSON-serializable dict
    }
    print(json.dumps(result))

if __name__ == '__main__':
    main(sys.argv[1:])
