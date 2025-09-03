import sys
import json
import numpy as np
import pandas as pd

def main(args):
    numbers = list(map(int, args))
    # NumPy를 사용한 계산
    mean = np.mean(numbers)
    std_dev = np.std(numbers)
    # Pandas 데이터프레임 생성
    df = pd.DataFrame({'Numbers': numbers})

    result = {
        'mean': mean,
        'std_dev': std_dev,
        'dataframe': df.to_dict()  # Pandas DataFrame을 JSON으로 변환
    }
    print(json.dumps(result))

if __name__ == '__main__':
    main(sys.argv[1:])
