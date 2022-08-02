"""
A monad design pattern 
that decorate the entire pipeline 
such that all functions within it are 
log enabled
"""
import pandas as pd
from monad import Monad

def create_dummy_df():
    df = pd.DataFrame(columns = ['Name', 'Articles', 'Improved'])
    df = df.append({'Name' : 'Ankit', 'Articles' : 97, 'Improved' : 2200},
            ignore_index = True)

    df = df.append({'Name' : 'Aishwary', 'Articles' : 30, 'Improved' : 50},
            ignore_index = True)

    df = df.append({'Name' : 'yash', 'Articles' : 17, 'Improved' : 220},
          ignore_index = True)
    return df

def append_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    result = pd.concat([df1, df2])
    return result

def reset_index(df: pd.DataFrame) -> pd.DataFrame:
    return df.reset_index()

if __name__ == '__main__':
    df = create_dummy_df()
    print(df)
    df_big = append_df(df, df)
    print(df_big)
    df_reset = reset_index(df_big)
    print(df_reset)