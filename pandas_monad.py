"""
A monad design pattern 
that decorate the entire pipeline 
such that all functions within it are 
log enabled
"""
import pandas as pd
from monad import Monad
import uuid

def create_dummy_df():
    df = pd.DataFrame(columns = ['Name', 'Articles', 'Improved'])
    df = df.append({'Name' : 'Ankit', 'Articles' : 97, 'Improved' : 2200},
            ignore_index = True)

    df = df.append({'Name' : 'Aishwary', 'Articles' : 30, 'Improved' : 50},
            ignore_index = True)

    df = df.append({'Name' : 'yash', 'Articles' : 17, 'Improved' : 220},
          ignore_index = True)
    return df

class PandasMonad(Monad):
    @property
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content property must be included in the return object
        """
        class ReturnObj:
            def __init__(self, content):
                self.__uuid_str = str(uuid.uuid4())
                content.to_parquet(f'tmp/{self.__uuid_str}.parquet')
                print('save to parquet')
            
            @property
            def content(self):
                print('read from parquet')
                # remove parquet
                return pd.read_parquet(f'tmp/{self.__uuid_str}.parquet')
            
        return ReturnObj
    

class PDProcess(PandasMonad):
    def run(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df_big = self.append_df(df1, df2)
        df_reset = self.reset_index(df_big)
        return df_reset

    def append_df(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        result = pd.concat([df1, df2])
        return result

    def reset_index(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.reset_index()
    
process = PDProcess()

if __name__ == '__main__':
    df = create_dummy_df()
    # process.return_cls(df), process.return_cls(df)
    result = process.target_main_func(process.return_cls(df), process.return_cls(df))
    print(result)
    print(result.content)