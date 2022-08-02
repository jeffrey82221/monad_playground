"""
A monad design pattern 
that decorate the entire pipeline 
such that all functions within it are 
log enabled
"""
import pandas as pd
from monad import Monad
import uuid
from functools import wraps
import time
import inspect
import traceback


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
            def __init__(self, content: pd.DataFrame):
                self.__uuid_str = str(uuid.uuid4())
                self.__file_dir = f'tmp/{self.__uuid_str}.parquet'
                content.to_parquet(self.__file_dir)
                print(f'save into {self.__file_dir}')
            
            @property
            def content(self) -> pd.DataFrame:
                print(f'load from {self.__file_dir}')
                # remove parquet
                return pd.read_parquet(f'tmp/{self.__uuid_str}.parquet')
            
        return ReturnObj

    def decorator(self, orig_func):
        return self.log_wrapping(orig_func)

    def log_wrapping(self, orig_func): 
        '''decorator for saving input, output & elapased time of a function'''
        @wraps(orig_func)
        def wrapper(*args, **kwargs):
            """
            function warped by warp_log
            """
            filename_with_path = inspect.getfile(orig_func)
            time_start = time.time()
            try:
                out = orig_func(*args, **kwargs)
                success = True
            except BaseException as e:
                exception = e
                error_traceback = str(traceback.format_exc())
                success = False
            time_elapsed = time.time() - time_start
            cols_in_args = [','.join(a.columns) for a in args]
            cols_in_kwargs = [key + ':' + ','.join(value.columns) for key, value in kwargs.items()]
            table_size_in_args = [','.join(str(len(a))) for a in args]
            table_size_in_kwargs = [key + ':' + ','.join(str(len(value))) for key, value in kwargs.items()]
            if success:
                if isinstance(out, list) or isinstance(out, tuple):
                    cols_in_out = [','.join(o.columns) for o in out]
                    table_size_in_out = [','.join(str(len(o))) for o in out]
                else:
                    cols_in_out = ','.join(out.columns)
                    table_size_in_out = ','.join(str(len(out)))
                logs = {
                    'success': success,
                    'in': {
                        'cols_in_args': cols_in_args,
                        'cols_in_kwargs': cols_in_kwargs,
                        'table_size_in_args': table_size_in_args,
                        'table_size_in_kwargs': table_size_in_kwargs
                    },
                    'out': {
                        'cols_in_out': cols_in_out,
                        'table_size_in_out': table_size_in_out
                    },
                    'time': time_elapsed,
                    'func': orig_func.__name__,
                    'module': filename_with_path
                }
            else:
                logs = {
                    'success': success,
                    'in': {
                        'cols_in_args': cols_in_args,
                        'cols_in_kwargs': cols_in_kwargs,
                        'table_size_in_args': table_size_in_args,
                        'table_size_in_kwargs': table_size_in_kwargs
                    },
                    'exception': str(exception),
                    'traceback': error_traceback,
                    'time': time_elapsed,
                    'func': orig_func.__name__,
                    'module': filename_with_path
                }
            # simulate the logging behavior
            print({'saved_log': logs})
            if success:
                return out
            else:
                raise exception
        return wrapper
    

class PDProcess(PandasMonad):
    def main(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df_big = self.bind(self.append_df)(self.return_cls(df), self.return_cls(df))
        df_reset = self.bind(self.reset_index)(df_big)
        return df_reset

    def append_df(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        result = pd.concat([df1, df2])
        return result

    def reset_index(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.reset_index()
    
process = PDProcess()

if __name__ == '__main__':
    df = create_dummy_df()
    result = process.main(df, df)
    print(result)
    print(result.content)