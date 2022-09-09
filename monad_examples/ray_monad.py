"""
A monad design pattern
that decorate the entire pipeline
such that all functions within it are
log enabled
"""
import ray
from ray.serve.deployment_graph import InputNode
from functools import wraps
import pandas as pd
import uuid
import setting
from monad import Monad


class RayMonad(Monad):
    @property
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content property must be included in the return object
        """
        class ReturnObj:
            def __init__(self, content):
                self.__content = content
                print('save to parquet')

            @property
            def content(self):
                print('read from parquet')
                # remove parquet
                return self.__content

        return ReturnObj
    def bind(self, orig_func):
        '''decorator to be bind to the `run` function, designed in monad pattern'''
        @wraps(orig_func)
        def wrapper_of_bind(*args, **kwargs):
            """
            function warped by bind
            """
            # extract content from the return object
            args = [a.content for a in args]
            kargs = dict([(key, value.content)
                         for key, value in kwargs.items()])
            # adopt the original function to content
            result = self.decorator(orig_func)(*args, **kargs)
            # encapsulate content into the return object
            if isinstance(result, list):
                return [self.return_cls(x) for x in result]
            elif isinstance(result, tuple):
                return tuple([self.return_cls(x) for x in result])
            else:
                return self.return_cls(result)
        return wrapper_of_bind

class PDProcess(RayMonad):
    def run(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df_big = self.append_df(df1, df2)
        df_reset = self.reset_index(df_big)
        return df_reset

    def append_df(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        result = pd.concat([df1, df2])
        return result

    def reset_index(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.reset_index()

ray.init()
process = PDProcess()
def create_dummy_df():
    df = pd.DataFrame(columns=['Name', 'Articles', 'Improved'])
    df = df.append({'Name': 'Ankit', 'Articles': 97, 'Improved': 2200},
                   ignore_index=True)

    df = df.append({'Name': 'Aishwary', 'Articles': 30, 'Improved': 50},
                   ignore_index=True)

    df = df.append({'Name': 'yash', 'Articles': 17, 'Improved': 220},
                   ignore_index=True)
    return df
if __name__ == '__main__':
    df = create_dummy_df()
    process.return_cls(df)
    # process.return_cls(df), process.return_cls(df)
    result = process.binded_run(process.return_cls(df), process.return_cls(df))
    print(result)
    print(result.content)
