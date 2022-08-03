import abc
from functools import wraps
from monad import Monad
import pandas as pd

class DagMonad(Monad):
    def __init__(self):
        # Initialize group task object here
        super().__init__()
    
    @property
    @abc.abstractmethod
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content and a dag task property must be included in the return object
        """
        class ReturnObj:
            def __init__(self):
                uuid_str = str(uuid.uuid4())
                self.file_dir = f'tmp/{uuid_str}.parquet'
                
            def set_dag_task(self, dag_task):
                self.dag_task = dag_task
                
            def save_content(self, content):
                content.to_parquet(self.file_dir)
                print(f'save into {self.file_dir}')
                
            def get_content(self):
                print(f'load from {self.file_dir}')
                # remove parquet
                return pd.read_parquet(self.file_dir)
            
        return ReturnObj
        
    def bind(self, orig_func):
        '''decorator for allowing a function to support monad design pattern'''
        @wraps(orig_func)
        def wrapper(*args, **kwargs):
            """
            function warped by bind
            """
            return_cnt = len(orig_func.__annotation__['return'])
            assert return_cnt > 0, 'Output should not be empty'
            for i in range(return_cnt):
                assert orig_func.__annotation__['return'][i] == pd.DataFrame, f'Output {i} is not pd.DataFrame'
            return_objs = [self.return_cls() for i in range(return_cnt)]
            assert len(args) + 1 == len(orig_func.__annotation__.keys()), 'Please make sure input count is consistent'
            def python_func():
                # extract content from the return object
                args = [a.content for a in args] 
                kargs = dict([(key, value.content) for key, value in kwargs.items()])
                # adopt the original function to content
                results = self.decorator(orig_func)(*args, **kargs)
                # encapsulate content into the return object 
                if isinstance(result, list) or isinstance(result, tuple):
                    assert len(result) == return_cnt, 'Please make sure output count is consistent'
                    for return_obj, result in zip(return_objs, results):
                        return_obj.save_content(result)
                else:
                    assert isinstance(results, pd.DataFrame), 'Please make sure all outputs are pd.DataFrame'
                    return_objs[0].save_content(results)
            
            # TODO:
            # 1. create task here
            # 2. add task to group here
            # 3. connect task to previous tasks here
            # 4. add current task to the return object for later use

        return wrapper
    
    @abc.abstractmethod
    def decorator(self, orig_func):
        """The decorator that bind into the function"""
        return orig_func
    
    
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
    result = process.target_main_func(process.return_cls(df), process.return_cls(df))
    print(result)
    print(result.content)