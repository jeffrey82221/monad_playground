import abc
from functools import wraps
from monad import Monad
import pandas as pd
import uuid

class DagMonad(Monad):
    def __init__(self, name, dag=None):        
        super().__init__()
        # Initialize group task object here
        if dag is not None:
            from airflow.utils.task_group import TaskGroup
            self.task_group = TaskGroup(group_id=f'{self.__class__.__name__}_{name}', dag=dag)
        self.__dag = dag
        self.__bind_index = 0
        
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
                self.dag_task = None
            @property
            def content(self):
                print(f'load from {self.file_dir}')
                # remove parquet
                return pd.read_parquet(self.file_dir)
            
            def set_dag_task(self, dag_task):
                self.dag_task = dag_task
                
            def set_content(self, content):
                content.to_parquet(self.file_dir)
                print(f'save into {self.file_dir}')
            
        return ReturnObj
        
    def bind(self, orig_func):
        '''Binding pandas operations in `run` task to a airflow task_group'''
        self.__bind_index += 1
        @wraps(orig_func)
        def wrapper(*args, **kwargs):
            """
            function warped by bind
            """
            if isinstance(orig_func.__annotations__['return'], tuple):
                return_cnt = len(orig_func.__annotations__['return'])
            elif isinstance(orig_func.__annotations__['return'], type):
                return_cnt = 1
            else:
                return_cnt = 0
            assert return_cnt > 0, 'Output should not be empty'
            if return_cnt > 1:
                for i in range(return_cnt):
                    assert orig_func.__annotations__['return'][i] == pd.DataFrame, f'Output {i} is not pd.DataFrame'
                else:
                    assert orig_func.__annotations__['return'] == pd.DataFrame, 'Output is not pd.DataFrame'
                    
            return_objs = [self.return_cls() for i in range(return_cnt)]
            assert len(args) + 1 == len(orig_func.__annotations__.keys()), 'Please make sure input count is consistent'
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
                        return_obj.set_content(result)
                else:
                    assert isinstance(results, pd.DataFrame), 'Please make sure all outputs are pd.DataFrame'
                    return_objs[0].set_content(results)
            
            # 1. create task here
            from airflow.operators.python import PythonOperator
            with self.task_group:
                task = PythonOperator(
                    task_id=f'{orig_func.__name__}_{self.__bind_index}',
                    python_callable=python_func,
                    dag=self.__dag
                )
            # 3. connect task to previous tasks here
            for a in args:
                if a.dag_task is not None:
                    task.set_upstream(a.dag_task)
            # 4. add current task to the return object for later use
            for r in return_objs:
                r.set_dag_task(task)
            # 5. make sure return_objs is correctly return
            if len(return_objs) == 1:
                return return_objs[0]
            else:
                return return_objs
        return wrapper
    
    @abc.abstractmethod
    def decorator(self, orig_func):
        """The decorator that bind into the function"""
        return orig_func
    
    
class PDProcess(DagMonad):
    def run(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df_big = self.append_df(df1, df2)
        df_reset = self.reset_index(df_big)
        return df_reset

    def append_df(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        result = pd.concat([df1, df2])
        return result

    def reset_index(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.reset_index()
    
def create_dummy_df():
    df = pd.DataFrame(columns = ['Name', 'Articles', 'Improved'])
    df = df.append({'Name' : 'Ankit', 'Articles' : 97, 'Improved' : 2200},
            ignore_index = True)

    df = df.append({'Name' : 'Aishwary', 'Articles' : 30, 'Improved' : 50},
            ignore_index = True)

    df = df.append({'Name' : 'yash', 'Articles' : 17, 'Improved' : 220},
          ignore_index = True)
    return df


if __name__ == '__main__':
    df = create_dummy_df()
    from datetime import datetime, timedelta
    from airflow.models import DAG
    default_args = {
        'owner': 'esb21375',                     # 請填寫 PM 的員編
        'start_date': datetime(2021, 9, 1),      # 盡量使用絕對時間
        'retries': 1,                            # 失敗 retry 多少次
        'retry_delay': timedelta(minutes=1),     # 失敗後間隔多久 retry
        'email': 'test@esunbank.com.tw',         # 至少填一個
        'execution_timeout': timedelta(hours=3)
    }

    dag = DAG(
        dag_id='demo',
        schedule_interval=None,
        catchup=False,                           # 當 backfill 時只執行最近一次
        max_active_runs=1,                       # DAG 最大同時執行數
        default_args=default_args,               # task level 參數
        description='if_polaris demo DAG，示範將顧客明細轉為顧客特徵 ETL DAG',  # 詳細描述這個 DAG 在做什麼
        access_control={
            'esun_default': {
                'can_read'
                # 'can_edit'
            }
        }
    )
    process = PDProcess('process', dag=dag)
    print(process.run.__annotations__)
    return_obj = process.return_cls()
    return_obj.set_content(df)
    result = process.target_main_func(return_obj, return_obj)
    print(result)
    print(process.run(df, df))