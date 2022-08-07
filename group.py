"""
TODO: 
- [ ] Create TableUnit 
- [ ] Add inputs and outputs properties 
- [ ] Turn off `self.__parse_main_func` in monad if dag == None
- [ ] Turn off `self.__parse_main_func` in monad if non-pd.DataFrame is included in the inputs / outputs of run
- [ ] Add `execute` method that read data from TableUnit(s) of input, passing it to `run`, and stored the result into TableUnit(s) of output
    - [ ] May have different behaviors dependent on the types: (pd.DataFrame, TableUnit, CSVGenerator, DataFrameGenerator) 
- [ ] Add `build_task` method where an airflow task is build and returned (called only if dag is not None) 
    - [ ] if decompose=False or non-pd.DataFrame is included in the inputs / outputs of run
        - [ ] A PythonOperator task calling `execute` should be contained 
        - [ ] add `check`, to the front & end of `execute` task
    - [ ] if all are pd.DataFrame & decompose = True
        - [ ] connect `check` & `read` & `write` tasks to the front & end of the task group. 
"""
import abc
from functools import wraps
from monad import Monad
import pandas as pd
import uuid
import typing 

class GroupMonad(Monad):
    def __init__(self, name, dag=None):        
        super().__init__()
        # Initialize group task object here
        if dag is not None:
            from airflow.utils.task_group import TaskGroup
            self.task_group = TaskGroup(group_id=f'{self.__class__.__name__}_{name}', dag=dag)
        self.__dag = dag
        self.__bind_index = 0
    
    @abc.abstractmethod
    def run(self, *args):
        """
        The main function to be altered by monad

        Originally: 

        a = self.func_1(b, c)
        d = self.func_2(a)
        return a, d

        Becomes:

        a = self.bind(self.func_1)(b, c)
        d = self.bind(self.func_2)(a)
        return a, d
        """
        raise NotImplementedError()

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
            if isinstance(orig_func.__annotations__['return'], typing._GenericAlias):
                return_cnt = len(orig_func.__annotations__['return'].__args__)
            elif isinstance(orig_func.__annotations__['return'], type):
                return_cnt = 1
            else:
                return_cnt = 0
            assert return_cnt > 0, 'Output should not be empty'
            if return_cnt > 1:
                for i in range(return_cnt):
                    assert orig_func.__annotations__['return'].__args__[i] == pd.DataFrame, f'Output {i} is not pd.DataFrame'
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
    
    
