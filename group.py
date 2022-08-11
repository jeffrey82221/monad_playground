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
import inspect
import traceback
import logging
import pprint

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
        def wrapper_of_bind(*args, **kwargs):
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
        return wrapper_of_bind
    
    @abc.abstractmethod
    def decorator(self, orig_func):
        """
        The decorator that bind into the function
        
        Example: 
        
        return self.a_decorator(self.b_decorator(orig_func))
        """
        return self.carbon_tracing(self.logging(orig_func))
    
    def carbon_tracing(self, orig_func):
        '''decorator for tracing the carbon footprint and timing'''
        from codecarbon import OfflineEmissionsTracker
        emission_tracker = OfflineEmissionsTracker(
            country_iso_code="TWN",
            measure_power_secs=30, # frequency of making a probe
            tracking_mode="machine", # machine or process
        )
        @wraps(orig_func)
        def wrapper_of_carbon_tracing(*args, **kwargs):
            emission_tracker.start()
            out = orig_func(*args, **kwargs)
            emission_tracker.stop()
            emission_summary = emission_tracker.final_emissions_data
            pprint.pprint(dict(emission_summary.values))
            return out
        return wrapper_of_carbon_tracing
    
    def logging(self, orig_func):
        '''decorator for saving input, output & elapased time of a function'''
        @wraps(orig_func)
        def wrapper_of_logging(*args, **kwargs):
            """
            function warped by warp_log
            """
            filename_with_path = inspect.getfile(orig_func)
            try:
                out = orig_func(*args, **kwargs)
                success = True
            except BaseException as e:
                exception = e
                error_traceback = str(traceback.format_exc())
                success = False
            cols_in_args = [', '.join(a.columns) for a in args]
            cols_in_kwargs = [key + ': ' + ', '.join(value.columns) for key, value in kwargs.items()]
            table_size_in_args = [', '.join(str(len(a))) for a in args]
            table_size_in_kwargs = [key + ': ' + ', '.join(str(len(value))) for key, value in kwargs.items()]
            if success:
                if isinstance(out, list) or isinstance(out, tuple):
                    cols_in_out = [', '.join(o.columns) for o in out]
                    table_size_in_out = [', '.join(str(len(o))) for o in out]
                else:
                    cols_in_out = ', '.join(out.columns)
                    table_size_in_out = ', '.join(str(len(out)))
                logs = {
                    'success': success,
                    'in': {
                        'args': {
                            'cols': cols_in_args,
                            'table_size': table_size_in_args
                        },
                        'kargs':{
                            'cols': cols_in_kwargs,
                            'table_size': table_size_in_kwargs
                        }
                    },
                    'out': {
                        'cols': cols_in_out,
                        'table_size': table_size_in_out
                    },
                    'func': orig_func.__name__,
                    'module': filename_with_path
                }
            else:
                logs = {
                    'success': success,
                    'in': {
                        'args_cols': cols_in_args,
                        'kwargs_cols': cols_in_kwargs,
                        'args_table_size': table_size_in_args,
                        'kwargs_table_size': table_size_in_kwargs
                    },
                    'exception': str(exception),
                    'traceback': error_traceback,
                    'func': orig_func.__name__,
                    'module': filename_with_path
                }
            # simulate the logging behavior
            print({'saved_log': logs})
            if success:
                return out
            else:
                raise exception
        return wrapper_of_logging
    
