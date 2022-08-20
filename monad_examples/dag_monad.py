"""
A monad design pattern 
that enable GroupMonad to be connected as DagMonad

TODO:
- [ ] Allow self.operations['a_func'](*args, **kwargs) to be decorated into 
    self.binded(self.operations['a_func'])(*args, **kwargs)
    (same to `self.decorator`)
- [ ] Define a __call__(self) method in the `Group` class to turn its instance
    into a callable function. 
    - [ ] Allow __call__ to be runnable on aicloud 
        - [ ] Takes TableUnit(s) as input and output TableUnit(s)
        - [ ] Assert the *args (input table units) are the same as 
            those defined in self.input_tables 
        - [ ] Returning self.output_tables
        - [ ] In the middle, do self.execute()
- [ ] Implement self.bind method for DAGMonad (allow run to be binded to airflow Dag)
    - [ ] (outside) Design ReturnObj for DAGMonad: 
        - [ ] It spill out dag_task so that they can be connected to the next task
        - [ ] It has `table_unit` property & `set_table_unit` method
    - [ ] (outside) `build` method of `Group` monad should takes tasks as input to be warpped into the input_objs
    - [ ] (inside) Step 1: Assert that the TableUnit(s) in the "input_objs" (ReturnObjs) is the same as self.input_tables 
    - [ ] (inside) Step 2: Send the tasks in the "input_objs" to `build` method 
    - [ ] (inside) Step 3: Take the output tasks returns from `build` and wrap them into ReturnObj(s) 
    - [ ] (inside) Step 4: Assert the count of output tasks is the same as that of self.output_tables
    - [ ] (inside) Step 5: Add self.output_tables into the ReturnObj(s) by its `set_table_unit` method
"""
import pandas as pd
import uuid
import inspect 
import traceback
import time
import setting
from functools import wraps
from monad import Monad


class TestClass:
    def __init__(self):
        pass
    def run(self, a, b):
        return a + b
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

class TestClass2:
    def __init__(self):
        pass
    def run(self, a, b):
        return a * b
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

class DagMonad(Monad):
    @property
    def operations(self):
        return {
            'a_func': TestClass(),
            'b_func': TestClass2(),
        }
    
    @property
    def a_func(self):
        return self.operations['a_func']

    @property
    def b_func(self):
        return self.operations['b_func']

    def run(self, a, b):
        c = self.a_func(a, b)
        d = self.b_func(a, b)
        return c, d

    def decorator(self, orig_func):
        """The decorator that bind into the function"""
        f = self.__pipe_connect_decoration(orig_func)
        f = self.__logging_decoration(f)
        return f

    def __pipe_connect_decoration(self, orig_func):
        @wraps(orig_func)
        def pipe_connect_wrapper(*args, **kwargs):
            return orig_func(*args, **kwargs)
        return pipe_connect_wrapper

    def __logging_decoration(self, orig_func):
        '''decorator for saving input, output & elapased time of a function'''
        @wraps(orig_func)
        def logging_wrapper(*args, **kwargs):
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
            if success:
                logs = {
                    'success': success,
                    'in': {
                        'args': str(args),
                        'kwargs': str(kwargs)
                    },
                    'out': str(out),
                    'time': time_elapsed,
                    'func': orig_func.__name__,
                    'module': filename_with_path
                }
            else:
                logs = {
                    'success': success,
                    'in': {
                        'args': str(args),
                        'kwargs': str(kwargs)
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
        return logging_wrapper


if __name__ == '__main__':
    dag_monad = DagMonad()
    ans = dag_monad.run(1, 2)
    print('ans:', ans)