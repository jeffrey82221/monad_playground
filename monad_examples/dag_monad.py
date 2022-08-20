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
import inspect 
import traceback
import time
import abc
import setting
from functools import wraps
from monad import Monad

class NestedMonad(Monad):
    def __init__(self):
        super().__init__()
        self.__setup_operations()
    
    def __setup_operations(self):
        for func_name in self.operations.keys():
            func = self.operations[func_name]
            exec(f'self.{func_name} = func')

    @abc.abstractproperty
    def operations(self):
        raise NotImplementedError

    @abc.abstractmethod
    def run(self, *args, **kargs):
        raise NotImplementedError
        
    def decorator(self, orig_func):
        """The decorator that bind into the function"""
        if isinstance(orig_func, NestedMonad):
            f = self.__logging_decoration(orig_func.decorated_run)
        else:
            f = self.__logging_decoration(orig_func.run)
        return f

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

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)


class SimpleGroup1:
    def run(self, a, b):
        return a + b
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

class SimpleGroup2:
    def run(self, a, b):
        return a * b
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

class DagProcess(NestedMonad):

    @property
    def operations(self):
        return {
            'a_func': SimpleGroup1(),
            'b_func': SimpleGroup2(),
        }
    
    def run(self, a, b):
        c = self.a_func(a, b)
        d = self.b_func(a, b)
        return c, d

class TopProcess(NestedMonad):
        
        
    @property
    def operations(self):
        return {
            'dag_process': DagProcess()
        }
    
    def run(self, x, y, z, w):
        i1, i2 = self.dag_process(x, y)
        j1, j2 = self.dag_process(z, w)
        return i1, i2, j1, j2

class SecondTopProcess(NestedMonad):
    @property
    def operations(self):
        return {
            'top_process': TopProcess()
        }
    
    def run(self, x, y, z, w):
        a, b, c, d = self.top_process(x, y, z, w)
        return a, b, c, d

if __name__ == '__main__':
    dag_monad = DagProcess()
    ans = dag_monad.decorated_run(1, 2)
    print('dag_monad result:', ans)
    top_monad = TopProcess()
    ans = top_monad.decorated_run(1, 2, 3, 4)
    print('top_monad result:', ans)
    second_top_monad = SecondTopProcess()
    ans = second_top_monad.run(1, 2, 3, 4)
    print('second_top_monad result:', ans)