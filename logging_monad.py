"""
A monad design pattern 
that decorate the entire pipeline 
such that all functions within it are 
log enabled
"""
import inspect
import time
import traceback
from functools import wraps
import logging
VERBOSE = False


def wrap_log(orig_func):
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
    return wrapper

class ReturnMonad:
    def __init__(self, content):
        self.content = content
        
    
def bind(orig_func):
    '''decorator for allowing a function to support monad design pattern'''
    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        """
        function warped by bind
        """
        # extract content from monad
        args = [a.content for a in args] 
        kargs = dict([(key, value.content) for key, value in kwargs.items()])
        # adopt the original function to content
        result = orig_func(*args, **kargs)
        # encapsulate content into monad 
        if isinstance(result, list):
            return [ReturnMonad(x) for x in result]
        elif isinstance(result, tuple):
            return tuple([ReturnMonad(x) for x in result])
        else:
            return ReturnMonad(result)
    return wrapper
    

def sub_func_1_plus(a, b):
    return a + b

def sub_func_2_prod(a, b):
    return a * b

def main_func(a, b):
    p1 = bind(wrap_log(sub_func_1_plus))(a, b)
    p2 = bind(wrap_log(sub_func_2_prod))(a, b)
    return p1, p2


if __name__ == '__main__':
    c = main_func(ReturnMonad(1), ReturnMonad(2))
    print(c)
    print(c[0].content)
    print(c[1].content)
