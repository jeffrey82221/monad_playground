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


def sub_func_1_plus(a, b):
    return a + b

def sub_func_2_prod(a, b):
    return a * b

def main_func(a, b):
    p1 = wrap_log(sub_func_1_plus)(a, b)
    p2 = wrap_log(sub_func_2_prod)(a, b)
    return p1, p2


if __name__ == '__main__':
    c = main_func(1, 2)
    print(c)
