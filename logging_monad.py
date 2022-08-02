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
import abc
VERBOSE = False


class Monad:
    @property
    @abc.abstractmethod
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content property must be included in the return object
        """
        class ReturnObj:
            def __init__(self, content):
                self.content = content
        return ReturnObj
    
    @abc.abstractmethod
    def decorator(self, orig_func): 
        """The decorator that bind into the function"""
        raise NotImplementedError('decorator must be implement')
        
    def bind(self, orig_func):
        '''decorator for allowing a function to support monad design pattern'''
        @wraps(orig_func)
        def wrapper(*args, **kwargs):
            """
            function warped by bind
            """
            # extract content from the return object
            args = [a.content for a in args] 
            kargs = dict([(key, value.content) for key, value in kwargs.items()])
            # adopt the original function to content
            result = self.decorator(orig_func)(*args, **kargs)
            # encapsulate content into the return object 
            if isinstance(result, list):
                return [self.return_cls(x) for x in result]
            elif isinstance(result, tuple):
                return tuple([self.return_cls(x) for x in result])
            else:
                return self.return_cls(result)
        return wrapper

class LoggingMonad(Monad):
    @property
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content property must be included in the return object
        """
        class ReturnObj:
            def __init__(self, content):
                self.content = content
        return ReturnObj
    
    def decorator(self, orig_func): 
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

class CustomizeProcess(LoggingMonad):
    def main_func(self, a, b):
        p1 = self.bind(self.sub_func_1_plus)(a, b)
        p2 = self.bind(self.sub_func_2_prod)(a, b)
        return p1, p2

    def sub_func_1_plus(self, a, b):
        return a + b

    def sub_func_2_prod(self, a, b):
        return a * b

    
process = CustomizeProcess()

if __name__ == '__main__':
    c = process.main_func(process.return_cls(1), process.return_cls(2))
    print(c)
    print(c[0].content)
    print(c[1].content)
