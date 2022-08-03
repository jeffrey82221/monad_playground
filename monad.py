import abc
from functools import wraps


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
        return orig_func
        
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

class DagMonad(Monad):
    def __init__(self):
        # Initialize group task object here
        pass
    @property
    @abc.abstractmethod
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content and a dag task property must be included in the return object
        """
        class ReturnObj:
            def __init__(self, content):
                self.content = content
            def set_dag_task(self, dag_task):
                self.dag_task = dag_task

        return ReturnObj
        
    def bind(self, orig_func):
        '''decorator for allowing a function to support monad design pattern'''
        @wraps(orig_func)
        def wrapper(*args, **kwargs):
            """
            function warped by bind
            """
            def python_func():
                # extract content from the return object
                args = [a.content for a in args] 
                kargs = dict([(key, value.content) for key, value in kwargs.items()])
                # adopt the original function to content
                result = self.decorator(orig_func)(*args, **kargs)
                # encapsulate content into the return object 
                if isinstance(result, list):
                    result = [self.return_cls(x) for x in result]
                elif isinstance(result, tuple):
                    result = tuple([self.return_cls(x) for x in result])
                else:
                    result = self.return_cls(result)
            # NOTE: try to come up with two decorator
            # one conduct real operation, one conduct no operation  
            # 1. create task here 
            # 2. add task to group here 
            # 3. connect task to previous tasks here 
            # 4. add current task to the return object for later use 

        return wrapper