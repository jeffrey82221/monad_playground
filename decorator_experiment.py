import inspect 
from functools import wraps 
def bind(orig_func):
    '''decorator for allowing a function to support monad design pattern'''
    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        """
        function warped by bind
        """
        result = orig_func(*args, **kwargs)
        return result
    return wrapper


def plus(a: int, b: int) -> (int, int):
    return a + b, b

print(plus.__annotations__.items())
@bind
def plus(a: int, b: int) -> (int, int):
    return a + b, b

print(plus.__annotations__.items())

print(plus(1, 2))


print(plus.__annotations__['return'])

print(len(plus.__annotations__['return']))
    
        