import inspect
from typing import Tuple
import typing
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


def plus(a: int, b: int) -> int:
    return a + b


print(plus.__annotations__['return'])
print(type(plus.__annotations__['return']))


@bind
def plus(a: int, b: int) -> Tuple[int, int, int]:
    return a + b, b, b


print(plus.__annotations__.items())

print(plus(1, 2))

print(type(plus.__annotations__['return']))
print(isinstance(plus.__annotations__['return'], typing._GenericAlias))

print(plus.__annotations__['return'])

print(plus.__annotations__['return'].__args__)
