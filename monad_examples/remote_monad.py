"""
A monad design pattern
that decorate the entire pipeline
such that all functions within it are
execute in a remote server (colab or mac)

TODO:
- [X] Build api that takes input and output pickle paths, and a function and do the following:
    - [X] extract elements from pickles on input paths
    - [X] send the elements to the function
    - [X] get the output from function
    - [X] save the output as pickle to the path!
    - [X] The API takes new input:
        - {
            'python_func': python_func,
            'args_paths': input_paths,
            'kwargs_paths': kwargs_paths,
            'output_paths': output_paths,
            'is_class_method': True/False
        }
- [ ] Move here the Monad class, which automatically decorate `run` method as `binded_run`
- [ ] Build a decorator (bind) that takes a function as input, and adapt it to the remote
API
    - [ ] It determine python_func via the source code of the func
    - [ ] It determine args_paths & kwargs_paths via get_path of the args & kwargs ReturnObj
    - [ ] It determine output_paths via the pre-build ReturnObjs' get_path
        - [ ] ReturnObjs of output is build according to the Return __annotations__ of the func
"""

from functools import wraps
import inspect
from textwrap import dedent
import uuid
from typing import Tuple
import setting
from monad import Monad
from monad_examples.remote_client.etl_api_caller import call_etl_api


class ReturnObj:
    """
    Representing a pickled python object on /tmp folder at remote executor
    """

    def __init__(self):
        self.file_name = str(uuid.uuid4())


def remote_adapt(orig_func):
    @wraps(orig_func)
    def wrapper(*args: ReturnObj, **kwargs: ReturnObj):
        input_info = inspect.getfullargspec(orig_func)
        if len(input_info.args) and input_info.args[0] == 'self':
            class_func = True
            args = args[1:]
        else:
            class_func = False
        # extract args file names from args and kwargs ReturnObj
        args_file_name = [a.file_name for a in args]
        kwargs_tuples = []
        for item in kwargs.items():
            if not isinstance(item, Returnobj):
                kwargs_tuples.append((item[0], 'default'))
            else:
                kwargs_tuples.append((item[0], item[1].file_name))
        # Construct new ReturnObjs according to the Return annotation of
        # orig_func
        if str(orig_func.__annotations__['return'])[:12] == 'typing.Tuple':
            # FIXME: there might be a problem for a nested Tuple
            return_cnt = len(str(orig_func.__annotations__['return']).replace(
                'typing.Tuple', '').split(','))
        elif orig_func.__annotations__['return'] is None:
            return_cnt = 0
        else:
            return_cnt = 1

        outputs = [ReturnObj() for i in range(return_cnt)]
        output_file_names = [o.file_name for o in outputs]
        call_etl_api(
            class_func,
            orig_func,
            args_file_name,
            dict(kwargs_tuples),
            output_file_names
        )
        return outputs

        # return the Output return_objs

    return wrapper


def create_values() -> Tuple[int, float]:
    return 1, 0.5


create_values = remote_adapt(create_values)
if __name__ == '__main__':
    ans = create_values()
    print('ans:', ans)
