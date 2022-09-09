"""
A monad design pattern
that decorate the entire pipeline
such that all functions within it are
connect to a ray dag!

PROBLEMS:

- [ ] input_node represent only a single input
    - [ ] need to combine *args & **kargs into one input
    - [ ] need remote function that extract *args and **kargs and compose
        *args and **kargs of `ReturnClass` where the contents are the
        outputs of the extract method (bind).
    - [ ] in the wrapper of bind, extract content from *args and **kargs
        and send them to the bined-function (make sure to make the function a remote function)
    - [ ] extract output (which is a ray binded result) and dispatch it into
        multiple output according to the output type hint of the bined function.
"""
from typing import Optional, Tuple
import ray
from ray.serve.deployment_graph import InputNode
from functools import wraps
from ray.dag.function_node import FunctionNode
import inspect
from monad import Monad


class ReturnObj:
    def __init__(self, content: FunctionNode):
        self.__content = content

    @property
    def content(self) -> FunctionNode:
        return self.__content


@ray.remote
def dispatch_input(whole_input: dict,
                   args_ind: Optional[int] = None, kwargs_key: Optional[str] = None):
    assert args_ind is not None or kwargs_key is not None
    if args_ind is not None:
        return whole_input['args'][args_ind]
    elif kwargs_key is not None:
        return whole_input['kwargs'][kwargs_key]
    else:
        raise ValueError('either args_ind or kwargs_key should be probided')


@ray.remote
def dispatch_output(whole_output, index: int):
    return whole_output[index]


@ray.remote
def aggregate_output(*args):
    return args


class RayMonad(Monad):

    def __init__(self):
        super().__init__()

    @property
    def return_cls(self):
        """
        The Return object used in monad
        NOTE: a content property must be included in the return object
        """
        return ReturnObj

    def bind(self, orig_func):
        '''decorator to be bind to the `run` function, designed in monad pattern'''
        @wraps(orig_func)
        def wrapper_of_bind(*args, **kwargs):
            """
            function warped by bind
            """
            # extract content from the return object
            args = [a.content for a in args]
            kargs = dict([(key, value.content)
                         for key, value in kwargs.items()])
            # adopt the original function to content

            @ray.remote
            def remote_func(*args, **kargs):
                return orig_func(*args, **kargs)
            result_node = remote_func.bind(*args, **kargs)
            # encapsulate content into the return object
            return_str = str(orig_func.__annotations__['return'])
            if 'typing.Tuple' in return_str:
                returning_items = []
                return_cnt = len(
                    return_str.split('typing.Tuple')[1].split(','))
                for i in range(return_cnt):
                    returning_items.append(
                        dispatch_output.bind(result_node, i))
                return tuple([self.return_cls(x) for x in returning_items])
            else:
                return self.return_cls(result_node)
        return wrapper_of_bind

    def build(self):
        args_cnt = 0
        kwargs_cnt = 0
        kwargs_names = []
        for name, parameter in dict(
                inspect.signature(self.run).parameters).items():
            if parameter.default == inspect._empty:
                args_cnt += 1
            else:
                kwargs_cnt += 1
                kwargs_names.append(parameter.name)
        args = []
        kwargs = dict()
        with InputNode() as input_node:
            for i in range(args_cnt):
                arg = dispatch_input.bind(input_node, args_ind=i)
                args.append(self.return_cls(arg))
            for kwarg_name in kwargs_names:
                kwarg = dispatch_input.bind(input_node, kwargs_key=kwarg_name)
                kwargs[kwarg_name] = self.return_cls(kwarg)
            return_objs = self.binded_run(*args, **kwargs)
            if isinstance(return_objs, list):
                output_node = aggregate_output.bind(
                    *[o.content for o in return_objs])
            elif isinstance(return_objs, tuple):
                output_node = aggregate_output.bind(
                    *[o.content for o in list(return_objs)])
            else:
                output_node = return_objs.content
        return output_node

    def execute(self, *args, **kwargs):
        dag = self.build()
        return ray.get(dag.execute(
            {
                'args': args,
                'kwargs': kwargs
            }
        ))
