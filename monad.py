import abc
from functools import wraps

import ast
import inspect
from copy import deepcopy
from textwrap import dedent
from typing import Callable
import astunparse


class ClassMethodTransformer(ast.NodeTransformer):
    """Convert class method call to the binded class method call."""

    def __init__(self, decoration='bind') -> None:
        self.decoration = decoration

    def visit_Call(self, node: ast.Call) -> ast.Call:
        """Apply transformer to visited node.

        This transformer converts class method call to the binded one;
        that is, `self.bind()` binds each class method call.

        Parameters:
            node: visited node to transform

        Return:
            node_trans: transformed node
        """
        assert node.func.value.id == "self", "Only calling of class method are permitted"
        node_trans = ast.copy_location(
            ast.Call(
                func=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id="self", ctx=ast.Load()),
                        attr=self.decoration,
                        ctx=ast.Load(),
                    ),
                    args=[
                        ast.Attribute(
                            value=ast.Name(id="self", ctx=ast.Load()),
                            attr=node.func.attr,
                            ctx=ast.Load(),
                        )
                    ],
                    keywords=[],
                ),
                args=node.args,
                keywords=node.keywords,
            ),
            node,
        )
        return node_trans
    
class Monad:
    def __init__(self) -> None:
        self.__parse_main_func(self.run, decoration='bind', target_func_name='binded_run')
        self.__parse_main_func(self.run, decoration='decorator', target_func_name='decorated_run')
    

    @abc.abstractmethod
    def run(self, *args):
        """
        The main function to be altered by monad

        Originally: 
        def run(self, a, b, c):
            a = self.func_1(b, c)
            d = self.func_2(a)
            return a, d

        Becomes:
        1) 
        def binded_run(self, a, b, c):
            a = self.bind(self.func_1)(b, c)
            d = self.bind(self.func_2)(a)
            return a, d
        2) 
        def decorated_run(self, a, b, c):
            a = self.decorator(self.func_1)(b, c)
            d = self.decorator(self.func_2)(a)
            return a, d
        """
        raise NotImplementedError()
        
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
        '''decorator to be bind to the `run` function, designed in monad pattern'''
        @wraps(orig_func)
        def wrapper_of_bind(*args, **kwargs):
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
        return wrapper_of_bind
    
    def __parse_main_func(self, func: Callable, decoration: str, target_func_name: str) -> None:
        """Parse `run` and construct the corresponding target
        function `target_func_name` as new class method.

        Parameters:
            func: function to parse,
            decoration: func name of `decorator`
            target_func_name: func name of `run` after alternation
        """
        # Retrieve `main_func` function node
        lines = inspect.getsourcelines(func)[0]
        main_func_str = dedent("".join(lines))
        main_func_node = ast.parse(main_func_str).body[0]
        
        # Construct `target_func_name` function node
        target_main_func_node = self.__gen_target_main_func_node(main_func_node, decoration, target_func_name)

        # Bind `target_func_name` as class method
        target_main_func_str = astunparse.unparse(target_main_func_node)
        print(target_main_func_str)
        exec(target_main_func_str, globals())
        exec(f'Monad.{target_func_name} = {target_func_name}')

    def __gen_target_main_func_node(
        self, main_func_node: ast.FunctionDef, decoration: str, target_func_name: str
    ) -> ast.FunctionDef:
        """Return `TARGET_MAIN_FUNC` node corresponding to `run`.

        Parameters:
            main_func_node: `run` function node
            decoration: func name of `decorator`
            target_func_name: func name of `run` after alternation

        Return:
            target_main_func_node: `target_func_name` function node
        """
        cls_method_traformer = ClassMethodTransformer(decoration=decoration)
        target_main_func_node = deepcopy(main_func_node)
        target_main_func_node.name = target_func_name
        target_main_func_node = cls_method_traformer.visit(target_main_func_node)

        # Remove function annotation from target_main_func
        for i, arg in enumerate(target_main_func_node.args.args):
            target_main_func_node.args.args[i].annotation = None
        target_main_func_node.returns = None

        return target_main_func_node

