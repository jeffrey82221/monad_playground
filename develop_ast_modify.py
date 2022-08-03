"""
TODO:
- [X] 1. change main_func -> target_main_func
- [X] 2. assert that all lines should be x = self.xxx(x, x)
- [X] 3. using `inspect.getsourcelines` to read the code of main_func
- [X] 4. adding target_main_func to CustomizeProcess when it is initialized
    - How?
        - 1. produce target_main_func at global
        - 2. assign target_main_func to CustomizeProcess when it is initialize

Reference:
https://stackoverflow.com/questions/972/
https://greentreesnakes.readthedocs.io/en/latest/tofrom.html#fix-locations
"""
import ast
import inspect
from copy import deepcopy
from textwrap import dedent
from typing import Callable

import astunparse


class ClassMethodTransformer(ast.NodeTransformer):
    """Convert class method call to the binded class method call."""

    def __init__(self) -> None:
        pass

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
                        attr="bind",
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


class CustomizeProcess:

    def __init__(self) -> None:
        self.__cls_method_trafo = ClassMethodTransformer()
        self.__parse_main_func(self.run)

    def run(self, *args, **kargs):
        p1 = self.sub_func_1_plus(a, b)
        p2 = self.sub_func_2_prod(a, b)
        return p1, p2

    def sub_func_1_plus(self, a, b):
        return a + b

    def sub_func_2_prod(self, a, b):
        return a * b

    def bind(self, func):
        return func

    def __parse_main_func(self, func: Callable) -> None:
        """Parse `run` and construct the corresponding target
        function `target_main_func` as new class method.

        Parameters:
            func: function to parse
        """
        # Retrieve `main_func` function node
        lines = inspect.getsourcelines(func)[0]
        main_func_str = dedent("".join(lines))
        main_func_node = ast.parse(main_func_str).body[0]
        
        # Construct `target_main_func` function node
        target_main_func_node = self.__gen_target_main_func_node(main_func_node)

        # Bind `target_main_func` as class method
        target_main_func_str = astunparse.unparse(target_main_func_node)
        exec(target_main_func_str, globals())
        CustomizeProcess.target_main_func = target_main_func

    def __gen_target_main_func_node(
        self, main_func_node: ast.FunctionDef
    ) -> ast.FunctionDef:
        """Return `target_main_func` node corresponding to `run`.

        Parameters:
            main_func_node: `main_func` function node

        Return:
            target_main_func_node: `target_main_func` function node
        """
        target_main_func_node = deepcopy(main_func_node)
        target_main_func_node.name = "target_main_func"
        target_main_func_node = self.__cls_method_trafo.visit(target_main_func_node)

        # Remove function annotation from target_main_func
        for i, arg in enumerate(target_main_func_node.args.args):
            target_main_func_node.args.args[i].annotation = None
        target_main_func_node.returns = None

        return target_main_func_node

    
if __name__ == '__main__':
    c = CustomizeProcess()
    print(c.target_main_func)
    print(c.target_main_func(2, 3))