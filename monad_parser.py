"""Monad customized process parser."""
import ast
from copy import deepcopy

import astunparse


class MonadParser:
    """Monad customized process parser.

    Parameters:
        file_path: path of file to parse
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.__cls_method_trafo = ClassMethodTransformer()

    def parse(self) -> ast.ClassDef:
        """Parse monad customized process and construct corresponding
        process class node with `target_main_func`.

        Return:
            proc_cls_node_parsed: parsed process class node
        """
        proc_module = self.__get_proc_module()
        proc_cls_node = self.__get_proc_cls_node(proc_module)
        main_func_node = self.__get_main_func_node(proc_cls_node)
        target_main_func_node = self.__gen_target_main_func_node(main_func_node)

        proc_cls_node_parsed = deepcopy(proc_cls_node)
        proc_cls_node_parsed.body.insert(0, target_main_func_node)

        return proc_cls_node_parsed

    def __get_proc_module(self) -> ast.Module:
        """Load and return customized process module.

        Return:
            proc_module: customized process module
        """
        with open(self.file_path, "r") as f:
            proc_module = ast.parse(f.read())

        return proc_module

    def __get_proc_cls_node(self, proc_module: ast.Module) -> ast.ClassDef:
        """Return customized process class node.

        Return:
            proc_cls_node: customized process class node
        """
        for node in proc_module.body:
            if isinstance(node, ast.ClassDef) and node.name.endswith("Process"):
                proc_cls_node = node

        return proc_cls_node

    def __get_main_func_node(self, proc_cls_node: ast.ClassDef) -> ast.FunctionDef:
        """Return `main_func` function node.

        Parameters:
            proc_cls_node: customized process class node

        Return:
            main_func_node: `main_func` function node
        """
        for node in proc_cls_node.body:
            if isinstance(node, ast.FunctionDef) and node.name == "main_func":
                main_func_node = node

        return main_func_node

    def __gen_target_main_func_node(
        self, main_func_node: ast.FunctionDef
    ) -> ast.FunctionDef:
        """Return `target_main_func` node corresponding to `main_func`.

        Parameters:
            main_func_node: `main_func` function node

        Return:
            target_main_func_node: `target_main_func` function node
        """
        target_main_func_node = deepcopy(main_func_node)

        target_main_func_node.name = "target_main_func"
        target_main_func_node = self.__cls_method_trafo.visit(target_main_func_node)

        return target_main_func_node


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
        if node.func.value.id == "self":
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
        return node


if __name__ == "__main__":
    monad_parser = MonadParser("./develop_ast_modify.py")
    proc_cls_node_parsed_ast = monad_parser.parse()
    proc_cls_node_parsed_str = astunparse.unparse(proc_cls_node_parsed_ast)

    with open("./result.py", "w") as f:
        for line in proc_cls_node_parsed_str:
            f.write(line)
