"""
TODO:
- [ ] 1. change main_func -> target_main_func 
- [ ] 2. assert that all lines should be x = self.xxx(x, x)

https://stackoverflow.com/questions/972/
https://greentreesnakes.readthedocs.io/en/latest/tofrom.html#fix-locations
"""
from monad_parser import MonadParser

class CustomizeProcess:
    
    def __init__(self):
        self.__parse_main_func()
    
    def main_func(self, a, b):
        p1 = self.sub_func_1_plus(a, b)
        p2 = self.sub_func_2_prod(a, b)
        return p1, p2
    
    def sub_func_1_plus(self, a, b):
        return a + b

    def sub_func_2_prod(self, a, b):
        return a * b
    
    def bind(self, func):
        return func
    
    def __parse_main_func(self) -> None:
        monad_parser = MonadParser(__file__)
        target_main_func_str = monad_parser.parse()
        print(target_main_func_str)
        exec(target_main_func_str, globals())
        self.target_main_func = target_main_func   # Bound method
