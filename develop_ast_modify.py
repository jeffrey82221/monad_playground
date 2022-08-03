"""
TODO:
- [ ] 1. change main_func -> target_main_func 
- [ ] 2. assert that all lines should be x = self.xxx(x, x)
- [ ] 3. using `inspect.getsourcelines` to read the code of main_func 
- [ ] 4. adding target_main_func to CustomizeProcess when it is initialized 
    - How? 
        - 1. produce target_main_func at global 
        - 2. assign target_main_func to CustomizeProcess when it is initialized
"""
import inspect

def target_main_func(self, a, b):
    p1 = self.sub_func_1_plus(a, b)
    p2 = self.sub_func_2_prod(a, b)
    return p1, p2

class CustomizeProcess:
    def __init__(self):
        # self.target_main_func = self.parse(self.main_func)
        self.target_main_func = target_main_func
    """
    def target_main_func(self, a, b):
        p1 = self.bind(self.sub_func_1_plus)(a, b)
        p2 = self.bind(self.sub_func_2_prod)(a, b)
        return p1, p2
    """
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
    
    def parse(self, func):
        lines = inspect.getsourcelines(func)[0]
        print(''.join(lines))
            
            


