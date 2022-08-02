"""
TODO:
- [ ] 1. change main_func -> target_main_func 
- [ ] 2. target_main_func should not have strong type 
- [ ] 3. assert that all lines should be x = self.xxx(x, x)
"""
class CustomizeProcess:
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
            