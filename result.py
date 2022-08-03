

class CustomizeProcess():

    def target_main_func(self, a, b):
        p1 = self.bind(self.sub_func_1_plus)(a, b)
        p2 = self.bind(self.sub_func_2_prod)(a, b)
        return (p1, p2)
    '\n    def target_main_func(self, a, b):\n        p1 = self.bind(self.sub_func_1_plus)(a, b)\n        p2 = self.bind(self.sub_func_2_prod)(a, b)\n        return p1, p2\n    '

    def main_func(self, a, b):
        p1 = self.sub_func_1_plus(a, b)
        p2 = self.sub_func_2_prod(a, b)
        return (p1, p2)

    def sub_func_1_plus(self, a, b):
        return (a + b)

    def sub_func_2_prod(self, a, b):
        return (a * b)

    def bind(self, func):
        return func
