class Parent:
    def __init__(self):
        print('class name:', self.__class__.__name__)
        pass
    
class Child(Parent):
    def __init__(self):
        super().__init__()
        
child = Child()


        