from ast import Not


class A(object):

    def label_method(self):
        print("A")

    @classmethod
    def switch(cls):
        raise NotImplementedError()


class B(object):

    def label_method(self):
        print("B")

    @classmethod
    def switch(cls):
        raise NotImplementedError()


class AorB(type):
    """ A Metaclass which decorates all the methods of the
        subclass using call_counter as the decorator
    """
    """
    @staticmethod
    def some_decorator(func):
        '''Decorator for counting the number of function
            or method calls to the function or method func
            
        '''
        def helper(*args, **kwargs):
            helper.calls += 1
            return func(*args, **kwargs)
        helper.calls = 0
        helper.__name__ = func.__name__

        return helper
    """

    def __new__(cls, clsname, superclasses, attributedict):
        """
        This is like a decorator to a class
        """
        print('cls:', cls)
        print('clsname', clsname)
        print('superclasses before', superclasses)
        # print('attributedict', attributedict)
        if 'mode' in attributedict and clsname != 'BaseC':
            switch_label = attributedict['mode']('self')
            print('switch_label', switch_label)
            if switch_label == 'A':
                # Only A as superclass
                superclasses = list(superclasses)
                superclasses.append(A)
                superclasses = tuple(superclasses)
            elif switch_label == 'B':
                # Only B as superclass
                superclasses = list(superclasses)
                superclasses.append(B)
                superclasses = tuple(superclasses)
            # apply mode attribute to superclasses
        print('superclasses after:', superclasses)
        return super().__new__(cls, clsname, superclasses, attributedict)

print('BaseC ################')
class BaseC(metaclass=AorB):
    def c_method(self):
        print('base c')

    def mode(self):
        pass

print('C ################')
class C(BaseC):
    def c_method(self):
        print('c method')

    def mode(self):
        return 'B'

print('D ################')
class D(BaseC):
    def d_method(self):
        print('d method')
    def mode(self):
        return 'A'

print('E ################')
class E(D):
    def mode(self):
        return 'B'



if __name__ == '__main__':
    # c = C()
    print('C mro:', C.__mro__)
    # d = D()
    print('D mro:', D.__mro__)
    # d.d_method()
    # d.c_method()
    print('E mro:', E.__mro__)