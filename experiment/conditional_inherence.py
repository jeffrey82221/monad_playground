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

    @staticmethod
    def some_decorator(func):
        """ Decorator for counting the number of function
            or method calls to the function or method func
        """
        def helper(*args, **kwargs):
            helper.calls += 1
            return func(*args, **kwargs)
        helper.calls = 0
        helper.__name__ = func.__name__

        return helper

    def __new__(cls, clsname, superclasses, attributedict):
        """
        This is like a decorator to a class
        """
        print('cls:', cls)
        print('clsname', clsname)
        print('superclasses', superclasses)
        print('attributedict', attributedict)
        switch_label = attributedict['switch'](None)
        if switch_label == 'A':
            # Only A as superclass
            superclasses = (A, )
        elif switch_label == 'B':
            # Only B as superclass
            superclasses = (B, )
        return type.__new__(cls, clsname, superclasses, attributedict)


class C(metaclass=AorB):
    def c_method(self):
        print('c method')

    def switch(self):
        return 'B'


class D(metaclass=AorB):
    def d_method(self):
        print('d method')

    def switch(self):
        return 'A'


if __name__ == '__main__':
    c = C()
    c.label_method()
    c.c_method()
    d = D()
    d.label_method()
    d.d_method()
