"""
selectively inherit `A` or `B` class via `AorBBase`
with switch defined on the `mode` method at the direct subclass of `AorBBase`
"""


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
    """ A Metaclass which decorates the entire class
    """
    def __new__(cls, clsname, superclasses, attributedict):
        """
        This is like a decorator to a class
        """
        print('cls:', cls)
        print('clsname', clsname)
        print('superclasses before', superclasses)
        if 'mode' in attributedict and 'AorBBase' in [
                c.__name__ for c in list(superclasses)]:
            switch_label = attributedict['mode']('self')
            print('switch_label', switch_label)
            if switch_label == 'A':
                # add A as superclass
                superclasses = list(superclasses)
                superclasses.append(A)
                superclasses = tuple(superclasses)
            elif switch_label == 'B':
                # add B as superclass
                superclasses = list(superclasses)
                superclasses.append(B)
                superclasses = tuple(superclasses)
        print('superclasses after:', superclasses)
        return super().__new__(cls, clsname, superclasses, attributedict)


print('BaseC ################')


class AorBBase(metaclass=AorB):
    def c_method(self):
        print('base c')

    def mode(self):
        pass


print('C ################')


class CinhA(AorBBase):
    def c_method(self):
        print('c method')

    def mode(self):
        return 'A'


print('D ################')


class DinhB(AorBBase):
    def d_method(self):
        print('d method')

    def mode(self):
        return 'B'


print('E ################')


class E(DinhB):
    # NOTE: mode in this level should not effect the selection of inheritance
    # at the level of class inherit BaseC
    def mode(self):
        return 'A'


if __name__ == '__main__':
    # c = C()
    print('CinhA mro:', CinhA.__mro__)
    # d = D()
    print('DinhB mro:', DinhB.__mro__)
    # d.d_method()
    # d.c_method()
    print('E mro:', E.__mro__)
