"""
A monad design pattern
that decorate the entire pipeline
such that all functions within it are
connect to a ray dag!

PROBLEMS:

- [X] input_node represent only a single input
    - [X] need to combine *args & **kargs into one input
    - [X] need remote function that extract *args and **kargs and compose
        *args and **kargs of `ReturnClass` where the contents are the
        outputs of the extract method (bind).
    - [X] in the wrapper of bind, extract content from *args and **kargs
        and send them to the bined-function (make sure to make the function a remote function)
    - [X] extract output (which is a ray binded result) and dispatch it into
        multiple output according to the output type hint of the bined function.
"""
from typing import Tuple
import ray
from ray_monad import RayMonad
import time


class PDProcess(RayMonad):
    def run(self, x: int, y: int) -> Tuple[int, int]:
        a = self.plus(x, y)
        c = self.plus(x, y)
        b = self.double(a)
        a, b = self.passing(a, b)
        return a, b, c

    def plus(self, x: int, y: int) -> int:
        time.sleep(5)
        return x + y

    def double(self, s: int) -> int:
        return s * 2

    def passing(self, a: int, b: int) -> Tuple[int, int]:
        return a, b


ray.init()
process = PDProcess()

if __name__ == '__main__':
    print('start normal run')
    start_time = time.time()
    ans = process.run(2, 3)
    print(ans)
    print("--- %s seconds ---" % (time.time() - start_time))
    print('start ray run')
    start_time = time.time()
    ans = process.execute(2, 3)
    print(ans)
    print("--- %s seconds ---" % (time.time() - start_time))
