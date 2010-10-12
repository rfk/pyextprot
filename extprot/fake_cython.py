"""

  extprot.fake_cython:  shim to pretend like we have cython installed.

"""


def locals(**kwds):
    def wrapper_maker(func):
        return func
    return wrapper_maker


int = None
short = None
longlong = None

