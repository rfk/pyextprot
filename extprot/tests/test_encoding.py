
#  Encoding tests based on examples from doc/encoding.md

import unittest

from extprot.types import *
from extprot.stream import StringStream


class a_bool(Message):
    v = Field(Bool)

class a_tuple(Message):
    v = Field(Tuple.build(Bool,Bool))

_a = Unbound()
class maybe(Union):
    _unbound_types = (_a,)
    class Unknown(Option):
        _types = ()
    class Known(Option):
        _types = (_a,)

class foo(Message):
    a = Field(bind(maybe,Int))
    b = Field(bind(maybe,Bool))

class some_ints_l(Message):
    l = Field(List.build(Int))

class some_ints_a(Message):
    l = Field(Array.build(Int))

class a_bool_and_int(Message):
    b = Field(a_bool)
    i = Field(Int)


class Test_Encoding(unittest.TestCase):

    def assertEncEquals(self,msg,enc):
        enc1 = map(ord,msg.to_string())
        self.assertEquals(enc1,enc)

    def test_a_bool(self):
        t = a_bool(True)
        self.assertEncEquals(t,[1,3,1,2,1])
        f = a_bool(False)
        self.assertEncEquals(f,[1,3,1,2,0])

    def test_a_tuple(self):
        tf = a_tuple((True,False))
        self.assertEncEquals(tf,[1,8,1,1,5,2,2,1,2,0])

    def test_foo(self):
        f = foo(maybe.Unknown,maybe.Known(True))
        self.assertEncEquals(f,[1,7,2,10,1,3,1,2,1])

    def test_some_ints(self):
        si_l = some_ints_l([1,2,3,-1])
        si_a = some_ints_a([1,2,3,-1])
        self.assertEquals(si_l.to_string(),si_a.to_string())
        self.assertEncEquals(si_l,[1,12,1,5,9,4,0,2,0,4,0,6,0,1])
 
    def test_a_bool_and_int(self):
        bi = a_bool_and_int(a_bool(True),-1)
        self.assertEncEquals(bi,[1,8,2,1,3,1,2,1,0,1])

