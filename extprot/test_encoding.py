
#  Encoding tests based on examples from doc/encoding.md

from extprot.types import *
from extprot.stream import StringStream

def test():

    class a_bool(Message):
        v = Field(Bool)
    t = a_bool(True)
    s = StringStream()
    t.to_stream(s)
    s.reset()
    assert map(ord,s.getstring()) == [1,3,1,2,1]
    f = a_bool(False)
    s = StringStream()
    f.to_stream(s)
    s.reset()
    assert map(ord,s.getstring()) == [1,3,1,2,0]

    class a_tuple(Message):
        v = Field(Tuple.build(Bool,Bool))
    tf = a_tuple((True,False))
    s = StringStream()
    tf.to_stream(s)
    s.reset()
    assert map(ord,s.getstring()) == [1,8,1,1,5,2,2,1,2,0]

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
    foo1 = foo(maybe.Unknown,maybe.Known(True))
    s = StringStream()
    foo1.to_stream(s)
    s.reset()
    assert map(ord,s.getstring()) == [1,7,2,10,1,3,1,2,1]

    class some_ints_l(Message):
        l = Field(List.build(Int))
    class some_ints_a(Message):
        l = Field(Array.build(Int))
    si_l = some_ints_l([1,2,3,-1])
    si_a = some_ints_a([1,2,3,-1])
    s1 = StringStream()
    s2 = StringStream()
    si_l.to_stream(s1)
    si_a.to_stream(s2)
    assert s1.getstring() == s2.getstring()
    assert map(ord,s1.getstring()) == [1,12,1,5,9,4,0,2,0,4,0,6,0,1]

    class a_bool_and_int(Message):
        b = Field(a_bool)
        i = Field(Int)
    bandi = a_bool_and_int(a_bool(True),-1)
    s = StringStream()
    bandi.to_stream(s)
    s.reset()
    assert map(ord,s.getstring()) == [1,8,2,1,3,1,2,1,0,1]
    

if __name__ == "__main__":
    test()

