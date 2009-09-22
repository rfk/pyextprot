
from os import path
import unittest
import pickle

import extprot
from extprot import types

class movie(types.Message):
    id = types.Field(types.Int)
    title = types.Field(types.String)
    actors = types.Field(types.List.build(types.String))

class recording(types.Union):
    class Vinyl(types.Message):
        title = types.Field(types.String)
    class CD(types.Message):
        title = types.Field(types.String)

class Pointer(types.Int):
    """Pointer type dynamically loading object references."""
    INSTANCES = {1: "hello", 3: "world"}
    @classmethod
    def convert(cls,value):
        if isinstance(value,str):
            for (k,v) in cls.INSTANCES.iteritems():
                if v == value:
                    return k
            raise ValueError("not a valid Pointer")
        return super(Pointer,cls).convert(value)
    @classmethod
    def retrieve(cls,value):
        try:
            return cls.INSTANCES[value]
        except KeyError:
            raise ValueError("no such Pointer: %s" % (value,))
 
class PointerMsg(types.Message):
    msg = types.Field(Pointer)

class PointerMsgs(types.Message):
    msgs = types.Field(types.List.build(Pointer))


file = path.join(path.dirname(__file__),"../../../examples/address_book.proto")
extprot.import_protocol(file,globals(),__name__)


class TestTypes(unittest.TestCase):

    def test_pickling_compiled(self):
        p1 = person("Guido",7)
        assert pickle.loads(pickle.dumps(p1)) == p1

    def test_pickling_manual(self):
        m1 = movie(1,"Bad Eggs",["Mick Molloy","Judith Lucy"])
        assert pickle.loads(pickle.dumps(m1)) == m1

    def test_pickling_union(self):
        cd = recording.CD("Delta's Greated Hits")
        assert pickle.loads(pickle.dumps(cd)) == cd

    def test_retrieve(self):
        p = PointerMsg("hello")
        self.assertEquals(p.msg,"hello")
        self.assertEquals(p.__dict__['msg'],1)
        p = PointerMsg(1)
        self.assertEquals(p.msg,"hello")
        self.assertEquals(p.__dict__['msg'],1)
        self.assertRaises(ValueError,PointerMsg,"bugaloo")
        p = PointerMsg(2)
        self.assertEquals(p.__dict__['msg'],2)
        self.assertRaises(ValueError,getattr,p,"msg")
        ps = PointerMsgs([])
        self.assertEquals(ps.msgs,[])
        self.assertEquals(ps.__dict__['msgs'],[])
        ps = PointerMsgs(["hello",3])
        self.assertEquals(ps.msgs[0],"hello")
        self.assertEquals(ps.msgs[1],"world")
        self.assertEquals(ps.__dict__['msgs'],[1,3])


