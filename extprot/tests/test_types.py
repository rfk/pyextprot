
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


