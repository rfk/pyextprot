
from os import path
import unittest
import pickle
import operator

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

class OnOff(types.Message):
    is_on = types.Field(types.Bool)

class IDs(types.Message):
    map = types.Field(types.Assoc.build(types.Int,types.String))


file = path.join(path.dirname(__file__),"../../examples/address_book.proto")
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

    def test_subclass_fields(self):
        class ActionMovie(movie):
            severity = types.Field(types.Int)
        self.assertEquals(len(ActionMovie._types),4)
        m1 = ActionMovie(1,"Terminator",["Arnie","Linda"],7)
        self.assertEquals(m1.title,"Terminator")
        self.assertEquals(m1.severity,7)
        class BuddyMovie(movie):
            suck_factor = types.Field(types.Int)
            actors = types.Field(types.List.build(types.String))
        self.assertRaises(ValueError,BuddyMovie,2,"Beverly Hills Cop III",["Eddie","ThatOtherGuy"],5)
        m2 = BuddyMovie(2,"Beverly Hills Cop III",5,["Eddie","ThatOtherGuy"])
        self.assertEquals(m2.title,"Beverly Hills Cop III")
        self.assertEquals(m2.id,2)
        self.assertEquals(m2.suck_factor,5)

    def test_bool_field(self):
        oo = OnOff(False)
        self.assertEquals(oo.is_on,False)
        self.assertEquals(OnOff.from_string(oo.to_string()).is_on,False)
        oo = OnOff(True)
        self.assertEquals(oo.is_on,True)
        self.assertEquals(OnOff.from_string(oo.to_string()).is_on,True)

    def test_large_int(self):
        bignum = 43 ** 72
        m1 = movie(bignum,"Improbable ID")
        self.assertEquals(m1.id,bignum)
        m2 = movie.from_string(m1.to_string())
        self.assertEquals(m2.id,bignum)
        self.assertEquals(m1,m2)

    def test_assoc_field(self):
        ids = IDs()
        self.assertEquals(ids.map,{})
        ids.map[1] = "ONE"
        ids.map[2] = "TWO"
        self.assertRaises(ValueError,operator.setitem,ids.map,"THREE",3)
        self.assertRaises(ValueError,operator.setitem,ids.map,3,3)
        ids2 = IDs.from_string(ids.to_string())
        self.assertEquals(ids,ids2)
        self.assertEquals(ids2.map[1],"ONE")
        self.assertEquals(ids2.map[2],"TWO")
        self.assertRaises(KeyError,operator.getitem,ids2.map,3)


