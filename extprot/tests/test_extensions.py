
import unittest

import extprot
from extprot import types


class TestExtensions(unittest.TestCase):

    def test_prim_to_union(self):
        class StringOrNone(types.Union):
            class Unset(types.Option):
                pass
            class Set(types.Option):
                _types = (types.String,)
        class M1(types.Message):
            value = types.Field(types.String)
        class M2(types.Message):
            value = types.Field(StringOrNone)
        m = M2.from_string(M1("hello").to_string())
        self.assertEquals(m.value,StringOrNone.Set("hello"))

        class M3(types.Message):
            value = types.Field(types.Int)
        self.assertRaises(types.ParseError,M2.from_string,M3(7).to_string())

    def test_prim_to_tuple(self):
        class AtLeastOneInt(types.Tuple):
            _types = (types.Int,types.List.build(types.Int))
        class M1(types.Message):
            value = types.Field(types.Int)
        class M2(types.Message):
            value = types.Field(AtLeastOneInt)
        m = M2.from_string(M1(7).to_string())
        self.assertEquals(m.value,(7,[]))

        class M3(types.Message):
            value = types.Field(types.String)
        self.assertRaises(types.ParseError,M2.from_string,M3("hello").to_string())


