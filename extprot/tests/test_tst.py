#
#  Pythonization of the "tst" example from extprot
#

import unittest
from os import path
import tempfile

import extprot
from extprot import types


class id(types.Int):
    pass


_unbound_types = tuple(types.Unbound() for _ in xrange(4))
class tuple4(types.Tuple):
    _unbound_types = _unbound_types
    _types = (_unbound_types[0],_unbound_types[1],_unbound_types[2],_unbound_types[3])


_unbound_types = tuple(types.Unbound() for _ in xrange(1))
class dim(types.Union):
    _unbound_types = _unbound_types
    class A(types.Option):
        _types = (types.Int,)
    class B(types.Option):
        _types = (types.Float,)
    class C(types.Option):
        _types = (types.Float,types.Int,types.List.build(types.String),types.Tuple.build(types.Float,types.Array.build(types.Bool),types.List.build(types.List.build(types.String))))
    class D(types.Option):
        _types = (_unbound_types[0],)


class source(types.Union):
    class One(types.Option):
        _types = ()
    class Some_other(types.Option):
        _types = ()


class a(types.Tuple):
    _types = (types.Int,types.Int)


_unbound_types = tuple(types.Unbound() for _ in xrange(1))
class meta(types.Union):
    _unbound_types = _unbound_types
    class Unset(types.Option):
        _types = ()
    class Set(types.Option):
        _types = (source,_unbound_types[0])


class metadata(types.Message):
    author = types.Field(types.bind(meta,types.String))
    pages = types.Field(types.bind(meta,types.Int))


class doc(types.Union):
    class Normal(types.Message):
        normal_id = types.Field(id)
        dim = types.Field(types.bind(dim,types.Int))
        normal_name = types.Field(types.String)
        metadata = types.Field(metadata)
    class Simple(types.Message):
        simple_id = types.Field(id)
        simple_name = types.Field(types.String)


def make_cases(metadata,meta,source,doc,dim,**extra):
    """Make a TestTst testcase from namespace containing types."""
    class Test(unittest.TestCase):

        def test_metadata(self):
            md1 = metadata(author=meta.Unset,pages=meta.Unset)
            md2 = metadata(author=meta.Unset,pages=meta.Set(source.One,7))
            md3 = metadata(meta.Set(source.Some_other(),"Ryan"),meta.Unset)
            md4 = metadata(md3.author,meta.Set(source.One,2578982))
            for md in (md1,md2,md3,md4):
                self.assertEquals(md,metadata.from_string(md.to_string()))

        def test_doc(self):
            md1 = metadata(author=meta.Unset,pages=meta.Unset)
            md2 = metadata(author=meta.Unset,pages=meta.Set(source.One,7))
            md3 = metadata(meta.Set(source.Some_other(),"Ryan"),meta.Unset)
            md4 = metadata(md3.author,meta.Set(source.One,2578982))
            doc1 = doc.Simple(7,"hello")
            doc2 = doc.Normal(7892,dim.A(3),"hello2",md1)
            assert doc2.dim[0] == 3
            doc3 = doc.Normal(42,dim.B(3.14159265),"testing extprot",md4)
            assert doc3.dim[0] == 3.14159265
            doc4 = doc.Normal(512,dim.D(85),"testing extprot",md3)
            assert doc4.dim[0] == 85
            doc5 = doc.Normal(512,dim.C(7.1,42,["hi","there"],(92.0,[True,],[])),"testing extprot",md2)
            assert doc5.dim[0] == 7.1
            assert doc5.dim[1] == 42
            assert " ".join(doc5.dim[2]) == "hi there"
            assert doc5.dim[3][0] == 92.0
            assert not doc5.dim[3][2]
            doc5.dim[3][2].append(["ateststring"])
            assert len(doc5.dim[3][2]) == 1
            self.assertRaises(ValueError,doc5.dim[3][2].append,7)
            for d in (doc1,doc2,doc3,doc4,doc5):
                self.assertEquals(d,doc.from_string(d.to_string()))

    return Test


#  test the hard-crafted translation at the start of this file
Test_handcrafted = make_cases(**globals())

#  test the dynamic in-memory compilation
file = path.join(path.dirname(__file__),"../../examples/tst.proto")
dynamic = {}
extprot.import_protocol(file,dynamic)
Test_dynamic = make_cases(**dynamic)

#  test the to-source-code compilation
file = path.join(path.dirname(__file__),"../../examples/tst.proto")
compiled = {}
modfile = tempfile.NamedTemporaryFile()
extprot.compile_protocol(open(file),modfile)
modfile.flush()
execfile(modfile.name,compiled)
Test_compiled = make_cases(**compiled)
modfile.close()

