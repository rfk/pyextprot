#
#  Pythonization of the "address_book" example from extprot
#

import unittest
from os import path
import tempfile

import extprot
from extprot import types

_unbound_types = tuple(types.Unbound() for _ in xrange(1))
class optional(types.Union):
    _unbound_types = _unbound_types
    class Unset(types.Option):
        _types = ()
    class Set(types.Option):
        _types = (_unbound_types[0],)


class phone_type(types.Union):
    class Mobile(types.Option):
        _types = ()
    class Home(types.Option):
        _types = ()
    class Work(types.Option):
        _types = ()


class person(types.Message):
    name = types.Field(types.String)
    id = types.Field(types.Int)
    email = types.Field(types.bind(optional,types.String))
    phones = types.Field(types.List.build(types.Tuple.build(types.String,phone_type)))


class address_book(types.Message):
    persons = types.Field(types.List.build(person))


def make_cases(optional,phone_type,person,address_book,**extra):
    """Make a TestAddressBook testcase from namespace containing types."""
    class TestAddressBook(unittest.TestCase):

        def test_types(self):
            self.assertEquals(len(person._types),4)
            self.assertEquals(len(optional._types),2)
            assert issubclass(person._types[2]._types[0],optional)
            self.assertEquals(len(person._types[2]._types[0]._types),2)

        def test_person(self):
            p1 = person("Ryan",1,optional.Set("ryan@rfk.id.au"),[])
            assert p1.name == "Ryan"
            assert p1.email[0] == "ryan@rfk.id.au"
            p2 = person("Lauren",2,optional.Unset,[("123456",phone_type.Home)])
            assert p2.name == "Lauren"
            assert len(p2.phones) == 1
            assert p2.phones[0][0] == "123456"
            assert p2.phones[0][1] == phone_type.Home
            p3 = person("Aidan",3)
            assert p3.name == "Aidan"
            assert p3.email is optional.Unset
            assert p3.phones == []

            assert p1 == person.from_string(p1.to_string())
            assert p3 == person.from_string(p3.to_string())


        def test_address_book(self):
            p1 = person("Ryan",1,optional.Set("ryan@rfk.id.au"),[])
            p2 = person("Lauren",2,optional.Unset,[("123456",phone_type.Home)])
            p3 = person("Aidan",3)
            book1 = address_book([p1,p2,p3])
            book2 = address_book.from_string(book1.to_string())
            assert book1 == book2

    return TestAddressBook

#  test the hard-crafted translation at the start of this file
TestAddessBook_handcrafted = make_cases(**globals())

#  test the dynamic in-memory compilation
file = path.join(path.dirname(__file__),"../../examples/address_book.proto")
dynamic = {}
extprot.import_protocol(file,dynamic)
Test_dynamic = make_cases(**dynamic)

#  test the to-source-code compilation
file = path.join(path.dirname(__file__),"../../examples/address_book.proto")
compiled = {}
modfile = tempfile.NamedTemporaryFile()
extprot.compile_protocol(file,modfile)
modfile.flush()
execfile(modfile.name,compiled)
Test_compiled = make_cases(**compiled)
modfile.close() 

