

from extprot import types
from extprot.stream import StringStream

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


def test():
    assert len(person._types) == 4
    assert len(optional._types) == 2
    assert issubclass(person._types[2]._types[0],optional)
    assert len(person._types[2]._types[0]._types) == 2
    p1 = person("Ryan",1,optional.Set("ryan@rfk.id.au"),[])
    assert p1.name == "Ryan"
    assert p1.email[0] == "ryan@rfk.id.au"
    p2 = person("Lauren",2,optional.Unset,[("12345678",phone_type.Home)])
    assert p2.name == "Lauren"
    assert len(p2.phones) == 1
    assert p2.phones[0][0] == "12345678"
    assert p2.phones[0][1] == phone_type.Home
    p3 = person("Aidan",3)
    assert p3.name == "Aidan"
    assert p3.email is optional.Unset
    assert p3.phones == []
 
    s = StringStream()
    p3.to_stream(s)
    assert p3 == person.from_stream(StringStream(s.getstring()))

    s = StringStream()
    p1.to_stream(s)
    assert p1 == person.from_stream(StringStream(s.getstring()))

    book1 = address_book([p1,p2,p3])
    s = StringStream()
    book1.to_stream(s)
    book2 = address_book.from_stream(StringStream(s.getstring()))
    assert book1 == book2

if __name__ == "__main__":
    test()


