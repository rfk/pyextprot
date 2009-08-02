
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

