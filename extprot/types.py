"""

  extprot.types:  basic type classes for extprot protocol definitions.

This module defines classes and functions used to compose the in-memory
object structure corresponding to an extprot protocol.  They essentially
form a class-based reification of the extprot type system.  Type definitions
are represented by various subclasses of 'Type', while messages are subclasses
of the specia. type 'Message'.

It's possible to directly compose these primitives into a protocol description
such as the following:

  # typedef 'id' as an integer
  class id(Int):
      pass
 
  # define 'person' message with id, name and at least one email
  class person(Message):
      id = Field(Int)
      name = Field(String)
      emails = Field(Tuple.build(String,List.build(String)))
 
However, it's probably more reliable to use the 'parser' module to generate
this object structure automatically from a .proto souce file.

"""

import struct

from extprot.errors import *


def _issubclass(cls,bases):
    """Like the builtin issubclass(), but doesn't raise TypeError."""
    try:
        return issubclass(cls,bases)
    except TypeError:
        return False


class Type(object):
    """Base class for all concrete extprot types.

    Subclasses of Type are the concrete types in an extprot protocol.  You
    generally won't want to instantiate them directly, but they have these
    interesting class-level methods:

        convert:      convert a python value to standard type representation
        default:      retreive default value for type, if any
        from_stream:  parse value from an extprot bytestream
        to_stream:    write value to an extprot bytestream

    If the type is composed from other types, the class attribute '_cls'
    will contain them as a tuple.  If the type is polymorphic, the class
    attribute '_unbound_types' will contain a tupe of instances of the
    special type 'Unbound'.  These unbound types can later be instantiated
    using the 'bind' function from this module.
    """

    _types = ()
    _unbound_types = ()

    @classmethod
    def convert(cls,value):
        """Convert a python value into internal representation."""
        return value

    @classmethod
    def _convert_types(cls,values,types=None):
        """Convert a sequence of values using a type tuple.

        If no type tuple is given, cls._types is used.
        """
        values = iter(values)
        if types is None:
            types = cls._types
        for t in types:
            try:
                v = values.next()
            except StopIteration:
                raise ValueError("too few values to convert")
            else:
                yield t.convert(v)
        try:
            values.next()
        except StopIteration:
            pass
        else:
            raise ValueError("too many values to convert")

    @classmethod
    def default(cls):
        """Return the default value for this type."""
        raise UndefinedDefaultError

    @classmethod
    def build(cls,*types):
        """Build an instance of this type using the given subtypes."""
        class Anon(cls):
            _types = types
        return Anon

    @classmethod
    def from_stream(cls,stream):
        """Parse a value of this type from an extprot bytestream."""
        raise NotImplementedError

    @classmethod
    def to_stream(cls,value,stream):
        """Write a value fo this type to an extprot bytestream."""
        raise NotImplementedError

    def __eq__(self,other):
        return self is other

    def __ne__(self,other):
        return not self == other


class Bool(Type):
    """Primitive boolean type."""

    @classmethod
    def convert(cls,value):
        return bool(value)

    @classmethod
    def from_stream(cls,stream):
        byte = stream.read_Bits8()
        return (byte != "\x00")

    @classmethod
    def to_stream(cls,value,stream):
        if value:
            byte = "\x01"
        else:
            byte = "\x00"
        stream.write_Bits8(byte)


class Byte(Type):
    """Primitive byte type, an 8-bit integer.

    The canonical representation is as a single-character string.
    """

    @classmethod
    def convert(cls,value):
        try:
            return chr(value)
        except TypeError:
            if not isinstance(value,str):
                raise ValueError("not a valid Byte: " + repr(value))
            if len(value) != 1:
                raise ValueError("not a valid Byte: " + value)
            return value

    @classmethod
    def from_stream(cls,stream):
        return stream.read_Bits8()

    @classmethod
    def to_stream(cls,value,stream):
        stream.write_Bits8(value)


class Int(Type):
    """Primitive signed integer type."""

    @classmethod 
    def convert(cls,value):
        return int(value)

    @classmethod
    def from_stream(cls,stream):
        n = stream.read_Vint()
        return (n >> 1) ^ -(n & 1)

    @classmethod
    def to_stream(cls,value,stream):
        stream.write_Vint((value << 1) | (value >> 63))


class Long(Type):
    """Primitive 64-bit integer type."""

    _max_long = 2**64

    @classmethod
    def convert(cls,value):
        packed = int(value)
        if packed > self._max_long:
            raise ValueError("too big for a long: " + repr(packed))

    @classmethod
    def from_stream(cls,stream):
        return stream.read_Bits64_long()

    @classmethod
    def to_stream(cls,value,stream):
        stream.write_Bits64_long(value)


class Float(Type):
    """Primitive 64-bit float type."""

    @classmethod
    def convert(cls,value):
        # TODO: a better way to convert/check float types?
        try:
            return struct.unpack("<d",struct.pack("<d",value))[0]
        except struct.error:
            raise ValueError("not a Float")

    @classmethod
    def from_stream(cls,stream):
        return stream.read_Bits64_float()

    @classmethod
    def to_stream(cls,value,stream):
        stream.write_Bits64_float(value)


class String(Type):
    """Primitive byte-string type."""

    @classmethod
    def convert(cls,value):
        if not isinstance(value,str):
            raise ValueError("not a valid String: " + repr(value))
        return value

    @classmethod
    def from_stream(cls,stream):
        return stream.read_Bytes()

    @classmethod
    def to_stream(cls,value,stream):
        stream.write_Bytes(value)


class Tuple(Type):
    """Composed tuple type.

    Sublcasses of Tuple represent tuples typed according to cls._types.
    To dynamically build a particular tuple type use the 'build' method
    like so:

        int3 = Tuple.build(Int,Int,Int)
        int_and_str = Tuple.build(Int,String)

    """

    @classmethod
    def convert(cls,value):
        values = tuple(cls._convert_types(value))
        return values

    @classmethod
    def default(cls):
        return tuple(t.default() for t in self._types)

    @classmethod
    def from_stream(cls,stream):
        prefix = stream.read_prefix(stream.TYPE_TUPLE)
        stream.read_int()  # length is ignored
        nelems = stream.read_int()
        values = []
        for t in cls._types[:nelems]:
            values.append(t.from_stream(stream))
        for t in cls._types[nelems:]:
            values.append(t.default())
        for _ in xrange(max(0,nelems - len(cls._types))):
            stream.skip_value()
        return cls._convert_types(values)

    @classmethod
    def to_stream(cls,value,stream):
        values = ((t.to_stream,v) for (t,v) in zip(cls._types,value))
        stream.write_Tuple(0,values)


class _TypedList(list):
    """Subclass of built-in list type that contains type-checked values.

    Instances of _TypedList are the canonical internal representation
    for the List and Array extprot types.
    """

    def __init__(self,type,items=()):
        self._type = type
        super(_TypedList,self).__init__(items)

    def __setitem__(self,key,value):
        if isinstance(key,slice):
            value = (self._type.convert(v) for v in value)
        else:
            value = self._type.convert(value)
        super(_TypedList,self).__setitem__(key,value)

    def __setslice__(self,i,j,sequence):
        items = (self._types.convert(i) for i in sequence)
        super(_TypedList,self).__setslice__(i,j,items)

    def __contains__(self,item):
        super(_TypedList,self).__contains__(self._type.convert(item))

    def append(self,item):
        return super(_TypedList,self).append(self._type.convert(item))

    def index(self,value,start=None,stop=None):
        return super(_TypedList,self).index(self._type.convert(item),start,stop)

    def extend(self,iterable):
        items = (self._types.convert(i) for i in iterable)
        return super(_TypedList,self).extend(items)

    def insert(self,index,object):
        return super(_TypedList,self).insert(index,self._type.convert(object))

    def remove(self,value):
        return super(_TypedList,self).remove(self._type.convert(value))

    def __iadd__(self,other):
        return super(_TypedList,self).__iadd__(_TypedList(self._type,other))


class _List(Type):
    """Base class for list-like composed types.

    This private class provides the shared implementation for the List
    and Array types.
    """

    @classmethod
    def convert(cls,value):
        try:
            return _TypedList(cls._types[0], value)
        except TypeError:
            raise ValueError("not a valid List")

    @classmethod
    def default(cls):
        return _TypedList(cls._types[0])

    @classmethod
    def from_stream(cls,stream):
        prefix = stream.read_prefix(stream.TYPE_HTUPLE)
        stream.read_int() # length is ignored
        nelems = stream.read_int()
        values = _TypedList(cls._types[0])
        for _ in xrange(nelems):
            values.append(cls._types[0].from_stream(stream))
        return values

    @classmethod
    def to_stream(cls,value,stream):
        write = cls._types[0].to_stream
        values = ((write,v) for v in value)
        stream.write_HTuple(0,values)


class List(_List):
    """Composed homogeneous list type.

    To dynamically construct a particular list type, use the 'build' method
    like so:

        list_of_ints = List.build(Int)
        list_of_2ints = List.build(Tuple.build(Int,Int))

    """
    pass


class Array(_List):
    """Compposed homogeneous array type.

    To dynamically construct a particular array type, use the 'build' method
    like so:

        array_of_ints = Array.build(Int)
        array_of_2ints = Array.build(Tuple.build(Int,Int))

    """
    pass


class _UnionMetaclass(type):
    """Metaclass for Union type.

    This metaclass is responsible for populating Union._type with a tuple
    of the declared option types, and Union._option_from_prefix to a mapping
    from extproc encoded prefixes to individual Option classes.
    """

    def __new__(mcls,name,bases,attrs):
        cls = super(_UnionMetaclass,mcls).__new__(mcls,name,bases,attrs)
        #  Find all attributes that are Option or Message classes, and
        #  sort them into cls._types tuple.
        #  TODO: what aboud subclasses of a Union subclass that declare
        #        additional options?  Fortunately I don't think the parser
        #        will ever generate suc a structure.
        if "_types" not in attrs:
            types = []
            is_message_union = False
            is_option_union = False
            for val in attrs.itervalues():
                if _issubclass(val,Option):
                    if is_message_union:
                       raise TypeError("cant union Option and Message")
                    is_option_union = True
                    types.append((val._creation_order,val))
                elif _issubclass(val,Message):
                    if is_option_union:
                        raise TypeError("cant union Option and Message")
                    is_message_union = True
                    types.append((val._creation_order,val))
                elif _issubclass(val,Type) or isinstance(val,Type):
                    raise TypeError("only Option and Message allowed in Union")
            types.sort()
            cls._types = tuple(t for (_,t) in types)
            #  Label each with their index in the union
            e_idx = 0
            t_idx = 0
            for t in cls._types:
                if _issubclass(t,Option) and not t._types:
                    t._index = e_idx
                    e_idx += 1
                else:
                    t._index = t_idx
                    t_idx += 1
        #  Build the prefix lookup table
        cls._option_from_prefix = {}
        for t in cls._types:
            # TODO: delegate this formatting to the stream somehow?
            #       a bit tricky when we don't have one yet.
            if _issubclass(t,Option) and not t._types:
                prefix = ((t._index << 4) | 10)
            else:
                prefix = ((t._index << 4) | 1)
            cls._option_from_prefix[prefix] = t
        return cls


class _OptionMetaclass(type):
    """Metaclass for Option type.

    This metaclass is responsible for populating Option._creation_order with
    a increasing number indicating the order in which subclasses were created.
    """

    _creation_counter = 0

    def __new__(mcls,name,bases,attrs):
        cls = super(_OptionMetaclass,mcls).__new__(mcls,name,bases,attrs)
        cls._creation_order = mcls._creation_counter
        mcls._creation_counter += 1
        return cls


class Option(Type):
    """Individual tagged entry in a Union type.

    Unlike other Type subclasses, Option classes are designed to be
    directly instantiated in order to tag the contained values.  The
    values contained in an instance can be obtained using standard 
    item access (e.g. opt[0], opt[1], etc).
    """

    __metaclass__ = _OptionMetaclass

    def __new__(cls,*values):
        """Custom instance constructor to special-case constant options.

        For Option subclasses that don't contain any values, this returns the
        class itself rather than an instance.
        """
        if not cls._types:
            if values:
                raise TypeError("values given to constant Option")
            return cls
        else:
            return Type.__new__(cls)

    def __init__(self,*values):
        self._values = tuple(self._convert_types(values))

    def __getitem__(self,index):
        return self._values[index]

    def __setitem__(self,index,value):
        self._values[index] = self._types[index].convert(value)

    def __eq__(self,other):
        return self._values == other._values

    @classmethod
    def convert(cls,value):
        if isinstance(value,cls):
            return value
        if _issubclass(value,cls):
            if value._types:
                raise ValueError("no data given to non-constant Option")
            return value
        #  To support easy syntax for polymorphic union types, we also
        #  accept instances of any of our base classes, as long as they
        #  have a compatible type signature.
        if isinstance(value,Option):
            if value.__class__ not in cls.__mro__:
                raise ValueError("not this Option type")
            if unify_types(cls._types,value.__class__._types) is None:
                raise ValueError("not this Option type")
            return cls(*value._values)
        if _issubclass(value,Option):
            if value._types:
                raise ValueError("no data given to non-constant Option")
            if value not in cls.__mro__:
                raise ValueError("not this Option type")
            if unify_types(cls._types,value._types) is None:
                raise ValueError("not this Option type")
            return cls
        #  Not an Option class or instance, must be a direct value tuple
        return cls(*value)

    @classmethod
    def from_stream(cls,stream,prefix=None):
        if prefix is None:
            prefix = stream.read_prefix()
        # Nothing to read for Enum options
        if not cls._types:
            return cls
        stream.read_int() # length is ignored
        nelems = stream.read_int()
        values = []
        for t in cls._types[:nelems]:
            values.append(t.from_stream(stream))
        for t in cls._types[nelems:]:
            stream.skip_value()
            values.append(t.default())
        return cls(*cls._convert_types(values))

    @classmethod
    def to_stream(cls,value,stream,types=None):
        if types is None:
            types = cls._types
        if not types:
            stream.write_Enum(cls._index)
        else:
            values = ((t.to_stream,v) for (t,v) in zip(types,value))
            stream.write_Tuple(cls._index,values)
        

class Field(Type):
    """Type representing a field on a Message.

    Field instances implement the descriptor protocol to map a given
    type to a name on a Message instance.  They're designed to be used
    as follows:

        class msg(Message):
            title = Field(String)
            contents = Field(List.build(String),mutable=True)

    Through some metaclass magic on the Message class, Field instances come
    to know the name (self._name) and index (self._index) by which they are
    attached to a message.
    """

    _creation_counter = 0

    def __init__(self,type,mutable=False):
        self._creation_order = Field._creation_counter
        Field._creation_counter += 1
        self._types = (type,)
        self.mutable = False

    def __get__(self,obj,type=None):
        try:
            return obj.__dict__[self._name]
        except KeyError:
            return self._types[0].default()

    def __set__(self,obj,value):
        if not self.mutable and obj._initialized:
            raise AttributeError("Field '"+self._name+"' is not mutable")
        if value is None:
            value = self._types[0].default()
        obj.__dict__[self._name] = self._types[0].convert(value)

    def from_stream(self,stream):
        return self._types[0].from_stream(stream)

    def to_stream(self,value,stream):
        return self._types[0].to_stream(value,stream)


class _MessageMetaclass(type):
    """Metaclass for message type.

    This metaclass is responsible for populating Message._creation_order with
    a increasing number indicating the order in which subclasses were created,
    setting the _name and _index properties on contained Field instances,
    and creating cls._types as a tuple of contained fields.
    """

    _creation_counter = 0

    def __new__(mcls,name,bases,attrs):
        cls = super(_MessageMetaclass,mcls).__new__(mcls,name,bases,attrs)
        cls._creation_order = mcls._creation_counter
        mcls._creation_counter += 1
        #  Find all attributes that are Field instances and
        #  sort them into cls._types tuple.
        types = []
        for (name,val) in attrs.iteritems():
            if isinstance(val,Field):
                types.append((val._creation_order,name,val))
        types.sort()
        cls._types = tuple(t for (_,_,t) in types)
        #  Label each field with its name and index in the message
        for (i,(_,nm,t)) in enumerate(types):
            t._index = i
            t._name = nm
        return cls


class Message(Type):
    """Composed message type.

    This is the basic unit of data transfer in extprot, and is basically
    a mapping from attribute names to typed values.
    """

    __metaclass__ = _MessageMetaclass

    #  Messages can be part of a Union, giving them a specific index.
    #  Stand-alone messaged always have index of zero.
    _index = 0

    def __init__(self,*args,**kwds):
        self._initialized = False
        #  Process positional and keyword arguments as Field values
        if len(args) > len(self._types):
            raise TypeError("too many positional arguments to Message")
        for (t,v) in zip(self._types,args):
            t.__set__(self,v)
        for t in self._types[len(args):]:
            try:
                v = kwds.pop(t._name)
            except KeyError:
                t.__set__(self,None)
            else:
                t.__set__(self,v)
        if kwds:
            raise TypeError("too many keyword arguments to Message")
        self._initialized = True
        #  Allow calling to_stream() on Message instances.
        def my_to_stream(stream):
            self.__class__.to_stream(self,stream)
        self.to_stream = my_to_stream

    @classmethod
    def convert(cls,value):
        if isinstance(value,cls):
            return value
        if isinstance(value,Message):
            raise ValueError("wrong message type")
        m = cls()
        try:
            value[""]
        except TypeError:
            return cls(*value)
        else:
            return cls(**value)

    @classmethod
    def default(cls):
        return cls()

    @classmethod
    def from_stream(cls,stream,prefix=None):
        if prefix is None:
            prefix = stream.read_prefix(stream.TYPE_TUPLE)
        stream.read_int()  # length is ignored
        nelems = stream.read_int()
        values = []
        for t in cls._types[:nelems]:
            values.append(t.from_stream(stream))
        for _ in xrange(max(0,nelems - len(cls._types))):
            stream.skip_value()
        # default values are handled by Message.__init__
        return cls(*values)

    @classmethod
    def to_stream(cls,value,stream):
        values = ((t.to_stream,t.__get__(value)) for t in cls._types)
        stream.write_Tuple(value._index,values)

    def __eq__(self,msg):
        if self.__class__ != msg.__class__:
            return False
        for t in self._types:
            if t.__get__(self) != t.__get__(msg):
                return False
        return True


class Union(Type):
    """Composed disjoint-union type.

    Subclasses of this type expose the different tags of the union as
    class-level attributes.  This is easiest to explain by example.  For
    the following union type:

        type stuff = Watzit | Thing string int

   The appropriate class structure definition would be:

        class stuff(Union):
            class Watzit(Option):
                _types = ()
            class Thing(Option):
                _types = (String,Int)

    Through some metaclass magic, stuff._types will come to contain the
    declared option types, in the order they were declared.
    To name instances of this type in code you use standard dotted notation:

        stuff1 = stuff.Watzit           # a contant option
        stuff2 = stuff.Thing("name",7)  # a non-contant option

    And the data associated with each option is accessed by indexing:

        stuff2[0] == "name"
        stuff2[1] == 7

    Disjoint message unions are constructed in a simlar way, using Message
    subclasses for the inner classes rather than Options.
    """

    __metaclass__ = _UnionMetaclass

    @classmethod
    def convert(cls,value):
        for t in cls._types:
            try:
                return t.convert(value)
            except ValueError, e:
                pass
        raise ValueError("could not convert Union type")

    @classmethod
    def default(cls):
        for t in cls._types:
            if _issubclass(t,Message):
                return t.default()
            if not t._types:
                return t
        raise UndefinedDefaultError

    @classmethod
    def from_stream(cls,stream):
        prefix = stream.read_prefix()
        try:
            opt = cls._option_from_prefix[prefix]
        except KeyError:
            raise ParseError("not a Union type")
        return opt.from_stream(stream,prefix=prefix)

    @classmethod
    def to_stream(cls,value,stream):
        value.to_stream(value,stream)

   
class Unbound(Type):
    """Unbound type, for representing type parameters.

    Attempts to use this clas in serialization raise errors.  Its intended
    use is for individual instances to represent types that are not yet bound
    in polymorphic type declaration (i.e. to appear in cls._unbound_types).
    """

    @classmethod
    def from_stream(self,stream):
        raise TypeError("parametric type not bound")

    @classmethod
    def to_stream(self,value,stream):
        raise TypeError("parametric type not bound")


def bind(ptype,*ctypes):
    """Dynamically bind unbound type parameters to create a new type class.

    Given a type class with Unbound instances in ptype._unbound_types, this
    function will create a new subclass of that type with the unbound types
    replaced by those specified in ctypes.
    """
    ubtypes = ptype._unbound_types
    if len(ctypes) > len(ubtypes):
        raise TypeError("too many type parameters")
    tpairs = zip(ubtypes,ctypes)
    btype = _bind_rec(ptype,tpairs)
    if btype is not ptype:
        setattr(btype,"_unbound_types",ubtypes[len(ctypes):])
    return btype


def _bind_rec(ptype,tpairs):
    """Recursive component of bind() function.
 
    Given a type class 'ptype' and a list of (unbound,replacement) pairs,
    this function returns a subclass of ptype in which the appropraite
    replacements have been made. If no replacements are performed, ptype is
    returned unchanged.
    """
    #  Base case: directly replace an Unbound instance
    if isinstance(ptype,Unbound):
        for (ub,bt) in tpairs:
            if ptype is ub:
                return bt
        return ptype
    #  Primitive types can't have replacement performed
    if not ptype._types:
        return ptype
    #  Check whether replacement actually occurs, recursively
    types = tuple(_bind_rec(t,tpairs) for t in ptype._types)
    if types == ptype._types:
        return ptype
    #  Create the bound subclass
    class btype(ptype):
        _types = types
    #  If any attributes of ptype are types that have been modified,
    #  update the attributes of btype to match.
    for nm in dir(ptype):
        val = getattr(ptype,nm) 
        for (i,t) in enumerate(ptype._types):
            if val is t:
                setattr(btype,nm,types[i])
                break
    return btype

def unify_types(type1,type2):
    """Perform simplistic unification between types or type tuples.

    This is simplistic type unification, where instances of Unbound() in 
    one type are allowed to match concrete types in the other.  The result
    if either None (no unification is possible) or a list of (ub,bt) pairs
    giving the necessary substitutions.
    """
    return _unify_types_rec(type1,type2,[])

def _unify_types_rec(type1,type2,pairs):
    """Recursive accumulator implementation of unify_types."""
    if type1 is type2:
        return pairs
    #  Handle instances of Unbound
    if isinstance(type1,Unbound):
        for (ub,bt) in pairs:
            if ub is type1:
                if _unify_types_rec(bt,type2,pairs) is None:
                    return None
                break
        else:
            pairs.append((type1,type2))
        return pairs
    if isinstance(type2,Unbound):
        for (ub,bt) in pairs:
            if ub is type2:
                if _unify_types_rec(bt,type1,pairs) is None:
                    return None
                break
        else:
            pairs.append((type2,type1))
        return pairs
    #  Handle individual Type instances.
    #  They must both be instances, and their classes must unify
    if isinstance(type1,Type):
        if not isinstance(type2,Type):
            return None
        if _unify_types_rec(type1.__class__,type2.__class__,pairs) is None:
            return None
        if type1._types is not type1.__class__._types:
            if _unify_types_rec(type1._types,type2._types,pairs) is None:
                return None
        elif type2._types is not type2.__class__._types:
            if _unify_types_rec(type1._types,type2._types,pairs) is None:
                return None
        return pairs
    if isinstance(type2,Type):
        return None
    #  Handle Type subclasses.
    #  They must both be classes, and one a base class of the other.
    if _issubclass(type1,Type):
        if not _issubclass(type2,Type):
            return None
        if type1 not in type2.__mro__ and type2 not in type1.__mro__:
            return None
        return _unify_types_rec(type1._types,type2._types,pairs)
    if _issubclass(type2,Type):
        return None
    #  Handle tuples of types.
    #  They must be equal length and matching items must unify
    if len(type1) != len(type2):
        return None
    for (t1,t2) in zip(type1,type2):
        if _unify_types_rec(t1,t2,pairs) is None:
            return None
    return pairs

