"""

  extprot.types:  basic type classes for extprot protocol definitions.

This module defines classes and functions used to compose the in-memory
object structure corresponding to an extprot protocol.  They essentially
form a class-based reification of the extprot type system.  Type definitions
are represented by various subclasses of 'Type', while messages are subclasses
of the special type 'Message'.

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
 
However, it's probably more reliable to use the 'compiler' module to generate
this object structure automatically from a .proto souce file.

"""

import sys
import struct
from itertools import izip

from extprot.errors import *
from extprot.utils import TypedList, TypedDict
from extprot.stream import Stream, StringStream, TYPE_VINT, TYPE_BITS8, \
                           TYPE_BITS32, TYPE_BITS64_LONG, TYPE_BITS64_FLOAT, \
                           TYPE_ENUM, TYPE_TUPLE, TYPE_BYTES, TYPE_HTUPLE, \
                           TYPE_ASSOC


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

        _ep_convert:        convert a python value to standard type repr
        _ep_default:        get the default value for type, if any
        _ep_parse:          convert from primitive value to standard repr
        _ep_render:         convert from standard repr to primitive value
        _ep_get_primtype:   primitive type to use for serialising value
        _ep_from_primtype:  typeclass to use for deserialising given type
        _ep_tag:            tag number for disjoint union members
        _ep_types:          tuple of types from which type is composed

    If the type is composed from other types, the class attribute '_ep_types'
    will contain them as a tuple.  If the type is polymorphic, the class
    attribute '_ep_unbound_types' will contain a tupe of instances of the
    special type 'Unbound'.  These unbound types can later be instantiated
    using the 'bind' function from this module.
    """

    _ep_tag = 0
    _ep_types = ()
    _ep_unbound_types = ()

    @classmethod
    def _ep_convert(cls,value):
        """Convert a python value into internal representation."""
        return value

    @classmethod
    def _ep_convert_types(cls,values,types=None):
        """Convert a sequence of values using a type tuple.

        If no type tuple is given, cls._ep_types is used.  If there aren't
        enough values for the number of types, we try to use default values.
        """
        values = iter(values)
        if types is None:
            types = cls._ep_types
        for t in types:
            try:
                v = values.next()
            except StopIteration:
                yield t._ep_default()
            else:
                yield t._ep_convert(v)
        try:
            values.next()
        except StopIteration:
            pass
        else:
            raise ValueError("too many values to convert")

    @classmethod
    def _ep_default(cls):
        """Return the default value for this type."""
        raise UndefinedDefaultError

    @classmethod
    def _ep_parse(cls,value):
        return value

    @classmethod
    def _ep_render(cls,value):
        return value

    @classmethod
    def _ep_get_primtype(cls,value):
        raise NotImplementedError

    @classmethod
    def _ep_from_primtype(cls,type,tag):
        if type != cls._ep_get_primtype(None)[1]:
            raise UnexpectedWireTypeError
        return cls

    @classmethod
    def build(cls,*types):
        """Build an instance of this type using the given subtypes."""
        class Anon(cls):
            _ep_types = types
        return Anon

    @classmethod
    def from_stream(cls,stream):
        """Parse a value of this type from an extprot bytestream."""
        return stream.read_value(cls)

    @classmethod
    def from_string(cls,string):
        """Read a value of this type from a string."""
        s = StringStream(string)
        return cls.from_stream(s)

    @classmethod
    def from_file(cls,file):
        """Read a value of this type from a file-like object."""
        s = Stream.make_stream(file)
        return cls.from_stream(s)

    def __eq__(self,other):
        return self is other

    def __ne__(self,other):
        return not self == other


class Bool(Type):
    """Primitive boolean type."""

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_BITS8

    @classmethod
    def _ep_convert(cls,value):
        return bool(value)

    @classmethod
    def _ep_default(cls):
        return False

    @classmethod
    def _ep_parse(cls,value):
        return (value != "\x00")

    @classmethod
    def _ep_render(cls,value):
        if value:
            return "\x01"
        return "\x00"


class Byte(Type):
    """Primitive byte type, an 8-bit integer.

    The canonical representation is as a single-character string.
    """

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_BITS8

    @classmethod
    def _ep_convert(cls,value):
        try:
            return chr(value)
        except TypeError:
            if not isinstance(value,str):
                raise ValueError("not a valid Byte: " + repr(value))
            if len(value) != 1:
                raise ValueError("not a valid Byte: " + value)
            return value


class Int(Type):
    """Primitive signed integer type."""

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_VINT

    @classmethod 
    def _ep_convert(cls,value):
        if not isinstance(value,(int,long)):
            raise ValueError("not a valid Int: " + repr(value))
        return value

    @classmethod
    def _ep_parse(cls,value):
        if value % 2 == 0:
            return value // 2
        else:
            return value // -2

    @classmethod
    def _ep_render(cls,value):
        if value >= 0:
            return value * 2
        else:
            return (value * -2) - 1


class Long(Type):
    """Primitive 64-bit integer type."""

    _ep_max_long = 2**64

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_BITS64_LONG

    @classmethod
    def _ep_convert(cls,value):
        packed = int(value)
        if packed > self._ep_max_long:
            raise ValueError("too big for a long: " + repr(packed))



class Float(Type):
    """Primitive 64-bit float type."""

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_BITS64_FLOAT

    @classmethod
    def _ep_convert(cls,value):
        # TODO: a better way to convert/check float types?
        try:
            return struct.unpack("<d",struct.pack("<d",value))[0]
        except struct.error:
            raise ValueError("not a Float")


class String(Type):
    """Primitive byte-string type."""

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_BYTES

    @classmethod
    def _ep_convert(cls,value):
        if isinstance(value,unicode):
            value = value.encode("ascii")
        elif not isinstance(value,str):
            raise ValueError("not a valid String: " + repr(value))
        return value


class Tuple(Type):
    """Composed tuple type.

    Sublcasses of Tuple represent tuples typed according to cls._ep_types.
    To dynamically build a particular tuple type use the 'build' method
    like so:

        int3 = Tuple.build(Int,Int,Int)
        int_and_str = Tuple.build(Int,String)

    """

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_TUPLE

    @classmethod
    def _ep_convert(cls,value):
        try:
            return tuple(cls._ep_convert_types(value))
        except TypeError:
            raise ValueError("not a valid Tuple")

    @classmethod
    def _ep_default(cls):
        return tuple(t.default() for t in self._ep_types)



class List(Type):
    """Composed homogeneous list type.

    To dynamically construct a particular list type, use the 'build' method
    like so:

        list_of_ints = List.build(Int)
        list_of_2ints = List.build(Tuple.build(Int,Int))

    """

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_HTUPLE

    @classmethod
    def _ep_convert(cls,value):
        try:
            return TypedList(cls._ep_types[0], value)
        except TypeError:
            raise ValueError("not a valid List")

    @classmethod
    def _ep_default(cls):
        return TypedList(cls._ep_types[0])


class Array(Type):
    """Composed homogeneous array type.

    To dynamically construct a particular array type, use the 'build' method
    like so:

        array_of_ints = Array.build(Int)
        array_of_2ints = Array.build(Tuple.build(Int,Int))

    """

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_HTUPLE

    @classmethod
    def _ep_convert(cls,value):
        try:
            return TypedList(cls._ep_types[0], value)
        except TypeError:
            raise ValueError("not a valid List")

    @classmethod
    def _ep_default(cls):
        return TypedList(cls._ep_types[0])



class _UnionMetaclass(type):
    """Metaclass for Union type.

    This metaclass is responsible for populating Union._ep_types with a tuple
    of the declared option types, and Union._ep_tag_map with a mapping from
    (type,tag) values to individual Option or Message type classes.
    """

    def __new__(mcls,name,bases,attrs):
        cls = super(_UnionMetaclass,mcls).__new__(mcls,name,bases,attrs)
        #  Find all attributes that are Option or Message classes, and
        #  sort them into cls._ep_types tuple.
        #  TODO: what about subclasses of a Union subclass that declare
        #        additional options?  Fortunately I don't think the parser
        #        will ever generate such a structure.
        if "_ep_types" not in attrs:
            types = []
            is_message_union = False
            is_option_union = False
            for val in attrs.itervalues():
                if _issubclass(val,Option):
                    if is_message_union:
                       raise TypeError("cant union Option and Message")
                    is_option_union = True
                    types.append((val._ep_creation_order,val))
                elif _issubclass(val,Message):
                    if is_option_union:
                        raise TypeError("cant union Option and Message")
                    is_message_union = True
                    types.append((val._ep_creation_order,val))
                elif _issubclass(val,Type) or isinstance(val,Type):
                    raise TypeError("only Option and Message allowed in Union")
            types.sort()
            cls._ep_types = tuple(t for (_,t) in types)
            #  Label each with their tag in the union.
            #  The tag space for enums and tuple options is distinct.
            e_idx = 0
            t_idx = 0
            for t in cls._ep_types:
                if _issubclass(t,Option) and not t._ep_types:
                    t._ep_tag = e_idx
                    e_idx += 1
                else:
                    t._ep_tag = t_idx
                    t_idx += 1
                #  Adjust __name__ and __module__ to allow pickling
                if _issubclass(t,Message):
                    mod = sys.modules.get(t.__module__)
                    if getattr(mod,t.__name__,None) is not t:
                        t.__name__ = cls.__name__+"."+t.__name__
                        t.__module__ = cls.__module__
        cls._ep_tag_map = {}
        for t in cls._ep_types:
            cls._ep_tag_map[(t._ep_get_primtype(None)[1],t._ep_tag)] = t
        return cls


class _OptionMetaclass(type):
    """Metaclass for Option type.

    This metaclass is responsible for populating Option._ep_creation_order with
    an increasing number indicating the order in which subclasses were created.
    """

    _ep_creation_counter = 0

    def __new__(mcls,name,bases,attrs):
        cls = super(_OptionMetaclass,mcls).__new__(mcls,name,bases,attrs)
        cls._ep_creation_order = mcls._ep_creation_counter
        mcls._ep_creation_counter += 1
        return cls

    def __len__(self):
        return 0


class Option(Type):
    """Individual tagged entry in a Union type.

    Unlike other Type subclasses, Option classes are designed to be
    directly instantiated in order to tag the contained values.  The
    values contained in an instance can be obtained using standard 
    item access (e.g. opt[0], opt[1], etc).
    """

    __metaclass__ = _OptionMetaclass

    @classmethod
    def _ep_get_primtype(cls,value):
        if not cls._ep_types:
            return cls,TYPE_ENUM
        return cls,TYPE_TUPLE

    def __new__(cls,*values):
        """Custom instance constructor to special-case constant options.

        For Option subclasses that don't contain any values, this returns the
        class itself rather than an instance.
        """
        if not cls._ep_types:
            if values:
                raise ValueError("values given to constant Option constructor")
            return cls
        else:
            return Type.__new__(cls)

    def __init__(self,*values):
        self._ep_values = tuple(self._ep_convert_types(values))

    def __getitem__(self,index):
        return self._ep_values[index]

    def __setitem__(self,index,value):
        self._ep_values[index] = self._ep_types[index]._ep_convert(value)

    def __len__(self):
        return len(self._ep_values)

    def __eq__(self,other):
        return self._ep_values == other._ep_values

    @classmethod
    def _ep_convert(cls,value):
        if isinstance(value,cls):
            return value
        if _issubclass(value,cls):
            if value._ep_types:
                raise ValueError("no data given to non-constant Option")
            return value
        #  To support easy syntax for polymorphic union types, we also
        #  accept instances of any of our base classes, as long as they
        #  have a compatible type signature.
        if isinstance(value,Option):
            if value.__class__ in cls.__mro__:
                return cls(*value._ep_values)
            if unify_types(cls._ep_types,value.__class__._ep_types) is not None:
                return cls(*value._ep_values)
        elif _issubclass(value,Option):
            if value._ep_types:
                raise ValueError("no data given to non-constant Option")
            if not cls._ep_types and value  in cls.__mro__:
                return cls
        #  Not an Option class or instance, must be a direct value tuple.
        #  A scalar is converted into a tuple of length 1.
        if isinstance(value,basestring):
            value = (value,)
        else:
            try:
                value = tuple(value)
            except TypeError:
                value = (value,)
        return cls(*value)



class Field(Type):
    """Type representing a field on a Message.

    Field instances implement the descriptor protocol to map a given
    type to a name on a Message instance.  They're designed to be used
    as follows:

        class msg(Message):
            title = Field(String)
            contents = Field(List.build(String),mutable=True)

    Through some metaclass magic on the Message class, Field instances come
    to know the name (self._ep_name) and index (self._ep_index) by which they
    are attached to a message.
    """

    _ep_creation_counter = 0

    def __init__(self,type,mutable=False):
        self._ep_creation_order = Field._ep_creation_counter
        Field._ep_creation_counter += 1
        self._ep_type = type
        self.mutable = mutable

    def __get__(self,obj,type=None):
        if obj is None:
            return self
        try:
            value = obj.__dict__[self._ep_name]
        except KeyError:
            value = self._ep_type._ep_default()
            obj.__dict__[self._ep_name] = value
        return value

    def __set__(self,obj,value):
        if not self.mutable and obj._ep_initialized:
            raise AttributeError("Field '"+self._ep_name+"' is not mutable")
        if value is None:
            try:
                value = self._ep_type._ep_default()
            except UndefinedDefaultError:
                msg = "value required for field " + self._ep_name
                raise UndefinedDefaultError(msg)
        obj.__dict__[self._ep_name] = self._ep_type._ep_convert(value)

    def _ep_convert(self):
        return self._ep_type._ep_convert()

    def _ep_default(self):
        return self._ep_type._ep_default()

    def _ep_parse(self,value):
        return self._ep_type._ep_parse(value)

    def _ep_render(self,value):
        return self._ep_type._ep_render(value)

    def _ep_get_primtype(self,value):
        return self._ep_type._ep_get_primtype(value)

    def _ep_from_primtype(self,type,tag):
        return self._ep_type._ep_from_primtype(type,tag)

    @property
    def _ep_types(self):
        return self._ep_type._ep_types


class _MessageMetaclass(type):
    """Metaclass for message type.

    This metaclass is responsible for populating Message._ep_creation_order
    with an increasing number indicating the order in which subclasses were
    created, setting the _ep_name and _ep_index properties on contained Field
    instances, and creating cls._ep_types as a tuple of contained fields.
    """

    _ep_creation_counter = 0

    def __new__(mcls,name,bases,attrs):
        cls = super(_MessageMetaclass,mcls).__new__(mcls,name,bases,attrs)
        cls._ep_creation_order = mcls._ep_creation_counter
        mcls._ep_creation_counter += 1
        #  Find all attributes that are Field instances, sort in creation order.
        types = []
        names = {}
        for (name,val) in attrs.iteritems():
            if isinstance(val,Field):
                types.append((val._ep_creation_order,name,val))
                names[name] = True
        types.sort()
        #  Find all base Field instances that haven't been overridden.
        btypes = []
        for base in bases:
            if issubclass(base,Message):
                for t in base._ep_types:
                    if t._ep_name not in names:
                        btypes.append(t)
                        names[t._ep_name] = True
        #  Merge types and base_types into the final types tuple.
        cls._ep_types = tuple(t for t in btypes)
        cls._ep_types = cls._ep_types + tuple(t for (_,_,t) in types)
        #  Label each field with its name and index in the message
        for (i,(_,nm,t)) in enumerate(types):
            t._ep_index = i + len(btypes)
            t._ep_name = nm
        return cls


class Message(Type):
    """Fake composed message type.

    This class exists only to fake out the _MessageMetaclass logic while
    the real Message class is being created.  _MessageMetaclass checks
    for things that are subclasses of Message, which it obviously can't
    do for the Message class itself.
    """
    pass


class Message(Type):
    """Composed message type.

    This is the basic unit of data transfer in extprot, and is basically
    a set of typed key-value pairs.
    """

    __metaclass__ = _MessageMetaclass

    @classmethod
    def _ep_get_primtype(cls,value):
        return cls,TYPE_TUPLE

    def __init__(self,*args,**kwds):
        self._ep_initialized = False
        #  Process positional and keyword arguments as Field values
        if len(args) > len(self._ep_types):
            raise TypeError("too many positional arguments to Message")
        for (t,v) in zip(self._ep_types,args):
            t.__set__(self,v)
        for t in self._ep_types[len(args):]:
            try:
                v = kwds.pop(t._ep_name)
            except KeyError:
                t.__set__(self,None)
            else:
                t.__set__(self,v)
        if kwds:
            raise TypeError("too many keyword arguments to Message")
        self._ep_initialized = True

    @classmethod
    def _ep_convert(cls,value):
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
    def _ep_default(cls):
        return cls()

    @classmethod
    def _ep_parse(cls,value):
        return cls(*value)

    @classmethod
    def _ep_render(cls,value):
        return [value.__dict__[t._ep_name] for t in cls._ep_types]

    def to_stream(self,stream):
        """Serialize this message to an extprot bytestream."""
        stream.write_value(self.__class__,self)

    def to_string(self):
        """Serialize this message to a string."""
        s = StringStream()
        self.to_stream(s)
        return s.getstring()

    def to_file(self,file):
        """Serialize this message to a file-like object."""
        self.to_stream(Stream.make_stream(file))

    def __eq__(self,msg):
        if self.__class__ != msg.__class__:
            return False
        for t in self._ep_types:
            if t.__get__(self) != t.__get__(msg):
                return False
        return True

    def __reduce__(self):
        """Pickle Messages by serializing them.

        For this to work, we have to be able to find the Message class
        after unpickling.  This means that self.__class__.__name__ and
        self.__class__.__module__ must be set to something useful. In
        particuler, __name__ on inner classes must be set to the full
        dotted name of the class.
        """
        cls = self.__class__
        args = (cls.__module__,cls.__name__,self.to_string())
        return (_unpickle_message,args)


def _unpickle_message(module,name,data):
    """Helper function for unpickling of Message insances."""
    mname = module.split(".")[-1]
    cls = __import__(module,fromlist=[mname])
    for nm in name.split("."):
        cls = getattr(cls,nm)
    return cls.from_string(data)
_unpickle_message.__safe_for_unpickling__ = True


class Union(Type):
    """Composed disjoint-union type.

    Subclasses of this type expose the different tags of the union as
    class-level attributes.  This is easiest to explain by example.  For
    the following union type:

        type stuff = Watzit | Thing string int

   The appropriate class structure definition would be:

        class stuff(Union):
            class Watzit(Option):
                _ep_types = ()
            class Thing(Option):
                _ep_types = (String,Int)

    Through some metaclass magic, stuff._ep_types will come to contain the
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
    def _ep_convert(cls,value):
        for t in cls._ep_types:
            try:
                v = t._ep_convert(value)
                return v
            except ValueError, e:
                pass
        raise ValueError("could not convert value " + repr(value) + " to Union type " + repr(cls))

    @classmethod
    def _ep_default(cls):
        for t in cls._ep_types:
            if _issubclass(t,Message):
                return t._ep_default()
            if not t._ep_types:
                return t
        raise UndefinedDefaultError

    @classmethod
    def _ep_get_primtype(cls,value):
        return value._ep_get_primtype(value)

    @classmethod
    def _ep_from_primtype(cls,type,tag):
        try:
            return cls._ep_tag_map[(type,tag)]
        except KeyError:
            raise UnexpectedWireTypeError

   
class Unbound(Type):
    """Unbound type, for representing type parameters.

    Attempts to use this class in serialization raise errors.  It's intended
    use is for individual instances to represent types that are not yet bound
    in polymorphic type declaration (i.e. to appear in cls._ep_unbound_types).
    """

    @classmethod
    def _ep_parse(self,value):
        raise TypeError("parametric type not bound")

    @classmethod
    def _ep_render(self,value):
        raise TypeError("parametric type not bound")

   
class Placeholder(Type):
    """Placeholder type, for representing not-yet-defined type names.

    Attempts to use this class in serialization raise errors.  Its intended
    use is for parsing routines to use it as a placeholder for typenames that
    haven't yet been defined.
    """

    def __init__(self,name):
        self.name = name

    @classmethod
    def _ep_parse(self,value):
        raise TypeError("parametric type not bound")

    @classmethod
    def _ep_render(self,value):
        raise TypeError("parametric type not bound")


def bind(ptype,*ctypes):
    """Dynamically bind unbound type parameters to create a new type class.

    Given a type class with Unbound instances in ptype._ep_unbound_types, this
    function will create a new subclass of that type with the unbound types
    replaced by those specified in ctypes.
    """
    ubtypes = ptype._ep_unbound_types
    if len(ctypes) > len(ubtypes):
        raise TypeError("too many type parameters")
    tpairs = zip(ubtypes,ctypes)
    btype = _bind_rec(ptype,tpairs)
    if btype is not ptype:
        setattr(btype,"_ep_unbound_types",ubtypes[len(ctypes):])
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
    if not ptype._ep_types:
        return ptype
    #  Check whether replacement actually occurs, recursively
    types = tuple(_bind_rec(t,tpairs) for t in ptype._ep_types)
    if types == ptype._ep_types:
        return ptype
    #  Create the bound subclass
    class btype(ptype):
        _ep_types = types
    #  If any attributes of ptype are types that have been modified,
    #  update the attributes of btype to match.
    for nm in dir(ptype):
        val = getattr(ptype,nm) 
        for (i,t) in enumerate(ptype._ep_types):
            if val is t:
                setattr(btype,nm,types[i])
                break
    return btype

def unify_types(type1,type2):
    """Perform simplistic unification between types or type tuples.

    This is simplistic type unification, where instances of Unbound() in 
    one type are allowed to match concrete types in the other.  The result
    is either None (no unification is possible) or a list of (ub,bt) pairs
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
        if type1._ep_types is not type1.__class__._ep_types:
            if _unify_types_rec(type1._ep_types,type2._ep_types,pairs) is None:
                return None
        elif type2._ep_types is not type2.__class__._ep_types:
            if _unify_types_rec(type1._ep_types,type2._ep_types,pairs) is None:
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
        return _unify_types_rec(type1._ep_types,type2._ep_types,pairs)
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


def resolve_placeholders(type):
    """Iterator for resolving Placeholder() types.

    This function is a generator producing (name,setter) pairs, where
    'name' is the name from a Placeholder instance attached to the given
    type object, and 'setter' is a function that should be called with the
    resolved value for that placeholder.  Parsing code should drive it in
    a loop like this:

        for (name,setter) in resolve_placeholders(mytype):
            setter(defined_names[name])
 
    """
    new_types = []
    try:
        t2 = type._ep_type
    except AttributeError:
        pass
    else:
        if isinstance(t2,Placeholder):
            result = []
            def setter(value):
                result.append(value)
            yield (t2.name,setter)
            if result:
                type._ep_type = result[0]
        else:
            for sub in resolve_placeholders(t2):
                yield sub
    for t2 in type._ep_types:
        if isinstance(t2,Placeholder):
            result = []
            def setter(value):
                result.append(value)
            yield (t2.name,setter)
            if result:
                new_types.append(result[0])
            else:
                new_types.append(t2)
        else:
            for sub in resolve_placeholders(t2):
                yield sub
            new_types.append(t2)
    new_types = tuple(new_types)
    if type._ep_types != new_types:
        type._ep_types = new_types
 

