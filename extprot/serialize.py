"""

  extprot.serialize:  low-level serialization machinery for extprot

This module implements the low-level details of reading and writing extprot
bytestreams.  It's the pure-python "baseline" version; you should also have
a Cython-generated version of this module named "_serialize" which is much
faster.

"""


import struct
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from extprot.errors import *

TYPE_VINT = 0
TYPE_BITS8 = 2
TYPE_BITS32 = 4
TYPE_BITS64_LONG = 6
TYPE_BITS64_FLOAT = 8
TYPE_ENUM = 10
TYPE_TUPLE = 1
TYPE_BYTES = 3
TYPE_HTUPLE = 5
TYPE_ASSOC = 7


_S_BITS32 = struct.Struct("<L")
_S_BITS64_LONG = struct.Struct("<Q")
_S_BITS64_FLOAT = struct.Struct("<d")


def from_string(string,typcls):
    """Parse an instance of the given typeclass from the given string."""
    s = StringStream(string)
    return s.read_value(typcls._ep_typedesc)

def from_file(file,typcls):
    """Parse an instance of the given typeclass from the given file."""
    s = Stream(file)
    return s.read_value(typcls._ep_typedesc)

def to_string(value,typcls):
    """Render an instance of the given typeclass into a string."""
    s = StringStream()
    s.write_value(value,typcls._ep_typedesc)
    return s.getstring()

def to_file(file,value,typcls):
    """Render an instance of the given typeclass into a file."""
    s = Stream(file)
    s.write_value(value,typcls._ep_typedesc)


class _InfiniteTuple(object):
    """A simulated infinite-length tuple with the same item at each index."""
    def __init__(self,item):
        self.item = item
    def __getitem__(self,i):
        return self.item


class TypeDesc(object):
    """Object used to direct the serialization process.

    Instances of TypeDesc are used to direct the serialization process.
    Method hooks on this class are called at appropriate times during parsing
    or rendering.

    The base class provides no useful functionality, it just errors out.
    Subclasses provide behaviour specialised to the base extprot types,
    and calling code can feel free to provide any custom behaviour it needs.

    This is broken off into a separate class so that the cython-based parser
    can implement the common methods in C.
    """

    #  Constructor functions called to create a new collection for
    #  compound types.  This must be a dict indexed by (type,tag) tuple.
    collection_constructor = {}

    #  Tuple of TypeDesc objects used when parsing children for a
    #  compound type.  It's a mapping as for collection_constructor.
    subtypes = {}

    def parse_value(self,value,type,tag):
        """Finalize parsing of a value from the extprot bytestream.

        This method will be called when a new value has been read off the
        bytestream.  Given the primitive type, tag and value, it should
        return the final object to be returned from the parser.
        """
        raise NotImplementedError

    def render_value(self,value):
        """Convert value to tagged primitive type for rendering.

        This method will be called when a value is about to be written out.
        It must convert the value to something renderable, and return a
        3-tuple giving the value, type and tag.
        """
        raise NotImplementedError

    def default_value(self,value):
        """Construct default value for this type.

        This method will be called whenever a default value of this type 
        is required.
        """
        raise UndefinedDefaultError



class SingleTypeDesc(TypeDesc):
    """TypeDesc class for types with a single type tag.

    This is a typedesc suitable for use with most primitive types, where
    there is a single type and tag regardless of the value.
    """

    type = 0
    tag = 0
    collection_constructor = {}
    subtypes = {}

    def parse_value(self,value,type,tag):
        if type != self.type:
            raise UnexpectedWireTypeError
        return value

    def render_value(self,value):
        return (value,self.type,self.tag)


class BoolTypeDesc(SingleTypeDesc):
    """TypeDesc class for boolean-like types."""

    def parse_value(self,value,type,tag):
        value = SingleTypeDesc.parse_value(self,value,type,tag)
        return (value != "\x00")

    def render_value(self,value):
        if value:
            value = "\x01"
        else:
            value = "\x00"
        return SingleTypeDesc.render_value(self,value)

    def default_value(self):
        return False


class IntTypeDesc(SingleTypeDesc):
    """TypeDesc class for integer-like types."""

    def parse_value(self,value,type,tag):
        value = SingleTypeDesc.parse_value(self,value,type,tag)
        if value % 2:
            return value // -2
        else:
            return value // 2

    def render_value(self,value):
        if value >= 0:
            value = value * 2
        else:
            value = (value * -2) - 1
        return SingleTypeDesc.render_value(self,value)


class TupleTypeDesc(SingleTypeDesc):
    """TypeDesc class for tuple-like types."""

    def parse_value(self,value,type,tag):
        #  Try to parse it as a proper tuple type
        subtypes = self.subtypes[(self.type,self.tag)]
        if type == self.type:
            if len(value) < len(subtypes):
                for t in subtypes[len(value):]:
                    value.append(t.default_value())
            return tuple(value)
        #  Try to promote it from a primitive type to the first tuple item.
        if not subtypes:
            err = "could not promote primitive to Tuple type"
            raise ParseError(err)
        else:
            values = [subtypes[0].parse_value(value,type,tag)]
            for t in subtypes[1:]:
                values.append(t.default_value())
            return tuple(values)

    def default_value(self):
        values = []
        subtypes = self.subtypes[(self.type,self.tag)]
        for t in subtypes:
            values.append(t.default_value())
        return tuple(values)


class MessageTypeDesc(TupleTypeDesc):
    """TypeDesc class for message types."""

    def parse_value(self,value,type,tag):
        value = TupleTypeDesc.parse_value(self,value,type,tag)
        #  Bypass typechecking by initialising the values before calling
        #  __init__.  We already know the types are valid.
        inst = self.type_class.__new__(self.type_class)
        for i in xrange(min(len(inst._ep_fields),len(value))):
            inst.__dict__[inst._ep_fields[i]._ep_name] = value[i]
        inst._ep_initialized = True
        inst.__init__(*value)
        return inst

    def render_value(self,value):
        value = [value.__dict__[f._ep_name] for f in self.type_class._ep_fields]
        return TupleTypeDesc.render_value(self,value)

    def default_value(self):
        return self.type_class()



class Stream(object):
    """Base class for processing an extprot bytestream.

    Instances of this class are used to read or write objects to a generic
    filelike object.  A specialized subclass StringString is used when
    writing to an in-memory string.
    """

    def __init__(self,file):
        self.file = file

    def read_value(self,typdesc):
        """Read a generic value from the stream.

        This method takes a type description object and constructs a value
        for it by reading a value off the stream.

        If there are no more values in the stream, EOFError will be raised.
        """
        try:
            prefix = self._read_int()
        except UnexpectedEOFError:
            raise EOFError
        type = prefix & 0xf
        tag = prefix >> 4
        #  If the LSB of the wiretype is 1, it is length-delimited.
        #  If not, it is a primitive type with known size.
        if type & 0x01:
            length = self._read_int()
            if type == TYPE_BYTES:
                value = self._read(length)
            else:
                #  For small items it's quicker to read all the data into a
                #  string and parse it in memory than to do many small reads.
                if length < 4096 and not isinstance(self,StringStream):
                    s = StringStream(self._read(length))
                else:
                    s = self
                try:
                    items = typdesc.collection_constructor[(type,tag)]()
                except KeyError:
                    raise UnexpectedWireTypeError
                try:
                    subtypes = typdesc.subtypes[(type,tag)]
                except KeyError:
                    raise UnexpectedWireTypeError
                if type == TYPE_TUPLE:
                    value = s._read_Tuple(items,subtypes)
                elif type == TYPE_ASSOC:
                    value = s._read_Assoc(items,subtypes)
                elif type == TYPE_HTUPLE:
                    value = s._read_HTuple(items,subtypes)
                else:
                    raise UnexpectedWireTypeError
        else:
            if type == TYPE_VINT:
                value = self._read_int()
            elif type == TYPE_BITS8:
                value = self._read(1)
            elif type == TYPE_BITS32:
                value = _S_BITS32.unpack(self._read(4))[0]
            elif type == TYPE_BITS64_LONG:
                value = _S_BITS64_LONG.unpack(self._read(8))[0]
            elif type == TYPE_BITS64_FLOAT:
                value = _S_BITS64_FLOAT.unpack(self._read(8))[0]
            elif type == TYPE_ENUM:
                value = None
            else:
                raise UnexpectedWireTypeError
        value = typdesc.parse_value(value,type,tag)
        return value

    def write_value(self,value,typdesc):
        """Write a generic value to the stream.

        This method takes a value and the typclass with which to render it,
        and writes that value onto the stream.
        """
        (value,type,tag) = typdesc.render_value(value)
        self._write_int(tag << 4 | type)
        if type == TYPE_VINT:
            self._write_int(value)
        elif type == TYPE_TUPLE:
            subtypes = typdesc.subtypes[(type,tag)]
            self._write_Tuple(value,subtypes)
        elif type == TYPE_BITS8:
            self._write(value)
        elif type == TYPE_BYTES:
            self._write_int(len(value))
            self._write(value)
        elif type == TYPE_BITS32:
            self._write(_S_BITS32.pack(value))
        elif type == TYPE_HTUPLE:
            subtypes = typdesc.subtypes[(type,tag)]
            self._write_HTuple(value,subtypes)
        elif type == TYPE_BITS64_LONG:
            self._write(_S_BITS64_LONG.pack(value))
        elif type == TYPE_ASSOC:
            subtypes = typdesc.subtypes[(type,tag)]
            self._write_Assoc(value,subtypes)
        elif type == TYPE_BITS64_FLOAT:
            self._write(_S_BITS64_FLOAT.pack(value))
        elif type == TYPE_ENUM:
            pass
        else:
            raise UnexpectedWireTypeError

    def skip_value(self):
        """Efficiently skip over the next value in the stream.

        This method skips over the next value in the stream without needing
        to parse its internal structure.  It's more efficient than calling
        read_value() and ignoring the result.

        If there is no value left on the stream, EOFError is raised.
        """
        try:
            prefix = self._read_int()
        except UnexpectedEOFError:
            raise EOFError
        type = prefix & 0xf
        tag = prefix >> 4
        #  If the LSB of the wiretype is 1, it is length-delimited.
        #  If not, it is a primitive type with known size.
        if type & 0x01:
            length = self._read_int()
            self._skip(length)
        elif type == TYPE_VINT:
            self._read_int()
        elif type == TYPE_BITS8:
            self._skip(1)
        elif type == TYPE_BITS32:
            self._skip(4)
        elif type == TYPE_BITS64_LONG:
            self._skip(8)
        elif type == TYPE_BITS64_FLOAT:
            self._skip(8)
        elif type == TYPE_ENUM:
            pass
        else:
            raise UnexpectedWireTypeError

    def getstring(self):
        """Get the contents of the stream as a string, if possible."""
        raise NotImplementedError

    def _read(self,size):
        data = self.file.read(size)
        if len(data) < size:
            raise UnexpectedEOFError
        return data

    def _skip(self,size):
        data = self.file.read(size)
        if len(data) < size:
            raise UnexpectedEOFError

    def _write(self,data):
        self.file.write(data)

    def _read_int(self):
        """Read an integer encoded in vint format.""" 
        b = ord(self._read(1))
        if b < 128:
            return b
        x = e = 0
        while b >= 128:
            x += (b - 128) << e
            e += 7
            b = ord(self._read(1))
        x += b << e
        return x

    def _write_int(self,x):
        """Write an integer encoded in vint format."""
        while x >= 128:
            b = x & 127
            self._write(chr(b | 128))
            x = x >> 7
        self._write(chr(x))

    def _read_Tuple(self,items,subtypes):
        """Read a Tuple type from the stream."""
        nitems = self._read_int()
        ntypes = len(subtypes)
        if nitems <= ntypes:
            for i in xrange(nitems):
                items.append(self.read_value(subtypes[i]))
        else:
            for i in xrange(ntypes):
                items.append(self.read_value(subtypes[i]))
            for i in xrange(ntypes,nitems):
                self.skip_value()
        return items

    def _write_Tuple(self,value,subtypes):
        """Write a Tuple type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_int(nitems)
        ntypes = len(subtypes)
        for i in xrange(nitems):
           s.write_value(value[i],subtypes[i])
        data = s.getstring()
        self._write_int(len(data))
        self._write(data)

    def _read_HTuple(self,items,subtypes):
        """Read a HTuple type from the stream."""
        nitems = self._read_int()
        ntypes = len(subtypes)
        for i in xrange(nitems):
            items.append(self.read_value(subtypes[i % ntypes]))
        return items

    def _write_HTuple(self,value,subtypes):
        """Write a HTuple type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_int(nitems)
        ntypes = len(subtypes)
        for i in xrange(nitems):
            s.write_value(value[i],subtypes[i % ntypes])
        data = s.getstring()
        self._write_int(len(data))
        self._write(data)

    def _read_Assoc(self,items,subtypes):
        """Read an Assoc type from the stream."""
        nitems = self._read_int()
        ntypes = len(subtypes)
        for i in xrange(nitems):
            key = self.read_value(subtypes[(2*i) % ntypes])
            val = self.read_value(subtypes[(2*i + 1) % ntypes])
            items[key] = val
        return items

    def _write_Assoc(self,value,subtypes):
        """Write an Assoc type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_int(nitems)
        ntypes = len(subtypes)
        for i,(key,val) in enumerate(value.iteritems()):
            s.write_value(key,subtypes[(2*i) % ntypes])
            s.write_value(val,subtypes[(2*i + 1) % ntypes])
        data = s.getstring()
        self._write_int(len(data))
        self._write(data)



class StringStream(Stream):
    """Special-purpose implementation of Stream for parsing strings.

    This implementation uses cStringIO to manage the data in memory.
    """

    def __init__(self,value=None):
       if value is None:
           file = StringIO()
       else:
           file = StringIO(value)
       super(StringStream,self).__init__(file)

    def getstring(self):
        return self.file.getvalue()

