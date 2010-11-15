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
    return s.read_value(typcls)

def from_file(file,typcls):
    """Parse an instance of the given typeclass from the given file."""
    s = Stream(file)
    return s.read_value(typcls)

def to_string(value,typcls):
    """Render an instance of the given typeclass into a string."""
    s = StringStream()
    s.write_value(value,typcls)
    return s.getstring()

def to_file(file,value,typcls):
    """Render an instance of the given typeclass into a file."""
    s = Stream(file)
    s.write_value(value,typcls)


class TypeMetaclass(type):
    """Metaclass for type classes, which direct the serialization process.

    This real guts of the type class structure is in extprot.types; this is
    just here for some typedefs in the parser.
    """

    def _ep_parse(cls,type,tag,value):
        """Convert a primitive type to standard repr for this type."""
        if type != cls._ep_prim_type:
            raise UnexpectedWireTypeError
        return value

    def _ep_parse_builder(cls,type,tag,nitems):
        """Get collection and subtypes for using during parsing."""
        raise UnexpectedWireTypeError

    def _ep_render(cls,value):
        """Convert value to tagged primitive type for rendering.

        This method returns a 4-tuple (type,tag,value,subtypes) where type
        if the primitive type identitier, tag is the value tag, value is
        the actual primitive value, and subtypes is a tuple of types to
        use for recursive rendering.
        """
        return (cls._ep_prim_type,cls._ep_tag,value,cls._types)


class Stream(object):
    """Base class for processing an extprot bytestream."""

    def __init__(self,file):
        self.file = file

    def read_value(self,typcls):
        """Read a generic value from the stream.

        This method takes a type class object and constructs a value for
        it by reading a value off the stream.

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
                nitems = s._read_int()
                items,subtypes = typcls._ep_parse_builder(type,tag,nitems)
                if type == TYPE_TUPLE:
                    value = s._read_Tuple(nitems,items,subtypes)
                elif type == TYPE_ASSOC:
                    value = s._read_Assoc(nitems,items,subtypes)
                elif type == TYPE_HTUPLE:
                    value = s._read_HTuple(nitems,items,subtypes)
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
        value = typcls._ep_parse(type,tag,value)
        return value

    def write_value(self,value,typcls):
        """Write a generic value to the stream.

        This method takes a value and the typclass with which to render it,
        and writes that value onto the stream.
        """
        (type,tag,value,subtypes) = typcls._ep_render(value)         
        self._write_int(tag << 4 | type)
        if type == TYPE_VINT:
            self._write_int(value)
        elif type == TYPE_TUPLE:
            self._write_Tuple(value,subtypes)
        elif type == TYPE_BITS8:
            self._write(value)
        elif type == TYPE_BYTES:
            self._write_int(len(value))
            self._write(value)
        elif type == TYPE_BITS32:
            self._write(_S_BITS32.pack(value))
        elif type == TYPE_HTUPLE:
            self._write_HTuple(value,subtypes)
        elif type == TYPE_BITS64_LONG:
            self._write(_S_BITS64_LONG.pack(value))
        elif type == TYPE_ASSOC:
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
        """Get the content of the strean as a string, if possible."""
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

    def _read_Tuple(self,nitems,items,subtypes):
        """Read a Tuple type from the stream."""
        ntypes = len(subtypes)
        if nitems <= ntypes:
            for i in xrange(nitems):
                items.append(self.read_value(subtypes[i]))
            for i in xrange(nitems,ntypes):
                items.append(subtypes[i]._ep_default())
        else:
            for i in xrange(ntypes):
                items.append(self.read_value(subtypes[i]))
            for i in xrange(ntypes,nitems):
                s.skip_value()
        return items

    def _write_Tuple(self,value,subtypes):
        """Write a Tuple type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_int(nitems)
        for i in xrange(nitems):
           s.write_value(value[i],subtypes[i])
        data = s.getstring()
        self._write_int(len(data))
        self._write(data)

    def _read_HTuple(self,nitems,items,subtypes):
        """Read a HTuple type from the stream."""
        #  TODO: This is an awful hack for backwards-compatability of some
        #        of my old code which wrote different types into an HTUPLE.
        #        It will be removed eventually.
        ntypes = len(subtypes)
        for i in xrange(nitems):
            items.append(self.read_value(subtypes[i % ntypes]))
        return items

    def _write_HTuple(self,value,subtypes):
        """Write a HTuple type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_int(nitems)
        #  TODO: This is an awful hack for backwards-compatability of some
        #        of my old code which wrote different types into an HTUPLE.
        #        It will be removed eventually.
        ntypes = len(subtypes)
        for i in xrange(nitems):
            s.write_value(value[i],subtypes[i % ntypes])
        data = s.getstring()
        self._write_int(len(data))
        self._write(data)

    def _read_Assoc(self,nitems,items,subtypes):
        """Read an Assoc type from the stream."""
        for i in xrange(nitems):
            key = self.read_value(subtypes[0])
            val = self.read_value(subtypes[1])
            items[key] = val
        return items

    def _write_Assoc(self,value,subtypes):
        """Write an Assoc type to the stream."""
        s = StringStream()
        npairs = len(value)
        s._write_int(npairs)
        for key,val in value.iteritems():
            s.write_value(key,subtypes[0])
            s.write_value(val,subtypes[1])
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

