"""

  extprot:  classes for working with extprot bytestreams

This module implements the low-level details of reading and writing extprot
bytestreams.  The class 'Stream' wraps a readable/writable file-like object
and provides methods such as read_Vint, write_Vint, etc for working with 
the primitive types of the extprot encoding.

The class StringStream is a simple shortcut to wrap a Stream around a
StringIO object.

"""

import struct
from StringIO import StringIO

from extprot.errors import *


class Stream(object):
    """Object for manipulating an extprot bytestream."""

    def __init__(self,stream):
        self.stream = stream

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

    def read(self,size):
        """Read a given number of raw bytes from the stream."""
        b = self.stream.read(size)
        if len(b) != size:
            raise UnexpectedEOFError
        return b

    def write(self,data):
        self.stream.write(data)

    def read_values(self):
        """Iterator over all values in the stream."""
        try:
            while True:
                yield self.read_value()
        except EOFError:
            pass

    def read_value(self,prefix=None):
        """Read a generic value from the stream.

        If there are no more values, EOFError will be raised.
        """
        #  For efficiency's sake we dispatch on the value of the wire_type
        #  rather than using a big if-elif chain.
        if prefix is None:
            try:
                prefix = self.read_prefix()
            except UnexpectedEOFError:
                # Actually, I expected that one
                raise EOFError
        wire_type = prefix & 0xf
        return self._LL_TYPES_READ[wire_type](self,prefix)

    def get_tag(self,prefix):
        """Extract the tag from an encoded prefix.

        The prefix is (tag << 4 | wire_type), so this simply shifts the
        prefix back 4 bytes to the right.
        """
        return prefix >> 4

    def read_int(self):
        """Read an integer encoded in vint format.""" 
        b = ord(self.read(1))
        if b < 128:
            return b
        x = e = 0
        while b >= 128:
            x += (b - 128) << e
            e += 7
            b = ord(self.read(1))
        x += (b << e)
        return x

    def write_int(self,x):
        """Write an integer encoded in vint format."""
        while x >= 128:
            b = x & 127  # lowest 7 bits
            self.write(chr(b | 128))  # write with high bit set
            x = x >> 7  # discard processed bits
        self.write(chr(x))

    def read_prefix(self,types=None):
        """Read a value prefix, maybe checking its type."""
        prefix = self.read_int()
        if types is not None:
            self.check_prefix_type(prefix,types)
        return prefix

    def write_prefix(self,type,tag=0):
        """Write a value prefix with the given type and optional tag."""
        self.write_int(tag << 4 | type)

    def check_prefix_type(self,prefix,types):
        """Check the type of the given prefix byte."""
        type = prefix & 0x0f
        if isinstance(types,int):
            if type != types:
                raise UnexpectedWireTypeError
        else:
            if type not in types:
                raise UnexpectedWireTypeError
    
    def read_array(self,prefix=None):
        """Read an array of items from the stream.

        These are encoded as [length][num elements]<elements>.  The length
        field is ignored (it's used to skip over unwanted values).
        """
        if prefix is None:
            prefix = self.read_prefix((self.TYPE_TUPLE,self.TYPE_HTUPLE))
        else:
            self.check_prefix_type(prefix,(self.TYPE_TUPLE,self.TYPE_HTUPLE))
        self.read_int() # length is ignored
        nelms = self.read_int()
        return [self.read_value() for _ in xrange(nelems)]

    def read_Vint(self,prefix=None):
        """Read a Vint from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_VINT)
        else:
            self.check_prefix_type(prefix,self.TYPE_VINT)
        return self.read_int()

    def write_Vint(self,data):
        self.write_prefix(self.TYPE_VINT)
        self.write_int(data)

    def read_Bits8(self,prefix=None):
        """Read a single byte from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_BITS8)
        else:
            self.check_prefix_type(prefix,self.TYPE_BITS8)
        return self.read(1)

    def write_Bits8(self,data):
        self.write_prefix(self.TYPE_BITS8)
        self.write(data)

    def read_Bits32(self,prefix=None):
        """Read a 32-bit integer from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_BITS32)
        else:
            self.check_prefix_type(prefix,self.TYPE_BITS32)
        b = self.read(4)
        return struct.unpack("<L",b)[0]

    def write_Bits32(self,data):
        self.write_prefix(self.TYPE_BITS32)
        self.write(struct.pack("<L",data))

    def read_Bits64_long(self,prefix=None):
        """Read a 64-bit integer from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_BITS64_LONG)
        else:
            self.check_prefix_type(prefix,self.TYPE_BITS64_LONG)
        b = self.read(8)
        return struct.unpack("<Q",b)[0]

    def write_Bits64_long(self,data):
        self.write_prefix(self.TYPE_BITS64_LONG)
        self.write(struct.pack("<Q",data))

    def read_Bits64_float(self,prefix=None):
        """Read a 64-bit float from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_BITS64_FLOAT)
        else:
            self.check_prefix_type(prefix,self.TYPE_BITS64_FLOAT)
        b = self.read(8)
        return struct.unpack("<d",b)[0]

    def write_Bits64_float(self,data):
        self.write_prefix(self.TYPE_BITS64_FLOAT)
        self.write(struct.pack("<d",data))

    def read_Enum(self,prefix=None):
        """Read a tagged Enum from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_ENUM)
        else:
            self.check_prefix_type(prefix,self.TYPE_ENUM)
        return prefix

    def write_Enum(self,data):
        self.write_prefix(self.TYPE_ENUM,data)

    def read_Bytes(self,prefix=None):
        """Read a byte string from the stream.

        Byte strings are encoded as [length]<bytes>.
        """
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_BYTES)
        else:
            self.check_prefix_type(prefix,self.TYPE_BYTES)
        size = self.read_int()
        return self.read(size)

    def write_Bytes(self,data):
        self.write_prefix(self.TYPE_BYTES)
        self.write_int(len(data))
        self.write(data)

    def read_Tuple(self,prefix=None):
        """Read a Tuple type from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_TUPLE)
        else:
            self.check_prefix_type(prefix,self.TYPE_TUPLE)
        tag = self.get_tag(prefix)
        return (tag,self.read_array(prefix))

    def write_Tuple(self,tag,items):
        self.write_prefix(self.TYPE_TUPLE,tag)
        sub = StringStream()
        items = list(items)
        sub.write_int(len(items))
        for (func,v) in items:
            func(v,sub)
        self.write_int(len(sub.getstring()))
        self.write(sub.getstring())

    def read_HTuple(self,prefix=None):
        """Read a HTuple type from the stream."""
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_HTUPLE)
        else:
            self.check_prefix_type(prefix,self.TYPE_HTUPLE)
        tag = self.get_tag(prefix)
        return (tag,self.read_array(prefix))

    def write_HTuple(self,tag,items):
        self.write_prefix(self.TYPE_HTUPLE,tag)
        sub = StringStream()
        items = list(items)
        sub.write_int(len(items))
        for (func,v) in items:
            func(v,sub)
        self.write_int(len(sub.getstring()))
        self.write(sub.getstring())

    def read_Assoc(self,prefix=None):
        """Read an Assoc type from the stream.

        These are encoded as [length][num pairs]<pairs>.  The length
        field is ignored (it's used to skip over unwanted values).
        """
        if prefix is None:
            prefix = self.read_prefix(self.TYPE_ASSOC)
        else:
            self.check_prefix_type(prefix,self.TYPE_ASSOC)
        tag = self.get_tag(prefix)
        self.read_int()  # length is ignored
        npairs = self.read_int()
        pairs = ((self.read_value(),self.read_value()) for _ in xrange(npairs))
        return (tag,dict(pairs))

    def write_Assoc(self,tag,items):
        self.write_prefix(self.TYPE_ASSOC,tag)
        sub = StringStream()
        items = list(items)
        sub.write_int(len(items))
        for (func,v) in items:
            func(v,sub)
        self.write_int(len(sub.getstring()))
        self.write(sub.getstring())

    def read_Invalid(self,prefix=None):
        """Raise an error since there's nothing valid to read."""
        raise UnexpectedWireTypeError

    _LL_TYPES_READ = (
        read_Vint, read_Tuple, read_Bits8, read_Bytes, read_Bits32,
        read_HTuple, read_Bits64_long, read_Assoc, read_Bits64_float,
        read_Invalid, read_Enum, read_Invalid, read_Invalid, read_Invalid,
        read_Invalid, read_Invalid,
    )

    def skip_value(self):
        # TODO: skip using length markers
        self.read_value()


class StringStream(Stream):
    """Simple Stream subclass that wraps a StringIO instance.

    The method getstring() returns the current string value of the stream,
    while reset() will position the stream back at position zero.
    """

    def __init__(self,string=None):
        if string is not None:
            stream = StringIO(string)
        else:
            stream = StringIO()
        super(StringStream,self).__init__(stream)

    def getstring(self):
        return self.stream.getvalue()

    def reset(self):
        self.stream.seek(0)


