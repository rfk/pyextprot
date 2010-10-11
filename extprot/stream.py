"""

  extprot.stream:  low-level machinery to serialise to/from streams

This module implements the low-level details of reading and writing extprot
bytestreams.  It's designed to be used both as a standard python module,
and to be compiled via Cython for a nice performance boost.

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


class Stream(object):
    """Generic extprot reader/writer class.

    This class allows serialization of extprot objects to/from a generic
    file-like stream using only its read or write methods.

    For specific types of stream there may be a more efficient subclass
    available (e.g. StringStream or FileStream).  To get the best stream
    for a given source object, use the classmethod Stream.make_stream.
    """

    def __init__(self,stream):
        self.stream = stream

    @staticmethod
    def make_stream(target):
        if isinstance(target,basestring):
            return StringStream(target)
        if isinstance(target,file):
            return FileStream(target)
        return Stream(target)

    #  Low-level stream access methods.  Subclasses might like to
    #  provide more efficient implementations of these.

    def read(self,size):
        """Read a given number of raw bytes from the stream."""
        b = self.stream.read(size)
        if len(b) != size:
            raise UnexpectedEOFError
        return b

    def write(self,data):
        """Write the given bytes to the stream."""
        self.stream.write(data)

    def skip_bytes(self,length):
        """Skip a given number of raw bytes from the stream."""
        self.read(length)

    #  Low-level utility methods.
    #  Maybe subclasses can implement these more efficiently?

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

    def read_prefix(self):
        """Read a (type,tag) prefix byte from the stream."""
        prefix = self.read_int()
        type = prefix & 0xf
        tag = prefix >> 4
        return (type,tag)

    def write_prefix(self,type,tag=0):
        """Write a value prefix with the given type and optional tag."""
        self.write_int(tag << 4 | type)

    #  Generic serializing/deserializing methods.
    #  Subclasses shouldn't need to touch these.

    def read_value(self,typcls):
        """Read a generic value from the stream.

        This method takes a type class object and constructs an instance
        of it by reading a value off the stream.

        If there are no more values in the stream, EOFError will be raised.
        """
        try:
            (type,tag) = self.read_prefix()
        except UnexpectedEOFError:
            raise EOFError
        # TODO: promotion from primitive types
        typcls = typcls._ep_from_primtype(type,tag)
        #  We depend on Cython to turn this into a switch.
        #  In interpreted mode it would be better to use a dict of
        #  function objects, but I'll compromise for compiled speed.
        if type == TYPE_VINT:
            value = self.read_Vint(typcls)
        elif type == TYPE_TUPLE:
            value = self.read_Tuple(typcls)
        elif type == TYPE_BITS8:
            value = self.read_Bits8(typcls)
        elif type == TYPE_BYTES:
            value = self.read_Bytes(typcls)
        elif type == TYPE_BITS32:
            value = self.read_Bits32(typcls)
        elif type == TYPE_HTUPLE:
            value = self.read_HTuple(typcls)
        elif type == TYPE_BITS64_LONG:
            value = self.read_Bits64_long(typcls)
        elif type == TYPE_ASSOC:
            value = self.read_Assoc(typcls)
        elif type == TYPE_BITS64_FLOAT:
            value = self.read_Bits64_float(typcls)
        elif type == TYPE_ENUM:
            value = typcls
        else:
            raise UnexpectedWireTypeError
        return typcls._ep_parse(value)

    def write_value(self,typcls,value):
        """Write a generic value to the stream."""
        (typcls,type) = typcls._ep_get_primtype(value)
        tag = typcls._ep_tag
        print "WRITE", typcls, type, value, tag
        self.write_prefix(type,tag)
        value = typcls._ep_render(value)
        if type == TYPE_VINT:
            self.write_Vint(typcls,value)
        elif type == TYPE_TUPLE:
            self.write_Tuple(typcls,value)
        elif type == TYPE_BITS8:
            self.write_Bits8(typcls,value)
        elif type == TYPE_BYTES:
            self.write_Bytes(typcls,value)
        elif type == TYPE_BITS32:
            self.write_Bits32(typcls,value)
        elif type == TYPE_HTUPLE:
            self.write_HTuple(typcls,value)
        elif type == TYPE_BITS64_LONG:
            self.write_Bits64_long(typcls,value)
        elif type == TYPE_ASSOC:
            self.write_Assoc(typcls,value)
        elif type == TYPE_BITS64_FLOAT:
            self.write_Bits64_float(typcls,value)
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
            (type,tag) = self.read_prefix()
        except UnexpectedEOFError:
            raise EOFError
        #  If the LSB of the wiretype is 1, it is length-delimited.
        #  If not, it is a primitive type with known size.
        if type & 0x01:
            length = self.read_int()
            self.skip_bytes(length)
        elif type == TYPE_VINT:
            self.read_int()
        elif type == TYPE_BITS8:
            self.skip_bytes(1)
        elif type == TYPE_BITS32:
            self.skip_bytes(4)
        elif type == TYPE_BITS64_LONG:
            self.skip_bytes(8)
        elif type == TYPE_BITS64_FLOAT:
            self.skip_bytes(8)
        elif type == TYPE_ENUM:
            pass
        else:
            raise UnexpectedWireTypeError

    def read_Vint(self,typcls):
        """Read a Vint from the stream."""
        return self.read_int()

    def write_Vint(self,typcls,value):
        """Write a Vint to the stream."""
        self.write_int(value)

    def read_Bits8(self,typcls):
        """Read a single byte from the stream."""
        return self.read(1)

    def write_Bits8(self,typcls,value):
        """Write a single byte to the stream."""
        self.write(value)

    def read_Bits32(self,typcls):
        """Read a 32-bit integer from the stream."""
        b = self.read(4)
        return _unpack_bits32(b)

    def write_Bits32(self,typcls,value):
        """Write a 32-bit integer to the stream."""
        self.write(_pack_bits32(value))

    def read_Bits64_long(self,typcls):
        """Read a 64-bit integer from the stream."""
        b = self.read(8)
        return _unpack_bits64_long(b)

    def write_Bits64_long(self,typcls,value):
        """Write a 64-bit integer to the stream."""
        self.write(_pack_bits64_long(value))

    def read_Bits64_float(self,typcls):
        """Read a 64-bit float from the stream."""
        b = self.read(8)
        return _unpack_bits64_float(b)

    def write_Bits64_float(self,typcls,value):
        """Write a 64-bit float to the stream."""
        self.write(_pack_bits64_float(value))

    def read_Bytes(self,typcls):
        """Read a byte string from the stream.

        Byte strings are encoded as [length]<bytes>.
        """
        size = self.read_int()
        return self.read(size)

    def write_Bytes(self,typcls,value):
        """Write a byte string to the stream."""
        self.write_int(len(value))
        self.write(value)

    def read_Tuple(self,typcls):
        """Read a Tuple type from the stream.

        These are encoded as [length][num elements]<elements>.  The length
        field is ignored (it's used to skip over unwanted values).
        """
        # TODO: convertion from primitive type to Tuple
        length = self.read_int()
        #  For small items it's quicker to read all the data into a string
        #  and parse it in memory than to do lots of small reads from the file.
        if length < 4096:
            stream = StringStream(self.read(length))
        else:
            stream = self
        ntypes = len(typcls._ep_types)
        nitems = stream.read_int()
        items = []
        if nitems <= ntypes:
            for i in xrange(nitems):
                items.append(stream.read_value(typcls._ep_types[i]))
            for i in xrange(nitems,ntypes):
                items.append(typcls._ep_types[i].default())
        else:
            for i in xrange(ntypes):
                items.append(stream.read_value(typcls._ep_types[i]))
            for i in xrange(ntypes,nitems):
                stream.skip_value()
        return items

    def write_Tuple(self,typcls,value):
        """Write a Tuple type to the stream."""
        sub = StringStream()
        nitems = len(value)
        sub.write_int(nitems)
        for i in xrange(nitems):
            sub.write_value(typcls._ep_types[i],value[i])
        data = sub.getstring()
        self.write_int(len(data))
        self.write(data)

    def read_HTuple(self,typcls):
        """Read a HTuple type from the stream.

        These are encoded as [length][num elements]<elements>.  The length
        field is ignored (it's used to skip over unwanted values).
        """
        length = self.read_int()
        #  For small items it's quicker to read all the data into a string
        #  and parse it in memory than to do lots of small reads from the file.
        if length < 4096:
            stream = StringStream(self.read(length))
        else:
            stream = self
        nitems = stream.read_int()
        items = typcls._ep_default()
        for i in xrange(nitems):
            items.append(stream.read_value(typcls._ep_types[0]))
        return items

    def write_HTuple(self,typcls,value):
        """Write a HTuple type to the stream."""
        sub = StringStream()
        nitems = len(value)
        sub.write_int(nitems)
        for item in value:
            sub.write_value(typcls._ep_types[0],item)
        data = sub.getstring()
        self.write_int(len(data))
        self.write(data)

    def read_Assoc(self,typcls):
        """Read an Assoc type from the stream.

        These are encoded as [length][num pairs]<pairs>.  The length
        field is ignored (it's used to skip over unwanted values).
        """
        length = self.read_int()
        #  For small items it's quicker to read all the data into a string
        #  and parse it in memory than to do lots of small reads from the file.
        if length < 4096:
            stream = StringStream(self.read(length))
        else:
            stream = self
        npairs = stream.read_int()
        items = typcls._ep_default()
        for i in xrange(npairs):
            key = stream.read_value(typcls._ep_types[0])
            val = stream.read_value(typcls._ep_types[1])
            items[key] = val
        return items

    def write_Assoc(self,typcls,value):
        """Write an Assoc type to the stream."""
        sub = StringStream()
        nitems = len(items)
        sub.write_int(nitems)
        for key,val in value.iteritems():
            sub.write_value(typcls._ep_types[0],key)
            sub.write_value(typcls._ep_types[1],val)
        data = sub.getstring()
        self.write_int(len(data))
        self.write(data)



class StringStream(Stream):
    """Stream subclass that wraps a StringIO instance.

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


class FileStream(Stream):
    """Stream subclass that wraps a standard file object.

    When not compiled with Cython, this class is identical to the base Stream
    class.  However, the Cython-compiled version can be native C functions for
    reading the file which can speed up IO performance.
    """
    pass



def _unpack_bits32(b):
    return struct.unpack("<L",b)[0]

def _pack_bits32(b):
    return struct.pack("<L",b)

def _unpack_bits64_long(b):
    return struct.unpack("<Q",b)[0]

def _pack_bits64_long(b):
    return struct.pack("<Q",b)

def _unpack_bits64_float(b):
    return struct.unpack("<d",b)[0]

def _pack_bits64_float(b):
    return struct.pack("<d",b)

