"""

  extprot.serialize:  low-level serialization machinery for extprot

This module implements the low-level details of reading and writing extprot
bytestreams.  It's designed to be used both as a standard python module,
and to be compiled via Cython for a nice performance boost.

"""


try:
    import cython 
except ImportError:
    from extprot import fake_cython as cython

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

#  When compiled with cython, E_TYPE_* is an enum value.
#  This allows the dispatcher to be compiled as a swith.
for nm,val in globals().items():
    if nm.startswith("TYPE_"):
        globals()["_E_"+nm] = val
del nm
del val
    

#  Functions exposed from this module.
#  When compiled using Cython, these are the only functions available.

def from_string(string,typcls):
    """Parse an instance of the given typeclass from the given string."""
    s = StringStream(string)
    return s.read_value(typcls)

def from_file(file,typcls):
    """Parse an instance of the given typeclass from the given file."""
    s = FilelikeStream(file)
    return s.read_value(typcls)

def to_string(value,typcls):
    """Render an instance of the given typeclass into a string."""
    s = StringStream()
    s.write_value(value,typcls)
    return s.getstring()

def to_file(file,value,typcls):
    """Render an instance of the given typeclass into a file."""
    s = FilelikeStream(file)
    s.write_value(value,typcl)


class _Stream(object):
    """Abstract class for processing an extprot bytestream."""

    def read_value(self,typcls):
        """Read a generic value from the stream.

        This method takes a type class object and constructs a value for
        it by reading a value off the stream.

        If there are no more values in the stream, EOFError will be raised.
        """
        return self._read_value(typcls)

    def write_value(self,value,typcls):
        """Write a generic value to the stream."""
        self._write_value(value,typcls)

    def getstring(self):
        return self._getstring()

    def _read(self,size):
        raise NotImplementedError

    def _skip(self,size):
        raise NotImplementedError

    def _write(self,data):
        raise NotImplementedError

    def _getstring(self):
        raise NotImplementedError

    @cython.locals(prefix=cython.int,tag=cython.int,type=cython.int)
    def _read_value(self,typcls):
        try:
            prefix = self._read_small_int()
        except UnexpectedEOFError:
            raise EOFError
        type = prefix & 0xf
        tag = prefix >> 4
        #  We depend on Cython to turn this into a switch.
        #  In interpreted mode it would be better to use a dict of function
        #  objects, but I'll compromise in favour of compiled speed.
        #  TODO: it doesn't seem to output a switch yet...
        if type == _E_TYPE_VINT:
            value = self._read_int()
        elif type == _E_TYPE_TUPLE:
            items,subtypes = typcls._ep_parse_builder(type,tag)
            value = self._read_Tuple(items,subtypes,None)
        elif type == _E_TYPE_BITS8:
            value = self._read(1)
        elif type == _E_TYPE_BYTES:
            size = self._read_small_int()
            value = self._read(size)
        elif type == _E_TYPE_BITS32:
            value = _unpack_bits32(self._read(4))
        elif type ==_E_TYPE_HTUPLE:
            items,subtypes = typcls._ep_parse_builder(type,tag)
            value = self._read_HTuple(items,subtypes,None)
        elif type == _E_TYPE_BITS64_LONG:
            value = _unpack_bits64_long(self._read(8))
        elif type == _E_TYPE_ASSOC:
            items,subtypes = typcls._ep_parse_builder(type,tag)
            value = self._read_Assoc(items,subtypes,None)
        elif type == _E_TYPE_BITS64_FLOAT:
            value = _unpack_bits64_float(self._read(8))
        elif type == _E_TYPE_ENUM:
            value = None
        else:
            raise UnexpectedWireTypeError
        value = typcls._ep_parse(type,tag,value)
        return value

    @cython.locals(tag=cython.int,type=cython.int)
    def _write_value(self,value,typcls):
        (type,tag,value,subtypes) = typcls._ep_render(value)         
        self._write_small_int(tag << 4 | type)
        if type == _E_TYPE_VINT:
            self._write_int(value)
        elif type == _E_TYPE_TUPLE:
            self._write_Tuple(value,subtypes,None)
        elif type == _E_TYPE_BITS8:
            self._write(value)
        elif type == _E_TYPE_BYTES:
            self._write_small_int(len(value))
            self._write(value)
        elif type == _E_TYPE_BITS32:
            self._write(_pack_bits32(value))
        elif type == _E_TYPE_HTUPLE:
            self._write_HTuple(value,subtypes,None)
        elif type == _E_TYPE_BITS64_LONG:
            self._write(_pack_bits64_long(value))
        elif type == _E_TYPE_ASSOC:
            self._write_Assoc(value,subtypes,None)
        elif type == _E_TYPE_BITS64_FLOAT:
            self._write(_pack_bits64_float(value))
        elif type == _E_TYPE_ENUM:
            pass
        else:
            raise UnexpectedWireTypeError


    @cython.locals(tag=cython.int,type=cython.int)
    def _skip_value(self):
        """Efficiently skip over the next value in the stream.

        This method skips over the next value in the stream without needing
        to parse its internal structure.  It's more efficient than calling
        _read_value() and ignoring the result.

        If there is no value left on the stream, EOFError is raised.
        """
        try:
            prefix = self._read_small_int()
        except UnexpectedEOFError:
            raise EOFError
        type = prefix & 0xf
        tag = prefix >> 4
        #  If the LSB of the wiretype is 1, it is length-delimited.
        #  If not, it is a primitive type with known size.
        if type & 0x01:
            length = self._read_small_int()
            self._skip(length)
        elif type == _E_TYPE_VINT:
            self._read_int()
        elif type == _E_TYPE_BITS8:
            self._skip(1)
        elif type == _E_TYPE_BITS32:
            self._skip(4)
        elif type == _E_TYPE_BITS64_LONG:
            self._skip(8)
        elif type == _E_TYPE_BITS64_FLOAT:
            self._skip(8)
        elif type == _E_TYPE_ENUM:
            pass
        else:
            raise UnexpectedWireTypeError

    @cython.locals(b=cython.int,e=cython.longlong)
    def _read_int(self):
        """Read an integer encoded in vint format.""" 
        b = ord(self._read(1))
        if b < 128:
            return b
        x = e = 0
        while b >= 128:
            h = (b - 128)
            h = h << e
            x += h
            e += 7
            b = ord(self._read(1))
        h = b
        h = h << e
        x += h
        return x

    @cython.locals(b=cython.int,e=cython.longlong,x=cython.longlong)
    def _read_small_int(self):
        """Read a small integer encoded in vint format.

        This is just like _read_int but assumes the result will fit in a
        C longlong.  This allows a small extra performance boost from Cython.
        """ 
        b = ord(self._read(1))
        if b < 128:
            return b
        x = e = 0
        while b >= 128:
            x += (b - 128) << e
            e += 7
            b = ord(self._read(1))
        x += (b << e)
        return x

    @cython.locals(b=cython.int)
    def _write_int(self,x):
        """Write an integer encoded in vint format."""
        while x >= 128:
            b = x & 127
            self._write(chr(b | 128))
            x = x >> 7
        self._write(chr(x))

    @cython.locals(b=cython.short,x=cython.longlong)
    def _write_small_int(self,x):
        """Write a small integer encoded in vint format.

        This is just like _write_int but assumes the value will fit in a
        C longlong.  This allows a small extra performance boost from Cython.
        """ 
        while x >= 128:
            b = x & 127
            self._write(chr(b | 128))
            x = x >> 7
        self._write(chr(x))

    @cython.locals(ntypes=cython.longlong,nitems=cython.longlong)
    def _read_Tuple(self,items,subtypes,s):
        """Read a Tuple type from the stream.

        These are encoded as [length][num elements]<elements>.  The length
        field is used to read all the data into a string for faster parsing.
        """
        length = self._read_small_int()
        #  For small items it's quicker to read all the data into a string
        #  and parse it in memory than to do lots of small reads from the file.
        if length < 4096 and not isinstance(self,StringStream):
            s = StringStream(self._read(length))
        else:
            s = self
        ntypes = len(subtypes)
        nitems = s._read_small_int()
        if nitems <= ntypes:
            for i in xrange(nitems):
                items.append(s._read_value(subtypes[i]))
            for i in xrange(nitems,ntypes):
                items.append(subtypes[i]._ep_default())
        else:
            for i in xrange(ntypes):
                items.append(s._read_value(subtypes[i]))
            for i in xrange(ntypes,nitems):
                s._skip_value()
        return items

    @cython.locals(nitems=cython.longlong)
    def _write_Tuple(self,value,subtypes,s):
        """Write a Tuple type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_small_int(nitems)
        for i in xrange(nitems):
           s. _write_value(value[i],subtypes[i])
        data = s._getstring()
        self._write_small_int(len(data))
        self._write(data)

    @cython.locals(nitems=cython.longlong)
    def _read_HTuple(self,items,subtypes,s):
        """Read a HTuple type from the stream.

        These are encoded as [length][num elements]<elements>.
        """
        length = self._read_small_int()
        #  For small items it's quicker to read all the data into a string
        #  and parse it in memory than to do lots of small reads from the file.
        if length < 4096 and not isinstance(self,StringStream):
            s = StringStream(self._read(length))
        else:
            s = self
        nitems = s._read_small_int()
        for i in xrange(nitems):
            items.append(s._read_value(subtypes[0]))
        return items

    @cython.locals(nitems=cython.longlong)
    def _write_HTuple(self,value,subtypes,s):
        """Write a HTuple type to the stream."""
        s = StringStream()
        nitems = len(value)
        s._write_small_int(nitems)
        for i in xrange(nitems):
            s._write_value(value[i],subtypes[0])
        data = s._getstring()
        self._write_small_int(len(data))
        self._write(data)

    @cython.locals(npairs=cython.longlong)
    def _read_Assoc(self,items,subtypes,s):
        """Read an Assoc type from the stream.

        These are encoded as [length][num pairs]<pairs>.
        """
        length = self._read_small_int()
        #  For small items it's quicker to read all the data into a string
        #  and parse it in memory than to do lots of small reads from the file.
        if length < 4096 and not isinstance(self,StringStream):
            s = StringStream(self._read(length))
        else:
            s = self
        npairs = s._read_small_int()
        for i in xrange(npairs):
            key = s._read_value(subtypes[0])
            val = s._read_value(subtypes[1])
            items[key] = val
        return items

    @cython.locals(npairs=cython.longlong)
    def _write_Assoc(self,value,subtypes,s):
        """Write an Assoc type to the stream."""
        s = StringStream()
        npairs = len(value)
        s._write_small_int(npairs)
        for key,val in value.iteritems():
            s._write_value(key,subtypes[0])
            s._write_value(val,subtypes[1])
        data = s._getstring()
        self._write_small_int(len(data))
        self._write(data)


class FilelikeStream(_Stream):
    """_Stream implementation wrapping a filelike object."""

    def __init__(self,file):
        self.file = file

    def _read(self,size):
        data = self.file.read()
        if len(data) < size:
            raise UnexpectedEOFError
        return data

    def _skip(self,size):
        data = self.file.read()
        if len(data) < size:
            raise UnexpectedEOFError

    def _write(self,data):
        self.file.write(data)


class PyStringStream(_Stream):
    """Pure-python implementation of StringStream

    This version uses cStringIO to manage the data in memory.  The class
    CStringStream provides an optimized Cython version.
    """

    def __init__(self,value=None):
       if value is None:
           buffer = StringIO()
       else:
           buffer = StringIO(value)
       self.buffer = buffer

    def _read(self,size):
        data = self.buffer.read(size)
        if len(data) < size:
            raise UnexpectedEOFErrror
        return data

    def _skip(self,size):
        self.buffer.seek(size,1)

    def _write(self,data):
        self.buffer.write(data)

    def _getstring(self):
        return self.buffer.getvalue()



try:
    StringStream = CStringStream
except NameError:
    StringStream = PyStringStream


#  Pure-python implementations of some utility functions.
#  These are replaced by specialised Cython versions during compilation.


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

