"""

  extprot._serialize:  low-level serialization machinery for extprot

This module implements the low-level details of reading and writing extprot
bytestreams.  It's a Cython-generated C extension module.  If it's not 
working for you, just use the pure-python "serialize" module instead.

"""

import struct

from extprot.errors import *

cdef extern from "stdlib.h":
    ctypedef unsigned long size_t
    void free(void *ptr)
    void *malloc(size_t size)
    void *realloc(void *ptr, size_t size)
    void *memcpy(void *dest, void *src, size_t n)

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)
    char* PyString_AsString(object string)

cdef extern from "object.h":
    ctypedef class __builtin__.type [object PyHeapTypeObject]:
        pass
 

#  We expose the various TYPE_* constants as Python ingtegers for other
#  modules to use, but internally we use an Enum so they can be optimised.

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

cdef enum TypeID:
    _E_TYPE_VINT = 0
    _E_TYPE_BITS8 = 2
    _E_TYPE_BITS32 = 4
    _E_TYPE_BITS64_LONG = 6
    _E_TYPE_BITS64_FLOAT = 8
    _E_TYPE_ENUM = 10
    _E_TYPE_TUPLE = 1
    _E_TYPE_BYTES = 3
    _E_TYPE_HTUPLE = 5
    _E_TYPE_ASSOC = 7


# TODO: pack/unpack floats natively instead of shelling out to struct module.
_S_BITS64_FLOAT = struct.Struct("<d")


cdef class TypeMetaclass(type):
    """Metaclass for type classes, which direct the serialization process.

    This real guts of the type class structure is in extprot.types; this is
    just here for some typedefs in the parser.  It also provides a generic
    implementation of typeclass methods which can be used to parse/render
    just about any sensible python value.
    """

    cpdef _ep_parse(cls,int type,long long tag,value):
        """Convert a primitive type to standard repr for this type."""
        return value

    cpdef tuple _ep_parse_builder(cls,int type,long long tag,long long nitems):
        """Get collection and subtypes for using during parsing."""
        if type == _E_TYPE_ASSOC:
            return ({},(cls,cls))
        elif type == _E_TYPE_HTUPLE:
            return ([],(cls,))
        else:
            return ([],tuple([cls for _ in nitems]))

    cpdef tuple _ep_render(cls,value):
        """Convert value to tagged primitive type for rendering.

        This method returns a 4-tuple (type,tag,value,subtypes) where type
        if the primitive type identitier, tag is the value tag, value is
        the actual primitive value, and subtypes is a tuple of types to
        use for recursive rendering.
        """
        if isinstance(value,basestring):
            return (_E_TYPE_BYTES,0,value,())
        if isinstance(value,(long,int)):
            return (_E_TYPE_INT,0,value,())
        if isinstance(value,float):
            return (_E_TYPE_BITS64_FLOAT,0,value,())
        if isinstance(value,tuple):
            return (_E_TYPE_TUPLE,0,value,[cls])
        if isinstance(value,list):
            return (_E_TYPE_HTUPLE,0,value,(cls,))
        if isinstance(value,dict):
            return (_E_TYPE_ASSOC,0,value,(cls,cls))
        raise ValueError("can't render value: %s" % (value,))



cdef class Stream(object):
    """Base class for processing an extprot bytestream."""

    cdef object file

    def __init__(self,file):
        self.file = file

    def read_value(self,TypeMetaclass typcls):
        """Read a generic value from the stream.

        This method takes a type class object and constructs a value for
        it by reading a value off the stream.

        If there are no more values in the stream, EOFError will be raised.
        """
        return self._read_value(typcls)

    def write_value(self,value,TypeMetaclass typcls):
        """Write a generic value to the stream."""
        self._write_value(value,typcls)

    def getstring(self):
        return self._getstring()

    cdef _getstring(self):
        raise NotImplementedError

    cdef _read(self,long long size):
        """Read a python string from the stream."""
        data = self.file.read(size)
        if len(data) < size:
            raise UnexpectedEOFError
        return data

    cdef unsigned char _read_char(self):
        """Read a single character from the stream."""
        data = self.file.read(1)
        if not data:
            raise UnexpectedEOFError
        return PyString_AsString(data)[0]

    cdef void _skip(self,long long size):
        """Skip the given number of bytes from the stream."""
        data = self.file.read(size)
        if len(data) < size:
            raise UnexpectedEOFError

    cdef void _write(self,data):
        """Write a Python string to the stream."""
        self.file.write(data)

    cdef void _write_char(self,char c):
        """Write a single character to the stream."""
        s = PyString_FromStringAndSize(&c,1)
        self.file.write(s)

    cdef _read_value(self,TypeMetaclass typcls):
        cdef long long prefix, tag, length, nitems
        cdef short type
        cdef long vi32
        cdef long long vi64
        cdef char* data
        cdef tuple subtypes
        cdef Stream s
        try:
            prefix = self._read_small_int()
        except UnexpectedEOFError:
            raise EOFError
        type = prefix & 0xf
        tag = prefix >> 4
        #  In theory we should be able to produce a switch here, but
        #  it's not working for me.  Instead, branch on whether its delimited.
        #  If the LSB of the wiretype is 1, it is length-delimited.
        #  If not, it is a primitive type with known size.
        if type & 0x01:
            length = self._read_small_int()
            if type == _E_TYPE_BYTES:
                value = self._read(length)
            else:
                s = self._get_substream(length)
                nitems = s._read_small_int()
                items,subtypes = typcls._ep_parse_builder(type,tag,nitems)
                if type == _E_TYPE_TUPLE:
                    value = s._read_Tuple(nitems,items,subtypes)
                elif type == _E_TYPE_ASSOC:
                    value = s._read_Assoc(nitems,items,subtypes)
                elif type == _E_TYPE_HTUPLE:
                    value = s._read_HTuple(nitems,items,subtypes)
                else:
                    raise UnexpectedWireTypeError
        else:
            if type == _E_TYPE_VINT:
                value = self._read_int()
            elif type == _E_TYPE_BITS8:
                value = self._read(1)
            elif type == _E_TYPE_BITS32:
                # TODO: more efficient unpacking of 32-bit integers.
                p_data = self._read(4)
                data = p_data
                vi32 = <long>data[0]
                vi32 += <long>data[1] << 8
                vi32 += <long>data[2] << 16
                vi32 += <long>data[3] << 24
                value = vi32
            elif type == _E_TYPE_BITS64_LONG:
                # TODO: more efficient unpacking of 64-bit integers.
                p_data = self._read(8)
                data = p_data
                vi64 = <long long>data[0]
                vi64 += <long long>data[1] << 8
                vi64 += <long long>data[2] << 16
                vi64 += <long long>data[3] << 24
                vi64 += <long long>data[4] << 32
                vi64 += <long long>data[5] << 40
                vi64 += <long long>data[6] << 48
                vi64 += <long long>data[7] << 56
                value = vi64
            elif type == _E_TYPE_BITS64_FLOAT:
                # TODO: more efficient unpacking of 64-bit floats.
                value = _S_BITS64_FLOAT.unpack(self._read(8))[0]
            elif type == _E_TYPE_ENUM:
                value = None
            else:
                raise UnexpectedWireTypeError
        value = typcls._ep_parse(type,tag,value)
        return value

    cdef _write_value(self,value,TypeMetaclass typcls):
        cdef long long tag
        cdef short type
        cdef long long vi64
        cdef long vi32
        (type,tag,value,subtypes) = typcls._ep_render(value)         
        self._write_small_int(tag << 4 | type)
        if type == _E_TYPE_VINT:
            self._write_int(value)
        elif type == _E_TYPE_TUPLE:
            self._write_Tuple(value,subtypes)
        elif type == _E_TYPE_BITS8:
            self._write(value)
        elif type == _E_TYPE_BYTES:
            self._write_int(len(value))
            self._write(value)
        elif type == _E_TYPE_BITS32:
            vi32 = value
            self._write_char(vi32 & 0xff)
            self._write_char((vi32 >> 8) & 0xff)
            self._write_char((vi32 >> 16) & 0xff)
            self._write_char((vi32 >> 24) & 0xff)
        elif type == _E_TYPE_HTUPLE:
            self._write_HTuple(value,subtypes)
        elif type == _E_TYPE_BITS64_LONG:
            vi64 = value
            self._write_char(vi64 & 0xff)
            self._write_char((vi64 >> 8) & 0xff)
            self._write_char((vi64 >> 16) & 0xff)
            self._write_char((vi64 >> 24) & 0xff)
            self._write_char((vi64 >> 32) & 0xff)
            self._write_char((vi64 >> 40) & 0xff)
            self._write_char((vi64 >> 48) & 0xff)
            self._write_char((vi64 >> 56) & 0xff)
        elif type == _E_TYPE_ASSOC:
            self._write_Assoc(value,subtypes)
        elif type == TYPE_BITS64_FLOAT:
            data = _S_BITS64_FLOAT.pack(value)
            self._write(data)
        elif type == _E_TYPE_ENUM:
            pass
        else:
            raise UnexpectedWireTypeError


    cdef _skip_value(self):
        """Efficiently skip over the next value in the stream.

        This method skips over the next value in the stream without needing
        to parse its internal structure.  It's more efficient than calling
        _read_value() and ignoring the result.

        If there is no value left on the stream, EOFError is raised.
        """
        cdef long long prefix, tag, length
        cdef short type
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
        elif type == TYPE_BITS8:
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

    cdef _read_int(self):
        """Read an integer encoded in vint format.""" 
        cdef unsigned short b
        cdef unsigned long long lx, lh, le
        cdef object x, h, e
        b = <unsigned short>self._read_char()
        if b < 128:
            return b
        #  Read as much as we can using C long longs
        lx = le = 0
        while lx < 144115188075855871LLU: # (2**(64-7)-1)
            if b < 128:
                lh = b
                lh = lh << le
                lx += lh
                return lx
            lh = b - 128
            lh = lh << le
            lx += lh
            le += 7
            b = <unsigned short>self._read_char()
        #  We're about to overflow lx, switch to a Python long
        x = lx
        while le < 9223372036854775808LLU: #(2**63)
            if b < 128:
                h = b
                h = h << le
                x += h
                return x
            h = b - 128
            h = h << le
            x += h
            le += 7
            b = <unsigned short>self._read_char()
        #  We're about to overflow le, switch to a Python long
        e = le
        while b >= 128:
            h = b - 128
            h = h << e
            x += h
            e += 7
            b = <unsigned short>self._read_char()
        h = b
        h = h << e
        x += h
        return x

    cdef long long _read_small_int(self):
        """Read a small integer encoded in vint format.

        This is just like _read_int except it assumes the result will fit
        in a C long long.  Useful for reading sizes, counts etc.
        """ 
        cdef unsigned int b
        cdef unsigned long long x, h, e
        b = <unsigned short>self._read_char()
        if b < 128:
            return b
        x = e = 0
        while b >= 128:
            h = b - 128
            h = h << e
            x += h
            e += 7
            b = <unsigned short>self._read_char()
        h = b
        h = h << e
        x += h
        return x

    cdef _write_int(self,x):
        """Write an integer encoded in vint format."""
        cdef int b
        cdef unsigned long long lx
        #  Work with python longs only while we must.
        while x >= 144115188075855871LLU: # (2**(64-7)-1)
            b = x & 127
            self._write_char(b | 128)
            x = x >> 7
        #  Once small enough, switch to a C long long.
        lx = x
        while lx >= 128:
            b = lx & 127
            self._write_char(b | 128)
            lx = lx >> 7
        self._write_char(lx)

    cdef _write_small_int(self,long long x):
        """Write a small integer encoded in vint format.

        This is just like _write_int except it assumes the input will fit
        in a C long long.
        """
        cdef int b
        while x >= 128:
            b = x & 127
            self._write_char(b | 128)
            x = x >> 7
        self._write_char(x)

    cdef Stream _get_substream(self,long long length):
        """Get a stream from which to read the next 'length' bytes.

        For file-based streams, it's usually more efficient to read all the
        bytes into a StringStream and parse them in memory.
        """
        if length < 4096:
            return StringStream(self._read(length))
        return self

    cdef _read_Tuple(self,long long nitems,items,tuple subtypes):
        """Read a Tuple type from the stream."""
        cdef long long ntypes, i
        ntypes = len(subtypes)
        if nitems <= ntypes:
            for i in xrange(nitems):
                items.append(self._read_value(subtypes[i]))
            for i in xrange(nitems,ntypes):
                items.append(subtypes[i]._ep_default())
        else:
            for i in xrange(ntypes):
                items.append(self._read_value(subtypes[i]))
            for i in xrange(ntypes,nitems):
                self._skip_value()
        return items

    cdef _write_Tuple(self,value,subtypes):
        """Write a Tuple type to the stream."""
        cdef long long nitems, i
        cdef Stream s
        s = StringStream()
        nitems = len(value)
        s._write_small_int(nitems)
        for i in xrange(nitems):
           s. _write_value(value[i],subtypes[i])
        data = s._getstring()
        self._write_small_int(len(data))
        self._write(data)

    cdef _read_HTuple(self,long long nitems,items,tuple subtypes):
        """Read a HTuple type from the stream."""
        cdef long long ntypes, i
        #  TODO: This is an awful hack for backwards-compatability of some
        #        of my old code which wrote different types into an HTUPLE.
        #        It will be removed eventually.
        ntypes = len(subtypes)
        for i in xrange(nitems):
            items.append(self._read_value(subtypes[i % ntypes]))
        return items

    cdef _write_HTuple(self,value,subtypes):
        """Write a HTuple type to the stream."""
        cdef long long nitems, ntypes, i
        cdef Stream s
        s = StringStream()
        nitems = len(value)
        s._write_small_int(nitems)
        #  TODO: This is an awful hack for backwards-compatability of some
        #        of my old code which wrote different types into an HTUPLE.
        #        It will be removed eventually.
        ntypes = len(subtypes)
        for i in xrange(nitems):
            s._write_value(value[i],subtypes[i % ntypes])
        data = s._getstring()
        self._write_small_int(len(data))
        self._write(data)

    cdef _read_Assoc(self,long long nitems,items,tuple subtypes):
        """Read an Assoc type from the stream.

        These are encoded as [length][num pairs]<pairs>.
        """
        cdef long long i
        for i in xrange(nitems):
            key = self._read_value(subtypes[0])
            val = self._read_value(subtypes[1])
            items[key] = val
        return items

    cdef _write_Assoc(self,value,subtypes):
        """Write an Assoc type to the stream."""
        cdef long long npairs
        cdef Stream s
        s = StringStream()
        npairs = len(value)
        s._write_int(npairs)
        for key,val in value.iteritems():
            s._write_value(key,subtypes[0])
            s._write_value(val,subtypes[1])
        data = s._getstring()
        self._write_int(len(data))
        self._write(data)

#  This is a simple cache of the most recently allocated StringStream
#  buffer, so that it can be re-used without constant mallocing.
cdef char* _spare_stringstream_buffer
cdef long long _spare_stringstream_length
_spare_stringstream_buffer = NULL
_spare_stringstream_length = 0

cdef class StringStream(Stream):
    """Special-purpose implementation of Stream for parsing strings.

    This implementation uses low-level byte arrays to manage and parse the
    stream in-memory.  StringStream is to Stream as StringIO is to file.
    """

    cdef char* buffer
    cdef long long curpos
    cdef long long length

    def __init__(self,value=None):
        global _spare_stringstream_buffer
        global _spare_stringstream_length
        cdef char* spare_buffer
        cdef long long spare_length
        self.curpos = 0
        if value is None:
#  TODO: make this thread-safe, or discard it.
#            if _spare_stringstream_buffer == NULL:
                self.length = 32
                self.buffer = <char*>malloc(self.length)
                if not self.buffer:
                    raise MemoryError
#            else:
#                spare_buffer = _spare_stringstream_buffer
#                spare_length = _spare_stringstream_length
#                _spare_stringstream_buffer = NULL
#                _spare_stringstream_length = 0
#                self.length = spare_length
#                self.buffer = spare_buffer
        else:
            self.length = len(value)
            self.buffer = PyString_AsString(value)
        super(StringStream,self).__init__(value)

    def __dealloc__(self):
        global _spare_stringstream_buffer
        global _spare_stringstream_length
        if self.file is None:
#            if _spare_stringstream_buffer == NULL:
#                _spare_stringstream_buffer = self.buffer
#                _spare_stringstream_length = self.length
#            else:
                free(self.buffer)

    cdef _read(self,long long size):
        if self.curpos + size > self.length:
            raise UnexpectedEOFError
        s = PyString_FromStringAndSize(self.buffer+self.curpos,size)
        self.curpos += size
        return s

    cdef unsigned char _read_char(self):
        cdef unsigned char c
        if self.curpos >= self.length:
            raise UnexpectedEOFError
        c = <unsigned char>self.buffer[self.curpos]
        self.curpos += 1
        return c

    cdef void _skip(self,long long size):
        if self.curpos + size > self.length:
            raise UnexpectedEOFError
        self.curpos += size

    cdef _growbuffer(self,long long dlen):
        self.length = self.length * 2
        while self.curpos + dlen > self.length:
            self.length = self.length * 2
        self.buffer = <char*>realloc(self.buffer,self.length)
        if not self.buffer:
            raise MemoryError

    cdef void _write(self,data):
        cdef char* cin
        cdef size_t dlen
        dlen = len(data)
        if self.curpos + dlen > self.length:
            self._growbuffer(dlen)
        cin = PyString_AsString(data)
        memcpy(self.buffer+self.curpos,cin,dlen)
        self.curpos += dlen

    cdef void _write_char(self,char c):
        if self.curpos >= self.length:
            self._growbuffer(1)
        self.buffer[self.curpos] = c
        self.curpos += 1

    cdef Stream _get_substream(self,long long length):
        return self

    cdef _getstring(self):
        return PyString_FromStringAndSize(self.buffer,self.curpos)




def from_string(string,typcls):
    """Parse an instance of the given typeclass from the given string."""
    cdef StringStream s
    s = StringStream(string)
    return s._read_value(typcls)

def from_file(file,typcls):
    """Parse an instance of the given typeclass from the given file."""
    cdef Stream s
    s = Stream(file)
    return s._read_value(typcls)

def to_string(value,typcls):
    """Render an instance of the given typeclass into a string."""
    cdef StringStream s
    s = StringStream()
    s._write_value(value,typcls)
    return s._getstring()

def to_file(file,value,typcls):
    """Render an instance of the given typeclass into a file."""
    cdef Stream s
    s = Stream(file)
    s._write_value(value,typcls)

