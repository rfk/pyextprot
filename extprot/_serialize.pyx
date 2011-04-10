"""

  extprot._serialize:  low-level serialization machinery for extprot

This module implements the low-level details of reading and writing extprot
bytestreams.  It's a Cython-generated C extension module.  If it's not 
working for you, just use the pure-python "serialize" module instead.

"""

import struct

from extprot.errors import *
from extprot.utils import TypedList, TypedDict

cdef extern from "stdlib.h":
    ctypedef unsigned long size_t
    void free(void *ptr)
    void *malloc(size_t size)
    void *realloc(void *ptr, size_t size)
    void *memcpy(void *dest, void *src, size_t n)

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)
    char* PyString_AsString(object string)


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



cdef class TypeDesc(object):
    """Object used to direct the serialization process.

    Instances of TypeDesc are used to direct the serialization process
    Each typeclass provides a TypeDesc object whose method hooks are called
    at appropriate times during parsing or redering.

    The base class provides no useful functionality, it just errors out.
    Subclasses provide behaviour specialised to the base extprot types,
    and calling code can feel free to provide any custom behaviour it needs.

    This is broken off into a separate class so that the cython-based parser
    can implement the common methods in C.
    """

    cdef public dict collection_constructor
    cdef public dict subtypes

    def __init__(self):
        if self.collection_constructor is None:
            self.collection_constructor = {}
        if self.subtypes is None:
            self.subtypes = {}

    cpdef parse_value(self,value,TypeID type,long long tag):
        """Finalize parsing of a value from the extprot bytestream.

        This method will be called when a new value has been read off the
        bytestream.  Given the primitive type, tag and value, it should
        return the final object to be returned from the parser.
        """
        raise NotImplementedError

    cpdef tuple render_value(self,value):
        """Convert value to tagged primitive type for rendering.

        This method will be called when a value is about to be written out.
        It must convert the value to something renderable, and return a
        3-tuple giving the value, type and tag.
        """
        raise NotImplementedError

    cpdef default_value(self):
        """Construct default value for this type.

        This method will be called whenever a default value of this type 
        is required.
        """
        raise UndefinedDefaultError



cdef class SingleTypeDesc(TypeDesc):
    """TypeDesc class for types with a single type tag.

    This is a typedesc suitable for use with most primitive types, where
    there is a single type and tag regardless of the value.
    """

    cdef public int type
    cdef public long long tag

    cpdef parse_value(self,value,TypeID type,long long tag):
        if type != self.type:
            raise UnexpectedWireTypeError
        return value

    cpdef tuple render_value(self,value):
        return (value,self.type,self.tag)


cdef class BoolTypeDesc(SingleTypeDesc):
    """TypeDesc class for boolean-like types."""

    cpdef parse_value(self,value,TypeID type,long long tag):
        value = SingleTypeDesc.parse_value(self,value,type,tag)
        return (value != "\x00")

    cpdef tuple render_value(self,value):
        if value:
            value = "\x01"
        else:
            value = "\x00"
        return SingleTypeDesc.render_value(self,value)

    cpdef default_value(self):
        return False


cdef class IntTypeDesc(SingleTypeDesc):
    """TypeDesc class for integer-like types."""

    cpdef parse_value(self,value,TypeID type,long long tag):
        value = SingleTypeDesc.parse_value(self,value,type,tag)
        if value % 2:
            return value // -2
        else:
            return value // 2

    cpdef tuple render_value(self,value):
        if value >= 0:
            value = value * 2
        else:
            value = (value * -2) - 1
        return SingleTypeDesc.render_value(self,value)



cdef class TupleTypeDesc(SingleTypeDesc):
    """TypeDesc class for tuple-like types."""

    cpdef parse_value(self,value,TypeID type,long long tag):
        cdef TypeDesc t
        cdef tuple subtypes
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

    cpdef default_value(self):
        cdef TypeDesc t
        cdef tuple subtypes
        values = []
        subtypes = self.subtypes[(self.type,self.tag)]
        for t in subtypes:
            values.append(t.default_value())
        return tuple(values)


cdef class MessageTypeDesc(TupleTypeDesc):
    """TypeDesc class for message types."""

    cpdef parse_value(self,value,TypeID type,long long tag):
        cdef int i
        value = TupleTypeDesc.parse_value(self,value,type,tag)
        #  Bypass typechecking by initialising the values before calling
        #  __init__.  We already know the types are valid.
        inst = self.type_class.__new__(self.type_class)
        for i in xrange(min(len(inst._ep_fields),len(value))):
            inst.__dict__[inst._ep_fields[i]._ep_name] = value[i]
        inst._ep_initialized = True
        inst.__init__(*value)
        return inst

    cpdef tuple render_value(self,value):
        value = [value.__dict__[f._ep_name] for f in self.type_class._ep_fields]
        return TupleTypeDesc.render_value(self,value)

    cpdef default_value(self):
        return self.type_class()
        


cdef class Stream(object):
    """Base class for processing an extprot bytestream.

    Instances of this class are used to read or write objects to a generic
    filelike object.  A specialized subclass StringString is used when
    writing to an in-memory string.
    """

    cdef object file

    def __init__(self,file):
        self.file = file

    def read_value(self,TypeDesc typdesc):
        """Read a generic value from the stream.

        This method takes a type description object and constructs a value
        for it by reading a value off the stream.

        If there are no more values in the stream, EOFError will be raised.
        """
        return self._read_value(typdesc)

    def write_value(self,value,TypeDesc typdesc):
        """Write a generic value to the stream."""
        self._write_value(value,typdesc)

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

    cdef _read_value(self,TypeDesc typdesc):
        cdef long long prefix, tag, length, nitems
        cdef TypeID type
        cdef long vi32
        cdef long long vi64
        cdef char* data
        cdef Stream s
        try:
            prefix = self._read_small_int()
        except UnexpectedEOFError:
            raise EOFError
        type = <TypeID>(prefix & 0xf)
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
                try:
                    items = typdesc.collection_constructor[(type,tag)]()
                except KeyError:
                    raise UnexpectedWireTypeError((type,tag))
                try:
                    subtypes = typdesc.subtypes[(type,tag)]
                except KeyError:
                    raise UnexpectedWireTypeError
                if type == _E_TYPE_TUPLE:
                    value = s._read_Tuple(items,subtypes)
                elif type == _E_TYPE_ASSOC:
                    value = s._read_Assoc(items,subtypes)
                elif type == _E_TYPE_HTUPLE:
                    value = s._read_HTuple(items,subtypes)
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
        value = typdesc.parse_value(value,type,tag)
        return value

    cdef _write_value(self,value,TypeDesc typdesc):
        cdef long long tag
        cdef TypeID type
        cdef long long vi64
        cdef long vi32
        orig_value = value
        (value,type,tag) = typdesc.render_value(value)
        self._write_small_int(tag << 4 | type)
        if type == _E_TYPE_VINT:
            self._write_int(value)
        elif type == _E_TYPE_TUPLE:
            subtypes = typdesc.subtypes[(type,tag)]
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
            subtypes = typdesc.subtypes[(type,tag)]
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
            subtypes = typdesc.subtypes[(type,tag)]
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
        cdef TypeID type
        try:
            prefix = self._read_small_int()
        except UnexpectedEOFError:
            raise EOFError
        type = <TypeID>(prefix & 0xf)
        tag = prefix >> 4
        #  If the LSB of the wiretype is 1, it is length-delimited.
        #  If not, it is a primitive type with known size.
        if type & 0x01:
            length = self._read_small_int()
            self._skip(length)
        else:
            if type == _E_TYPE_VINT:
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
        lx = le = 0
        #  Read as much as we can using C long longs.
        #  These constants are (2**(64-8)-1) and (64-8).
        #  I haven't sat down and done the math to figure out whether
        #  they could be higher; these are safe low-thought values.
        while lx < 72057594037927935LLU and le < 56:
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
        #  We're about to overflow lx, but le might be OK for a while.
        x = lx
        while le < 57:
            if b < 128:
                lh = b
                lh = lh << le
                x += lh
                return x
            lh = b - 128
            lh = lh << le
            x += lh
            le += 7
            b = <unsigned short>self._read_char()
        e = le
        #  Now carry on using just Python longs.
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

    cdef _read_Tuple(self,items,tuple subtypes):
        """Read a Tuple type from the stream."""
        cdef long long ntypes, nitems, i
        nitems = self._read_small_int()
        ntypes = len(subtypes)
        if nitems <= ntypes:
            for i in xrange(nitems):
                items.append(self._read_value(subtypes[i]))
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

    cdef _read_HTuple(self,items,tuple subtypes):
        """Read a HTuple type from the stream."""
        cdef long long ntypes, nitems, i
        nitems = self._read_small_int()
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
        ntypes = len(subtypes)
        for i in xrange(nitems):
            s._write_value(value[i],subtypes[i % ntypes])
        data = s._getstring()
        self._write_small_int(len(data))
        self._write(data)

    cdef _read_Assoc(self,items,tuple subtypes):
        """Read an Assoc type from the stream.

        These are encoded as [length][num pairs]<pairs>.
        """
        cdef long long ntypes, nitems, i
        nitems = self._read_small_int()
        ntypes = len(subtypes)
        for i in xrange(nitems):
            key = self._read_value(subtypes[(2*i) % ntypes])
            val = self._read_value(subtypes[(2*i + 1) % ntypes])
            items[key] = val
        return items

    cdef _write_Assoc(self,value,subtypes):
        """Write an Assoc type to the stream."""
        cdef long long npairs, i
        cdef Stream s
        s = StringStream()
        npairs = len(value)
        s._write_int(npairs)
        ntypes = len(subtypes)
        i = 0
        for key,val in value.iteritems():
            s._write_value(key,subtypes[(2*i) % ntypes])
            s._write_value(val,subtypes[(2*i + 1) % ntypes])
            i += 1
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
    return s._read_value(typcls._ep_typedesc)

def from_file(file,typcls):
    """Parse an instance of the given typeclass from the given file."""
    cdef Stream s
    s = Stream(file)
    return s._read_value(typcls._ep_typedesc)

def to_string(value,typcls):
    """Render an instance of the given typeclass into a string."""
    cdef StringStream s
    s = StringStream()
    s._write_value(value,typcls._ep_typedesc)
    return s._getstring()

def to_file(file,value,typcls):
    """Render an instance of the given typeclass into a file."""
    cdef Stream s
    s = Stream(file)
    s._write_value(value,typcls._ep_typedesc)


