
import cython

cdef extern from "stdlib.h":
    ctypedef unsigned long size_t
    void free(void *ptr)
    void *malloc(size_t size)
    void *realloc(void *ptr, size_t size)
    size_t strlen(char *s)
    char *strcpy(char *dest, char *src)

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)


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


cdef class _Stream:
    cdef _read(self,long long size)
    cdef void _skip(self,long long size)
    cdef void _write(self,data)
    cdef _getstring(self)
    cdef object _read_value(self,typcls)
    cdef void _skip_value(self)
    cdef void _write_value(self,value,typcls)
    cdef object _read_int(self,object x=*)
    cdef void _write_int(self,x)
    cdef long long _read_small_int(self)
    cdef void _write_small_int(self,x)
    cdef object _read_Tuple(self,items,subtypes,_Stream s)
    cdef void _write_Tuple(self,value,subtypes,_Stream s)
    cdef object _read_HTuple(self,items,subtypes,_Stream s)
    cdef void _write_HTuple(self,value,subtypes,_Stream s)
    cdef object _read_Assoc(self,items,subtypes,_Stream s)
    cdef void _write_Assoc(self,value,subtypes,_Stream s)
    

cdef class PyStringStream(_Stream):
    cdef object buffer

cdef class FilelikeStream(_Stream):
    cdef object file



