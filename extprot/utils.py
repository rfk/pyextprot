"""

  extprot.utils:  misc utility clases for extprot.


"""


class TypedList(list):
    """Subclass of built-in list type that contains type-checked values.

    Instances of TypedList are the canonical internal representation
    for the List and Array extprot types.
    """

    def __init__(self,type,items=()):
        self._type = type
        items = [self._type._ep_convert(i) for i in items]
        super(TypedList,self).__init__(items)

    def _store(self,value):
        return self._type._ep_convert(value)

    def __setitem__(self,key,value):
        if isinstance(key,slice):
            value = [self._store(v) for v in value]
        else:
            value = self._store(value)
        super(TypedList,self).__setitem__(key,value)

    def __setslice__(self,i,j,sequence):
        values = [self._store(v) for v in sequence]
        super(TypedList,self).__setslice__(i,j,values)

    def __contains__(self,value):
        value = self._store(value)
        return super(TypedList,self).__contains__(value)

    def __iter__(self):
        return list.__iter__(self)

    def append(self,item):
        return super(TypedList,self).append(self._store(item))

    def index(self,value,start=None,stop=None):
        return super(TypedList,self).index(self._store(value),start,stop)

    def extend(self,iterable):
        items = [self._store(i) for i in iterable]
        return super(TypedList,self).extend(items)

    def insert(self,index,object):
        return super(TypedList,self).insert(index,self._store(object))

    def remove(self,value):
        return super(TypedList,self).remove(self._store(value))

    def __iadd__(self,other):
        return super(TypedList,self).__iadd__(TypedList(self._type,other))



class TypedDict(dict):
    """Subclass of built-in dict type that contains type-checked values.

    Instances of TypedDict are the canonical internal representation
    for the Assoc extprot type.
    """

    def __init__(self,ktype,vtype,items=()):
        items = [(ktype._ep_convert(k),vtype._ep_convert(v)) for (k,v) in dict(items).iteritems()]
        super(TypedDict,self).__init__(items)
        self._ktype = ktype
        self._vtype = vtype

    def _kstore(self,key):
        return self._ktype._ep_convert(key)

    def _vstore(self,value):
        return self._vtype._ep_convert(value)

    def __setitem__(self,key,value):
        super(TypedDict,self).__setitem__(self._kstore(key),self._vstore(value))

    def setdefault(self,key,value):
        return super(TypedDict,self).setdefault(self._kstore(key),self._vstore(value))

    def update(self,source,**kwds):
        try:
            source.keys
        except AttributeError:
            for (k,v) in source:
                self[k] = v
        else:
            for k in source:
                self[k] = source[k]
        for (k,v) in kwds:
            self[k] = v

