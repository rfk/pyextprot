"""

  extprot:  compact, efficient, extensible binary serialization format

This is a python implementation of the 'extprot' serialization scheme, the
details of which are descripted in detail at:

    http://eigenclass.org/R2/writings/extprot-extensible-protocols-intro

Similar to Google's Protocol Buffers and Apache Thrift, extprot allows the
definition of structured data "messages".  Messages are essentially a set
of typed key-value pairs that can be efficiently serialized to/from a
compact binary format, and are defined in a language-neutral "protocol" file.
Here's a simple example of an extprot message:

    message person = {
        id:   int;
        name: string;
        emails: [ string ]
    }

Here the 'person' message contains three fields: 'id' is an integer, 'name'
is a string, and 'emails' is a list of strings. Such protocol descriptions
are compiled into a set of Python classes that can be manipulated using 
standard syntax and idioms.  If the above protocol is recorded in the file
"person.proto", here's a simple example of how it might be used:

    >>> extprot.import_protocol("person.proto",globals())
    >>> p1 = person(1,"Guido")
    >>> print p1.emails   # fields use a sensible default if possible
    []
    >>> p1.emails.append("guido@python.org")
    >>> p1.emails.append(7)   # all fields are dynamically typechecked
    Traceback (mosts recent call last):
        ...
    ValueError: not a valid String: 7
    >>> print repr(p1.to_string())
    '\x01\x1f\x03\x00\x02\x03\x05Guido\x05\x13\x01\x03\x10guido@python.org'
    >>> print person.from_string(p1.to_string()).name
    'Guido'
    >>>
    
Extprot compares favourably to related serialization technologies:

   * powerful type system;  strongly-typed tuples and lists, tagged disjoint
                            unions, parametric polymorphism.
   * self-delimitng data;   all serialized messages indicate their length,
                            allowing easy streaming and skipping of messages.
   * self-describing data;  the 'skeleton' of a message can be reconstructed
                            without having the protocol definition.
   * compact binary format; comparable to protocol-buffers/thrift, but with
                            some overhead due to self-delimiting nature.

These features combine to make extprot strongly extensible, often allowing
messages to maintain backward *and* forward compatibility across protocol 
extensions that include: adding fields to a message, adding elements to a
tuple, adding cases to a disjoint union, and promoting a primitive type into
a tuple, list or union.

"""

__ver_major__ = 0
__ver_minor__ = 1
__ver_patch__ = 0
__ver_sub__ = ""
__version__ = "%d.%d.%d%s" % (__ver_major__,__ver_minor__,
                              __ver_patch__,__ver_sub__)


from extprot.errors import *


def import_protocol(filename,namespace):
    """Dynamically load extprot protocol objects into the given namespace.

    This function dynamically compiles the protocol definitions found in
    the file 'filename' and loads the resulting objects into 'namespace'.
    For now this is the only way to compile a protocol file; soon support
    will be added for writing the compiled classes out to a Python file.
    """
    from extprot.compiler import NamespaceCompiler
    nsc = NamespaceCompiler()
    nsc.compile(filename)
    for (n,v) in nsc.namespace.iteritems():
        namespace[n] = v 


def import_protocol_string(string,namespace):
    """Dynamically load extprot protocol objects into the given namespace.

    This function dynamically compiles the protocol definitions found in
    the string 'string' and loads the resulting objects into 'namespace'.
    """
    from extprot.compiler import NamespaceCompiler
    nsc = NamespaceCompiler()
    nsc.compile_string(string)
    for (n,v) in nsc.namespace.iteritems():
        namespace[n] = v 


