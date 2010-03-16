"""

  extprot:  compact, efficient, extensible binary serialization format

This is a python implementation of the 'extprot' serialization scheme, the
details of which are described at:

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
    '\\x01\\x1f\\x03\\x00\\x02\\x03\\x05Guido\\x05\\x13\\x01\\x03\\x10guido@python.org'
    >>> print person.from_string(p1.to_string()).name
    'Guido'
    >>>
    
Extprot compares favourably to related serialization technologies:

   * powerful type system;  strongly-typed tuples and lists, tagged disjoint
                            unions, parametric polymorphism.
   * self-delimiting data;  all serialized messages indicate their length,
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

The function extprot.import_protocol() will dynamically load a protocol file
and convert it into the corresponding python class structure. This is quite
convenient while developing a protocol since it avoids an extra compilation
step, but it does add some startup overhead and requires the pyparsing module.

To compile a protocol definition into python sourcecode for the corresponding
class definitions, use the function extprot.compile_protocol() or pipe the file
through extprot/compiler.py like so:

  $ cat mydefs.proto | python extprot/compiler.py > mydefs.py

"""

__ver_major__ = 0
__ver_minor__ = 2
__ver_patch__ = 4
__ver_sub__ = ""
__version__ = "%d.%d.%d%s" % (__ver_major__,__ver_minor__,
                              __ver_patch__,__ver_sub__)


from extprot.errors import *


def import_protocol(filename,namespace,module=None):
    """Dynamically load extprot protocol objects into the given namespace.

    This function dynamically compiles the protocol definitions found in
    the file 'filename' and loads the resulting objects into 'namespace'.
    If the optional argument 'module' is provided, this string will be
    set as the __module__ attribute of the created type classes.  You'll
    need to use this if you want them to be pickleable.

    If the protocol file is fairly static then you may prefer to compile it
    into a python module, so it can be imported directly without any parsing
    overhead.  The function extprot.compile_protocol() can be used for this.
    """
    from extprot.compiler import NamespaceCompiler
    nsc = NamespaceCompiler(module=module)
    nsc.compile(filename)
    for (n,v) in nsc.namespace.iteritems():
        namespace[n] = v 


def import_protocol_string(string,namespace,module=None):
    """Dynamically load extprot protocol objects into the given namespace.

    This function dynamically compiles the protocol definitions found in
    the string 'string' and loads the resulting objects into 'namespace'.
    If the optional argument 'module' is provided, this string will be
    set as the __module__ attribute of the created type classes.  You'll
    need to use this if you want them to be picklable.
    """
    from extprot.compiler import NamespaceCompiler
    nsc = NamespaceCompiler(module=module)
    nsc.compile_string(string)
    for (n,v) in nsc.namespace.iteritems():
        namespace[n] = v 


def compile_protocol(infile,outfile):
    """Compile extprot protocol objects into python sourcecode.

    This function compiles the protocol definitions found in 'infile'
    to sourcecode for their corresponding python class definitions.  The
    compiled code is written into 'outfile'.  Both 'infile' and 'outfile'
    can be specified as either a pathname or a file-like object.

    If the protocol defnitions change often, it may be more convenient to
    use the function extprot.import_protocol() to load them directly from
    protocol file, at the expense of some additional parsing overhead.
    """
    from extprot.compiler import ModuleCompiler
    mc = ModuleCompiler()
    mc.compile(infile)
    for ln in mc.code_lines:
        outfile.write(ln)
        outfile.write("\n")


def compile_protocol_string(string):
    """Compile extprot protocol objects into python sourcecode.

    This function compiles the protocol definitions found in the given
    string, returning sourcecode for their corresponding python class
    definitions.
    """
    from extprot.compiler import ModuleCompiler
    mc = ModuleCompiler()
    mc.compile_string(string)
    return "\n".join(mc.code_lines)

