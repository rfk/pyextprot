"""

  extprot.compiler:  protocol description compiler for extprot

This module provides the necessary infrastructure to compile the language-
neutral extprot protocol description format into the Python class structure
defined by the extprot.types module.

The NamespaceCompiler class dynamically compiles a protocol definition into
class objects and stores them into a given namespace.  You would use it like
so:

    nsc = NamespaceCompiler(globals())
    nsc.compile("mymessages.proto")
    nsc.compile_string("message a_bool { v: bool; }")

The ModuleCompiler class compiles a protocol definition into the sourcecode
for these class objects, which can then be written into a file.

    mc = ModuleCompiler()
    mc.compile("mymessages.proto")
    open("mymessage.py").write("\n".join(mc.code_lines))

If you run this module as a script, it will run the ModuleCompiler class over
stdin and write the resulting sourcecode to stdout.  Use it like so:

    $ cat mymessage.proto | python extprot/compiler.py > mymessages.py

"""

from pyparsing import *

from extprot.errors import *
from extprot import types


class BaseCompiler(object):
    """Base compiler class for extprot protocol descriptions.

    This class defines the structure of the extprot grammar and connects
    up its various build_* methods as parser actions.  Subclasses should
    override these methods to provide the appropriate behaviour.
    """

    def __init__(self):
        self.grammar = self._make_grammar()

    def compile(self,stream):
        self.grammar.parseFile(stream)

    def compile_string(self,string):
        self.grammar.parseString(string)

    def build_prim_type(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_tuple_type(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_array_type(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_list_type(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_named_type(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_union_type(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_type_expr(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_type_stmt(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_type_def(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_field_def(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_simple_message(self,instring,loc,tokenlist):
        return [tokenlist]

    def build_union_message(self,instring,loc,tokenlist):
        return [tokenlist]

    def _make_grammar(self):
        prim_type = ["bool","byte","int","long","float","string"]
        prim_type = Or(map(Keyword,prim_type))
        prim_type.setParseAction(self.build_prim_type)

        keyword = ["type","message","mutable"]
        keyword = Or([prim_type,Or(map(Keyword,keyword))])
        ident = NotAny(keyword) + Word(alphanums+"_")
        pident = Combine(Optional("'") + ident)

        comment = Forward()
        comment_start = Literal("(*")
        comment_end = Literal("*)")
        comment_content = Regex("[^(\*\))]*")
        comment << (comment_start + comment_content + Optional(comment) + \
                    comment_content + comment_end)

        type_expr = Forward()

        t_tuple = Suppress("(") + delimitedList(type_expr,"*") + Suppress(")")
        t_tuple.setParseAction(self.build_tuple_type)

        t_array = Suppress("[|") + type_expr + Suppress("|]")
        t_array.setParseAction(self.build_array_type)

        t_list = Suppress("[") + type_expr + Suppress("]")
        t_list.setParseAction(self.build_list_type)

        t_named = pident + Optional(Suppress("<")+type_expr+Suppress(">"))
        t_named.setParseAction(self.build_named_type)

        t_union = delimitedList(Group(ident + ZeroOrMore(type_expr)),"|")
        t_union.setParseAction(self.build_union_type)

        type_expr << (prim_type | t_named | t_tuple | t_array | t_list)
        type_expr.setParseAction(self.build_type_expr)

        type_stmt = t_union | type_expr
        type_stmt.setParseAction(self.build_type_stmt)

        type_def = Suppress(Keyword("type")) + ident + ZeroOrMore(pident)
        type_def += Suppress("=") + type_stmt
        type_def.setParseAction(self.build_type_def)
                 
        field_def = Optional(Keyword("mutable")) + ident
        field_def += Suppress(":") + type_expr
        field_def.setParseAction(self.build_field_def)

        field_defs = delimitedList(field_def,";")  + Optional(Suppress(";"))
        field_defs = Suppress("{") + field_defs + Suppress("}")

        simple_message = Suppress(Keyword("message")) + ident + Suppress("=")
        simple_message += field_defs
        simple_message.setParseAction(self.build_simple_message)

        union_message = Suppress(Keyword("message")) + ident + Suppress("=")
        union_message += delimitedList(Group(ident + field_defs),"|")
        union_message.setParseAction(self.build_union_message)

        message = union_message | simple_message

        protocol = StringStart() + ZeroOrMore(message | type_def) + StringEnd()
        protocol.ignore(comment)

        return protocol


class NamespaceCompiler(BaseCompiler):
    """Compile a .proto file directly to objects in a python namespace.

    If the optional argument "module" is given, the created type classes
    will have that set as their __module__ attribute.  You'll need to do this
    if you want to make them pickleable.
    """

    def __init__(self,namespace=None,module=None):
        if namespace is None:
            namespace = {}
        if module is None:
            module = "<extprot.dynamic>"
        self.namespace = namespace
        self.module = module
        super(NamespaceCompiler,self).__init__()

    def build_prim_type(self,instring,loc,tokenlist):
        type = tokenlist[0]
        if type == "int":
            return types.Int
        if type == "bool":
            return types.Bool
        if type == "byte":
            return types.Byte
        if type == "long":
            return types.Long
        if type == "float":
            return types.Float
        if type == "string":
            return types.String
        raise CompilerError("unrecognised primitive type: " + type)

    def build_tuple_type(self,instring,loc,tokenlist):
        return types.Tuple.build(*tokenlist)

    def build_array_type(self,instring,loc,tokenlist):
        return types.Array.build(tokenlist[0])

    def build_list_type(self,instring,loc,tokenlist):
        return types.List.build(tokenlist[0])

    def build_named_type(self,instring,loc,tokenlist):
        #  We can only resolve names once the entire defn is built.
        #  For now we just store a placeholder.
        return types.Placeholder(tuple(tokenlist))

    def build_union_type(self,instring,loc,tokenlist):
        class Anon(types.Union):
            for opt_data in tokenlist:
                class Opt(types.Option):
                    pass
                Opt._types = tuple(opt_data[1:])
                self._adjust_type_name(Opt,opt_data[0])
                locals()[opt_data[0]] = Opt
                del Opt
            del opt_data
        return Anon

    def build_type_expr(self,instring,loc,tokenlist):
        return tokenlist[0]

    def build_type_stmt(self,instring,loc,tokenlist):
        #  Always take a subclass for top-level type statements,
        #  so we can safely set the __name__
        class Anon(tokenlist[0]):
            _types = tokenlist[0]._types
        return Anon

    def build_type_def(self,instring,loc,tokenlist):
        name = tokenlist[0]
        pvars = tokenlist[1:-1]
        type = tokenlist[-1]
        #  Create Unbound() instances for polymorphic vars
        unbounds = []
        pvar_map = {}
        for pvar in pvars:
            ub = types.Unbound()
            pvar_map[pvar] = ub
            unbounds.append(ub)
        type._unbound_types = tuple(unbounds)
        #  Resolve any placeholder types, and store in the namespace
        self._resolve_placeholders(type,pvar_map)
        self._adjust_type_name(type,name)
        self.namespace[name] = type
        return None

    def build_field_def(self,instring,loc,tokenlist):
        if len(tokenlist) == 3:
            mutable = True
            name = tokenlist[1]
            type = tokenlist[2]
        else:
            mutable = False
            name = tokenlist[0]
            type = tokenlist[1]
        return (name,types.Field(type,mutable=mutable))

    def build_simple_message(self,instring,loc,tokenlist):
        name = tokenlist[0]
        fields = tokenlist[1:] 
        class Anon(types.Message):
            for (nm,f) in fields:
                locals()[nm] = f
            del nm, f
        #  Resolve any placeholder types, and store in the namespace
        self._resolve_placeholders(Anon)
        self._adjust_type_name(Anon,name)
        self.namespace[name] = Anon
        return None

    def build_union_message(self,instring,loc,tokenlist):
        name = tokenlist[0]
        messages = tokenlist[1:]
        class Anon(types.Union):
            for msg in messages:
                m_name = msg[0]
                m_dict = {}
                for (nm,f) in msg[1:]:
                    m_dict[nm] = f
                Msg = types._MessageMetaclass(m_name,(types.Message,),m_dict)
                locals()[m_name] = Msg
            del msg, Msg, m_name, m_dict, nm, f
        #  Resolve any placeholder types, and store in the namespace
        self._resolve_placeholders(Anon)
        self._adjust_type_name(Anon,name)
        self.namespace[name] = Anon
        return None

    def _resolve_placeholders(self,type,locals={}):
        for (phname,setter) in types.resolve_placeholders(type):
            name = phname[0]
            params = phname[1:]
            try:
                val = locals[name]
            except KeyError:
                try:
                    val = self.namespace[name]
                except KeyError:
                    raise CompileError("unresolved name: " + repr(name))
            if params:
                val = types.bind(val,*params)
            setter(val)

    def _adjust_type_name(self,type,name):
        """Set __name__ and __module__ to something useful."""
        type.__module__ = self.module
        type.__name__ = name
        for t1 in type._types:
            if types._issubclass(t1,types.Message):
                t1.__name__ = name+"."+t1.__name__


class ModuleCompiler(BaseCompiler):
    """Compile a .proto file into sourcecode for a python module.

    Currently the code is built in memory as a list of strings.  After calling
    compile() or compile_string(), the result is availabe in the attribute
    'code_lines'.
    """

    def __init__(self,code_lines=None):
        if code_lines is None:
            code_lines = []
        self.code_lines = code_lines
        super(ModuleCompiler,self).__init__()

    def compile(self,stream):
        self._defined_names = {}
        self.code_lines.append("")
        self.code_lines.append("from extprot import types")
        self.code_lines.append("")
        self.code_lines.append("")
        super(ModuleCompiler,self).compile(stream)
 
    def compile_string(self,stream):
        self._defined_names = {}
        self.code_lines.append("")
        self.code_lines.append("from extprot import types")
        self.code_lines.append("")
        self.code_lines.append("")
        super(ModuleCompiler,self).compile_string(stream)

    def _tuple_string(self,values):
        tstr = ",".join(values)
        if len(values) == 1:
            tstr += ","
        return tstr

    def build_prim_type(self,instring,loc,tokenlist):
        type = tokenlist[0]
        if type == "int":
            return 'types.Int'
        if type == "bool":
            return 'types.Bool'
        if type == "byte":
            return 'types.Byte'
        if type == "long":
            return 'types.Long'
        if type == "float":
            return 'types.Float'
        if type == "string":
            return 'types.String'
        raise CompilerError("unrecognised primitive type: " + type)

    def build_tuple_type(self,instring,loc,tokenlist):
        types = self._tuple_string(tokenlist)
        return 'types.Tuple.build(%s)' % (types,)

    def build_array_type(self,instring,loc,tokenlist):
        return 'types.Array.build(%s)' % (tokenlist[0],)

    def build_list_type(self,instring,loc,tokenlist):
        return 'types.List.build(%s)' % (tokenlist[0],)

    def build_named_type(self,instring,loc,tokenlist):
        #  We can only resolve names once the entire defn is built.
        #  For now we just store a placeholder.
        if len(tokenlist) == 1:
            return self._get_placeholder_name(tokenlist[0])
        nm = self._get_placeholder_name(tokenlist[0])
        args = self._tuple_string(tokenlist[1:])
        return "types.bind(" + nm + "," + args + ")"

    def build_union_type(self,instring,loc,tokenlist):
        lines = ['types.Union']
        if tokenlist:
            for opt_data in tokenlist:
                opt_name = opt_data[0]
                opt_types = self._tuple_string(opt_data[1:])
                lines.append("class %s(types.Option):" % (opt_name,))
                lines.append("    _types = (%s)" % (opt_types,))
        else:
            lines.append("pass")
        return [lines]

    def build_type_expr(self,instring,loc,tokenlist):
        return tokenlist[0]

    def build_type_stmt(self,instring,loc,tokenlist):
        if isinstance(tokenlist[0],basestring):
            if "(" not in tokenlist[0]:
                return [[tokenlist[0],"pass"]]
            bits = tokenlist[0].split(".build(")
            if len(bits) > 1:
                return [[bits[0],"_types = ("+bits[1][:-1]+")"]]
        return [tokenlist[0]]

    def build_type_def(self,instring,loc,tokenlist):
        lines = []
        name = tokenlist[0]
        pvars = tokenlist[1:-1]
        type = tokenlist[-1]
        if pvars:
            lines.append("_ubts = tuple(types.Unbound() for _ in xrange(%s))")
            lines[-1] = lines[-1] % (len(pvars),)
        if isinstance(type,basestring):
            lines.append("%s = %s" % (name,type,))
            if pvars:
                lines.append("%s._unbound_types = _ubts" % (name,))
        else:
            lines.append("class %s(%s):" % (name,type[0]))
            if pvars:
                lines.append("    _unbound_types = _ubts")
            for ln in type[1:]:
                lines.append("    " + ln)
        pvar_map = {}
        for (i,nm) in enumerate(pvars):
            pvar_map[nm] = "_ubts[%s]" % (i,)
        self.code_lines.extend(self._resolve_placeholder_names(lines,pvar_map))
        self.code_lines.append("")
        self._defined_names[name] = name
        return lines

    def build_field_def(self,instring,loc,tokenlist):
        if len(tokenlist) == 3:
            name = tokenlist[1]
            type = tokenlist[2]
            return "%s = types.Field(%s,mutable=True)" % (name,type,)
        else:
            name = tokenlist[0]
            type = tokenlist[1]
            return "%s = types.Field(%s)" % (name,type,)

    def build_simple_message(self,instring,loc,tokenlist):
        name = tokenlist[0]
        fields = tokenlist[1:] 
        lines = ["class %s(types.Message):" % (name,)]
        for f in fields:
            lines.append("    " + f)
        self.code_lines.extend(self._resolve_placeholder_names(lines))
        self.code_lines.append("")
        self._defined_names[name] = name
        return None

    def build_union_message(self,instring,loc,tokenlist):
        name = tokenlist[0]
        messages = tokenlist[1:]
        lines = ["class %s(types.Union):" % (name,)]
        for msg in messages:
            m_name = msg[0]
            lines.append("    class %s(types.Message):" % (m_name,))
            for field in msg[1:]:
                lines.append("        " + field)
        self.code_lines.extend(self._resolve_placeholder_names(lines))
        self.code_lines.append("")
        self._defined_names[name] = name
        return None

    def _get_placeholder_name(self,name):
        return "##" + name + "##"

    def _resolve_placeholder_names(self,lines,locals={}):
        newlines = []
        for ln in lines:
            for (name,value) in locals.iteritems():
                ln = ln.replace(self._get_placeholder_name(name),value)
            for (name,value) in self._defined_names.iteritems():
                ln = ln.replace(self._get_placeholder_name(name),value)
            bits = ln.split("##")
            if len(bits) > 1:
                raise CompilerError("unresolved name: " + bits[1])
            newlines.append(ln)
        return newlines


if __name__ == "__main__":
    import sys
    c = ModuleCompiler()
    c.compile(sys.stdin)
    print "\n".join(c.code_lines)


