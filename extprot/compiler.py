
from pyparsing import *

from extprot.errors import *
from extprot import types


class BaseCompiler:

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
    """Compile a .proto file directly to objects in a python namespace."""

    def __init__(self,namespace=None):
        if namespace is None:
            namespace = {}
        self.namespace = namespace
        BaseCompiler.__init__(self)

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
        return types.Unbound()

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
                Opt.__name__ = opt_data[0]
                Opt.__module__ = "<extprot.dynamic>"
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
        type.__name__ = name
        type.__module__ = "<extprot.dynamic>"
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
        Anon.__name__ = name
        Anon.__module__ = "<extprot.dynamic>"
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
        Anon.__name__ = name
        Anon.__module__ = "<extprot.dynamic>"
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


if __name__ == "__main__":
    import sys
    ns = {}
    NamespaceCompiler(ns).compile(sys.stdin)
    print ns


