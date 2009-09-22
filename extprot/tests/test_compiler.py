

import sys
from os import path
import tempfile
import unittest

import extprot
from extprot.types import *

pfile = path.join(path.dirname(__file__),"../../../examples/address_book.proto")

class TestCompiler(unittest.TestCase):

    def test_import_protocol(self):
        # Try it with the filename
        namespace = {}
        extprot.import_protocol(pfile,namespace)
        assert issubclass(namespace["person"],Message)
        assert issubclass(namespace["address_book"],Message)
        # Try it with filelike object
        namespace = {}
        extprot.import_protocol(open(pfile,"r"),namespace)
        assert issubclass(namespace["person"],Message)
        assert issubclass(namespace["address_book"],Message)

    def test_import_protocol_string(self):
        namespace = {}
        extprot.import_protocol_string(open(pfile).read(),namespace)
        assert issubclass(namespace["person"],Message)
        assert issubclass(namespace["address_book"],Message)

    def test_compile_protocol(self):
        #  Try compiling from filename
        out1 = tempfile.TemporaryFile()
        extprot.compile_protocol(pfile,out1)
        out1.seek(0)
        out1 = out1.read().strip()
        #  Try compiling from filelike object
        out2 = tempfile.TemporaryFile()
        extprot.compile_protocol(open(pfile),out2)
        out2.seek(0)
        out2 = out2.read().strip()
        #  Try compiling from string
        out3 = extprot.compile_protocol_string(open(pfile).read()).strip()
        #  Should all give the same result
        self.assertEquals(out1,out2)
        self.assertEquals(out1,out3)
        #  And should have certain things in them
        self.assertTrue("class person(types.Message):" in out1)
        self.assertTrue("class optional(types.Union):" in out1)
        self.assertTrue("class Set(types.Option):" in out1)


