
import unittest

from extprot.stream import StringStream

class Test_Stream(unittest.TestCase):
    
    def test_write_int(self):
        s = StringStream()
        s.write_int(34)
        self.assertEqual(StringStream(s.getstring()).read_int(),34)

    def test_write_bigger_int(self):
        s = StringStream()
        s.write_int(128)
        self.assertEqual(StringStream(s.getstring()).read_int(),128)

    def test_write_vints(self):
        s = StringStream()
        s.write_Vint(34)
        s.write_Vint(2053)
        s.write_Vint(314159265)
        s = StringStream(s.getstring())
        self.assertEquals(s.read_Vint(),34)
        self.assertEquals(s.read_Vint(),2053)
        self.assertEquals(s.read_Vint(),314159265)
 

