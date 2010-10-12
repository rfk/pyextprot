
import os
import extprot
import unittest

class TestMisc(unittest.TestCase):

    def test_README(self):
        """Ensure that the README is in sync with the docstring.

        This test should always pass; if the README is out of sync it just
        updates it with the contents of extprot.__doc__.
        """
        dirname = os.path.dirname
        readme = os.path.join(dirname(dirname(dirname(__file__))),"README.txt")
        if not os.path.isfile(readme):
            f = open(readme,"wb")
            f.write(extprot.__doc__.encode())
            f.close()
        else:
            f = open(readme,"rb")
            if f.read() != extprot.__doc__:
                f.close()
                f = open(readme,"wb")
                f.write(extprot.__doc__)
