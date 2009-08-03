#
#  This is the extprot setuptools script.
#  Originally developed by Ryan Kelly, 2009.
#
#  This script is placed in the public domain.
#

from distutils import setup

#  Import to allow pertinent info to be extracted
import extprot

VERSION = extprot.__version__

# Package MetaData
NAME = "extprot"
DESCRIPTION = "efficient binary serialization format for extensible protocols"
AUTHOR = "Ryan Kelly"
AUTHOR_EMAIL = "ryan@rfk.id.au"
URL = "http://github.com/rfk/extprot/tree/master"
LICENSE = "MIT"
KEYWORDS = "serialization protocol"
LONG_DESC = extprot.__doc__

#  Module Lists
PACKAGES = ["extprot"]
EXT_MODULES = []
PKG_DATA = {}

##
##  Main call to setup() function
##

setup(name=NAME,
      version=VERSION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      url=URL,
      description=DESCRIPTION,
      long_description=LONG_DESC,
      keywords=KEYWORDS,
      packages=PACKAGES,
      ext_modules=EXT_MODULES,
      package_data=PKG_DATA,
      license=LICENSE,
     )
