#
#  This is the extprot setuptools script.
#  Originally developed by Ryan Kelly, 2009.
#
#  This script is placed in the public domain.
#

import os
import sys
import subprocess
from distutils.core import setup
from distutils.extension import Extension

#  Import to allow pertinent info to be extracted
import extprot

VERSION = extprot.__version__

# Package MetaData
NAME = "extprot"
DESCRIPTION = "compact, efficient, extensible binary serialization format"
AUTHOR = "Ryan Kelly"
AUTHOR_EMAIL = "ryan@rfk.id.au"
URL = "http://github.com/rfk/extprot/tree/master"
LICENSE = "MIT"
KEYWORDS = "serialization protocol"
LONG_DESC = extprot.__doc__
CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2",
    "Topic :: Software Development :: Libraries",
]

#  Module Lists
PACKAGES = ["extprot","extprot.tests"]
EXT_MODULES = []
PKG_DATA = {}

#  If building a source distribution, cython-compile necessary modules.

def prep_ext_module(nm):
    path = os.path.join(os.path.dirname(__file__),"extprot",nm+".pyx")
    subprocess.check_call(["cython",path])

if "sdist" in sys.argv:
    prep_ext_module("_serialize")

#  Add the source for cython extension modules if it exists.

def add_ext_module(nm):
    path = os.path.join(os.path.dirname(__file__),"extprot",nm+".c")
    if os.path.exists(path):
        EXT_MODULES.append(Extension("extprot."+nm,[path]))

add_ext_module("_serialize")

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
      classifiers=CLASSIFIERS,
     )

