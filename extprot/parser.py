
from pyparsing import *


declration = message | type
protocol = ZeroOrMore(declaration)
