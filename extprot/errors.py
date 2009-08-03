"""

  extprot.errors:  error classes for the extprot package

"""

class Error(Exception):
    """Base class for all extprot-related errors."""
    pass

class CompileError(Error):
    """Error compiling a protocol definition."""
    pass

class ParseError(Error):
    """Error parsing a badly-formed byte stream."""
    pass

class UnexpectedEOFError(ParseError):
    """Error when EOF is reached in mid-parse."""
    pass

class UnexpectedWireTypeError(ParseError):
    """Error when an unexpected wire type is read."""
    pass

class UndefinedDefaultError(Error):
    """Error raised when a default is needed, but not provided."""
    pass

