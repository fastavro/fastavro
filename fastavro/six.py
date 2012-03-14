'''Compatiblity for Python versions.

Some of this code is "lifted" from CherryPy.
'''
import sys

_encoding = 'UTF-8'

if sys.version_info >= (3, 0):
    from io import BytesIO as MemoryIO
    xrange = range


    def btou(n, encoding=_encoding):
        return n.decode(encoding)


    def utob(n, encoding=_encoding):
        return bytes(n, encoding)

else:  # Python 2x
    from cStringIO import StringIO as MemoryIO
    xrange = xrange


    def btou(n, encoding=_encoding):
        return n


    def utob(n, encoding=_encoding):
        return n

