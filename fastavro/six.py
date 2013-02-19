# cython: auto_cpdef=True

'''Compatiblity for Python versions.

Some of this code is "lifted" from CherryPy.
'''
import sys
import json
from sys import stdout

_encoding = 'UTF-8'

if sys.version_info >= (3, 0):
    from io import BytesIO as MemoryIO
    xrange = range


    def py3_btou(n, encoding=_encoding):
        return n.decode(encoding)


    def py3_utob(n, encoding=_encoding):
        return bytes(n, encoding)

    def json_dump(obj):
        json.dump(obj, stdout, indent=4)

else:  # Python 2x
    from cStringIO import StringIO as MemoryIO
    xrange = xrange


    def py2_btou(n, encoding=_encoding):
        return n


    def py2_utob(n, encoding=_encoding):
        return n

    _outenc = stdout.encoding or _encoding
    def json_dump(obj):
        json.dump(obj, stdout, indent=4, encoding=_outenc)

# We do it this way and not just redifine function since Cython do not like it
if sys.version_info >= (3, 0):
    btou = py3_btou
    utob = py3_utob
else:
    btou = py2_btou
    utob = py2_utob
