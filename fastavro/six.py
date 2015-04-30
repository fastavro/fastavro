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

    unicode = str
    long = int

    def py3_json_dump(obj, indent):
        json.dump(obj, stdout, indent=indent)

else:  # Python 2x
    from cStringIO import StringIO as MemoryIO  # NOQA
    xrange = xrange

    def py2_btou(n, encoding=_encoding):
        return unicode(n, encoding)

    def py2_utob(n, encoding=_encoding):
        return n.encode(encoding)

    unicode = unicode
    long = long

    _outenc = getattr(stdout, 'encoding', None) or _encoding

    def py2_json_dump(obj, indent):
        json.dump(obj, stdout, indent=indent, encoding=_outenc)

# We do it this way and not just redifine function since Cython do not like it
if sys.version_info >= (3, 0):
    btou = py3_btou
    utob = py3_utob
    json_dump = py3_json_dump
else:
    btou = py2_btou
    utob = py2_utob
    json_dump = py2_json_dump
