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

    def py3_json_dump(obj, indent):
        json.dump(obj, stdout, indent=indent)

    def py3_iteritems(obj):
        return obj.items()

    def py3_is_str(obj):
        return isinstance(obj, (bytes, str,))

else:  # Python 2x
    from cStringIO import StringIO as MemoryIO  # flake8: noqa
    xrange = xrange

    def py2_btou(n, encoding=_encoding):
        return unicode(n, encoding) # flake8: noqa

    def py2_utob(n, encoding=_encoding):
        return n.encode(encoding)

    _outenc = getattr(stdout, 'encoding', None) or _encoding

    def py2_json_dump(obj, indent):
        json.dump(obj, stdout, indent=indent, encoding=_outenc)

    def py2_iteritems(obj):
        return obj.iteritems()

    def py2_is_str(obj):
        return isinstance(obj, basestring) # flake8: noqa

# We do it this way and not just redifine function since Cython do not like it
if sys.version_info >= (3, 0):
    btou = py3_btou
    utob = py3_utob
    json_dump = py3_json_dump
    long = int
    iteritems = py3_iteritems
    is_str = py3_is_str
else:
    btou = py2_btou
    utob = py2_utob
    json_dump = py2_json_dump
    iteritems = py2_iteritems
    long = long
    is_str = py2_is_str
