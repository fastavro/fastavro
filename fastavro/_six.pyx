# cython: auto_cpdef=True

'''Compatiblity for Python versions.

Some of this code is "lifted" from CherryPy.
'''
import sys
from sys import stdout
from struct import unpack

_HAS_UJSON = False

try:
    import ujson as json

    _HAS_UJSON = True
except ImportError:
    import json

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

    def py3_iterkeys(obj):
        return obj.keys()

    def py3_itervalues(obj):
        return obj.values()

    def py3_iteritems(obj):
        return obj.items()

    def py3_is_str(obj):
        return isinstance(obj, str)

    def py3_mk_bits(bits):
        return bytes([bits & 0xff])

    def py3_bytes2ints(datum):
        return list(datum)

    def py3_fstint(datum):
        return datum[0]

else:  # Python 2x
    from cStringIO import StringIO as MemoryIO  # flake8: noqa
    xrange = xrange

    def py2_btou(n, encoding=_encoding):
        return unicode(n, encoding) # flake8: noqa

    def py2_utob(n, encoding=_encoding):
        return n.encode(encoding)

    _outenc = getattr(stdout, 'encoding', None) or _encoding

    def py2_json_dump(obj, indent):
        if _HAS_UJSON:
            json.dump(obj, stdout, indent=indent)
        else:
            json.dump(obj, stdout, indent=indent, encoding=_outenc)

    def py2_iterkeys(obj):
        return obj.iterkeys()

    def py2_itervalues(obj):
        return obj.itervalues()

    def py2_iteritems(obj):
        return obj.iteritems()

    def py2_is_str(obj):
        return isinstance(obj, basestring) # flake8: noqa

    def py2_mk_bits(bits):
        return chr(bits & 0xff)

    def py2_str2ints(datum):
        return map(lambda x:ord(x), datum)

    def py2_fstint(datum):
        return unpack('!b', datum[0])[0]

# We do it this way and not just redifine function since Cython do not like it
if sys.version_info >= (3, 0):
    btou = py3_btou
    utob = py3_utob
    json_dump = py3_json_dump
    long = int
    iterkeys = py3_iterkeys
    itervalues = py3_itervalues
    iteritems = py3_iteritems
    is_str = py3_is_str
    mk_bits = py3_mk_bits
    str2ints = py3_bytes2ints
    fstint = py3_fstint
else:
    btou = py2_btou
    utob = py2_utob
    json_dump = py2_json_dump
    iterkeys = py2_iterkeys
    itervalues = py2_itervalues
    iteritems = py2_iteritems
    long = long
    is_str = py2_is_str
    mk_bits = py2_mk_bits
    str2ints = py2_str2ints
    fstint = py2_fstint
