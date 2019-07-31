# cython: language_level=3str
# cython: auto_cpdef=True

'''Compatiblity for Python versions.

Some of this code is "lifted" from CherryPy.
'''
from __future__ import absolute_import
import sys
from struct import unpack

import json

_encoding = 'UTF-8'

if sys.version_info >= (3, 0):
    from io import BytesIO as MemoryIO
    from io import StringIO as StringIO
    xrange = range

    def py3_btou(n, encoding=_encoding):
        return n.decode(encoding)

    def py3_utob(n, encoding=_encoding):
        return bytes(n, encoding)

    def py3_json_dump(obj, indent):
        json.dump(obj, sys.stdout, indent=indent)

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

    def py3_appendable(file_like):
        if file_like.seekable() and file_like.tell() != 0:
            if "<stdout>" == getattr(file_like, "name", ""):
                # In OSX, sys.stdout is seekable and has a non-zero tell() but
                # we wouldn't want to append to a stdout. In the python REPL,
                # sys.stdout is named `<stdout>`
                return False
            if file_like.readable():
                return True
            else:
                raise ValueError(
                    "When appending to an avro file you must use the "
                    + "'a+' mode, not just 'a'"
                )
        else:
            return False

    def py3_int_to_be_signed_bytes(num, bytes_req):
        return num.to_bytes(bytes_req, byteorder='big', signed=True)

    def py3_be_signed_bytes_to_int(data):
        return int.from_bytes(data, byteorder='big', signed=True)


else:  # Python 2x
    from cStringIO import StringIO as MemoryIO  # noqa
    from cStringIO import StringIO as StringIO  # noqa
    xrange = xrange

    def py2_btou(n, encoding=_encoding):
        return unicode(n, encoding)  # noqa

    def py2_utob(n, encoding=_encoding):
        return n.encode(encoding)

    _outenc = getattr(sys.stdout, 'encoding', None) or _encoding

    def py2_json_dump(obj, indent):
        kwargs = {}
        if indent is not None:
            kwargs['indent'] = indent
        json.dump(obj, sys.stdout, encoding=_outenc, **kwargs)

    def py2_iterkeys(obj):
        return obj.iterkeys()

    def py2_itervalues(obj):
        return obj.itervalues()

    def py2_iteritems(obj):
        return obj.iteritems()

    def py2_is_str(obj):
        return isinstance(obj, basestring)  # noqa

    def py2_mk_bits(bits):
        return chr(bits & 0xff)

    def py2_str2ints(datum):
        return map(lambda x: ord(x), datum)

    def py2_fstint(datum):
        return unpack('!b', datum[0])[0]

    def _readable(file_like):
        try:
            file_like.read()
        except Exception:
            return False
        return True

    def py2_appendable(file_like):
        # On Python 2 things are a mess. We basically just rely on looking at
        # the mode. If that doesn't exist (like in the case of an io.BytesIO)
        # then we check the position and readablility.
        try:
            file_like.mode
        except AttributeError:
            # This is probably some io stream so we rely on its tell() working
            try:
                if file_like.tell() != 0 and _readable(file_like):
                    return True
            except (OSError, IOError):
                pass
            return False

        if "a" in file_like.mode:
            if "+" in file_like.mode:
                return True
            else:
                raise ValueError(
                    "When appending to an avro file you must use the "
                    + "'a+' mode, not just 'a'"
                )
        else:
            return False

    def py2_int_to_be_signed_bytes(num, bytes_req):
        if num < 0:
            num = 2 ** (8 * bytes_req) + num
        hex_str = '%x' % num
        hex_str = ((bytes_req * 2) - len(hex_str)) * '0' + hex_str
        return hex_str.decode('hex')

    def py2_be_signed_bytes_to_int(data):
        output = int(data.encode('hex'), 16)
        bitsize = len(data) * 8
        if output < (2 ** (bitsize - 1)):
            return output
        return output - (2 ** bitsize)

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
    appendable = py3_appendable
    int_to_be_signed_bytes = py3_int_to_be_signed_bytes
    be_signed_bytes_to_int = py3_be_signed_bytes_to_int
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
    appendable = py2_appendable
    int_to_be_signed_bytes = py2_int_to_be_signed_bytes
    be_signed_bytes_to_int = py2_be_signed_bytes_to_int
