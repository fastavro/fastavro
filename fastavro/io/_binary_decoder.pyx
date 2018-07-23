from struct import unpack

from ..six import xrange, btou
from .._types cimport int32, uint32, ulong64, long64

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.


cdef union float_uint32:
    float f
    uint32 n


cdef union double_ulong64:
    double d
    ulong64 n


class ReadError(Exception):
    pass


cdef class BinaryDecoder(object):
    """Decoder for the avro binary format.

    NOTE: All attributes and methods on this class should be considered
    private.

    Parameters
    ----------
    fo: file-like
        Input stream


    Example::

        from fastavro.io import reader, BinaryDecoder

        with open('some-file.avro', 'rb') as fo:
            for record in reader(BinaryDecoder(fo), schema):
                print(record)
    """
    def __cinit__(self, fo):
        self.fo = fo

    cpdef read_null(self):
        """null is written as zero bytes."""
        return None

    cpdef read_boolean(self):
        """A boolean is written as a single byte whose value is either 0
        (false) or 1 (true).
        """
        cdef unsigned char ch_temp
        cdef bytes bytes_temp = self.fo.read(1)
        if len(bytes_temp) == 1:
            # technically 0x01 == true and 0x00 == false, but many languages will
            # cast anything other than 0 to True and only 0 to False
            ch_temp = bytes_temp[0]
            return ch_temp != 0
        else:
            raise ReadError

    cpdef long64 read_long(self) except? -1:
        """int and long values are written using variable-length, zig-zag
        coding."""
        cdef ulong64 b
        cdef ulong64 n
        cdef int32 shift
        cdef bytes c = self.fo.read(1)

        # We do EOF checking only here, since most reader start here
        if not c:
            raise StopIteration

        b = <unsigned char>(c[0])
        n = b & 0x7F
        shift = 7

        while (b & 0x80) != 0:
            c = self.fo.read(1)
            b = <unsigned char>(c[0])
            n |= (b & 0x7F) << shift
            shift += 7

        return (n >> 1) ^ -(n & 1)

    cpdef read_int(self):
        """int and long values are written using variable-length, zig-zag
        coding."""
        return self.read_long()

    cpdef read_float(self):
        """A float is written as 4 bytes.

        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        cdef bytes data
        cdef unsigned char ch_data[4]
        cdef float_uint32 fi
        data = self.fo.read(4)
        if len(data) == 4:
            ch_data[:4] = data
            fi.n = (ch_data[0] |
                    (ch_data[1] << 8) |
                    (ch_data[2] << 16) |
                    (ch_data[3] << 24))
            return fi.f
        else:
            raise ReadError

    cpdef read_double(self):
        """A double is written as 8 bytes.

        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        cdef bytes data
        cdef unsigned char ch_data[8]
        cdef double_ulong64 dl
        data = self.fo.read(8)
        if len(data) == 8:
            ch_data[:8] = data
            dl.n = (ch_data[0] |
                    (<ulong64>(ch_data[1]) << 8) |
                    (<ulong64>(ch_data[2]) << 16) |
                    (<ulong64>(ch_data[3]) << 24) |
                    (<ulong64>(ch_data[4]) << 32) |
                    (<ulong64>(ch_data[5]) << 40) |
                    (<ulong64>(ch_data[6]) << 48) |
                    (<ulong64>(ch_data[7]) << 56))
            return dl.d
        else:
            raise ReadError

    cpdef read_bytes(self):
        """Bytes are encoded as a long followed by that many bytes of data."""
        cdef long64 size = self.read_long()
        return self.fo.read(<long>size)

    cpdef read_utf8(self):
        """A string is encoded as a long followed by that many bytes of UTF-8
        encoded character data.
        """
        return btou(self.read_bytes(), 'utf-8')

    cpdef read_fixed(self, int size):
        """Fixed instances are encoded using the number of bytes declared in the
        schema."""
        return self.fo.read(size)

    cpdef read_enum(self):
        """An enum is encoded by a int, representing the zero-based position of the
        symbol in the schema.
        """
        return self.read_long()

    cpdef read_array_start(self):
        """Arrays are encoded as a series of blocks."""
        self._block_count = self.read_long()

    cpdef read_array_end(self):
        pass

    def _iter_array_or_map(self):
        """Each block consists of a long count value, followed by that many
        array items. A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative, then the count is followed immediately
        by a long block size, indicating the number of bytes in the block.
        The actual count in this case is the absolute value of the count
        written.
        """
        while self._block_count != 0:
            if self._block_count < 0:
                self._block_count = -self._block_count
                # Read block size, unused
                self.read_long()

            for i in range(self._block_count):
                yield
            self._block_count = self.read_long()

    cpdef iter_array(self):
        return self._iter_array_or_map()

    cpdef iter_map(self):
        return self._iter_array_or_map()

    cpdef read_map_start(self):
        """Maps are encoded as a series of blocks."""
        self._block_count = self.read_long()

    cpdef read_map_end(self):
        pass

    cpdef read_index(self):
        """A union is encoded by first writing a long value indicating the
        zero-based position within the union of the schema of its value.

        The value is then encoded per the indicated schema within the union.
        """
        return self.read_long()
