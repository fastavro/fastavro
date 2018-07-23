from .._types cimport long64

cdef class BinaryDecoder(object):
    cdef public object fo
    cdef long64 _block_count

    cpdef read_null(self)
    cpdef read_boolean(self)
    cpdef long64 read_long(self) except? -1
    cpdef read_int(self)
    cpdef read_float(self)
    cpdef read_double(self)
    cpdef read_bytes(self)
    cpdef read_utf8(self)
    cpdef read_fixed(self, int size)
    cpdef read_enum(self)
    cpdef read_array_start(self)
    cpdef read_array_end(self)
    cpdef iter_array(self)
    cpdef iter_map(self)
    cpdef read_map_start(self)
    cpdef read_map_end(self)
    cpdef read_index(self)
