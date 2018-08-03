from .._types cimport long64

cdef class BinaryDecoder(object):
    cdef public object fo
    cdef long64 _block_count

    cpdef read_null(self)
    cdef read_boolean(self)
    cpdef long64 read_long(self) except? -1
