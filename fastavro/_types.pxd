ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64
cdef union float_uint32:
    float f
    uint32 n