# cython: auto_cpdef=True

'''Avro Extensions'''

FIXED_EXTENSIONS = {
    # name  : (size, pack/unpack fmt string)
    'int8_t': (1, 'b'),
    'int16_t': (2, 'h'),
    'int32_t': (4, 'i'),
    'int64_t': (8, 'l'),
    'uint8_t': (1, 'B'),
    'uint16_t': (2, 'H'),
    'uint32_t': (4, 'I'),
    'uint64_t': (8, 'L'),
}
