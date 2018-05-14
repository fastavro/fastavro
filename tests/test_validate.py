from fastavro.write import _validate


def test_validator_numeric():
    for datum, schema in [
        (1, 'int'),
        (1, 'long'),
        (1.0, 'float'),
        (1.0, 'double'),
        (1, 'float'),
        (1, 'double'),
    ]:
        assert _validate(datum, schema)

    for datum, schema in [
        (1.0, 'int'),
        (1.0, 'long'),
        ("1.0", 'float'),
        ("1.0", 'double'),
        ("1", 'float'),
        ("1", 'double'),
    ]:
        assert not _validate(datum, schema)
    # and plenty more to add I suppose


def test_validator_numeric_numpy():
    import numpy as np
    np_ints = [
        np.int_,
        np.intc,
        np.intp,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
    ]

    np_floats = [
        np.float_,
        np.float16,
        np.float32,
        np.float64,
    ]

    schema_ints = ['int', 'long']

    schema_floats = ['float', 'double']

    # all these should work
    for nptype, schema in zip(np_ints, schema_ints):
        assert _validate(nptype(1), schema)

    for nptype, schema in zip(np_ints, schema_floats):
        assert _validate(nptype(1), schema)

    for nptype, schema in zip(np_floats, schema_floats):
        assert _validate(nptype(1), schema)

    # these shouldn't work
    for nptype, schema in zip(np_floats, schema_ints):
        assert not _validate(nptype(1), schema)
