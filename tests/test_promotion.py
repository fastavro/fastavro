from io import BytesIO
import fastavro


def roundtrip(record, writer_schema, reader_schema):
    new_file = BytesIO()
    fastavro.writer(new_file, writer_schema, [record])
    new_file.seek(0)

    new_records = list(fastavro.reader(new_file, reader_schema))
    return new_records[0]


def test_int_promotion():
    int_schema = {
        "type": "int",
    }

    long_schema = {
        "type": "long",
    }

    result = roundtrip(1, int_schema, long_schema)
    assert result == 1
    assert isinstance(result, int)

    float_schema = {
        "type": "float",
    }

    result = roundtrip(1, int_schema, float_schema)
    assert result == 1.0
    assert isinstance(result, float)

    double_schema = {
        "type": "double",
    }

    result = roundtrip(1, int_schema, double_schema)
    assert result == 1.0
    # Python doesn't have a double type, but float is close enough
    assert isinstance(result, float)


def test_long_promotion():
    long_schema = {
        "type": "long",
    }

    float_schema = {
        "type": "float",
    }

    result = roundtrip(1, long_schema, float_schema)
    assert result == 1.0
    assert isinstance(result, float)

    double_schema = {
        "type": "double",
    }

    result = roundtrip(1, long_schema, double_schema)
    assert result == 1.0
    # Python doesn't have a double type, but float is close enough
    assert isinstance(result, float)


def test_float_promotion():
    float_schema = {
        "type": "float",
    }

    double_schema = {
        "type": "double",
    }

    result = roundtrip(1.0, float_schema, double_schema)
    assert result == 1.0
    # Python doesn't have a double type, but float is close enough
    assert isinstance(result, float)


def test_string_promotion():
    string_schema = {
        "type": "string",
    }

    bytes_schema = {
        "type": "bytes",
    }

    result = roundtrip("foo", string_schema, bytes_schema)
    assert result == b"foo"
    assert isinstance(result, bytes)


def test_bytes_promotion():
    bytes_schema = {
        "type": "bytes",
    }

    string_schema = {
        "type": "string",
    }

    result = roundtrip(b"foo", bytes_schema, string_schema)
    assert result == "foo"
