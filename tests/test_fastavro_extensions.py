import fastavro

from fastavro.six import MemoryIO


def test_fastavro_extensions():
    fo = MemoryIO()

    schema = {
        "type": "record",
        "name": "extension_test",
        "doc": "Complex schema with avro extensions",
        "fields": [
            {"name": "fixed_int8",  "type":
                {"type": "fixed", "name": "int8_t", "size": 1}},
            {"name": "fixed_int16", "type":
                {"type": "fixed", "name": "int16_t", "size": 2}},
            {"name": "fixed_int32", "type":
                {"type": "fixed", "name": "int32_t", "size": 4}},
            {"name": "fixed_int64", "type":
                {"type": "fixed", "name": "int64_t", "size": 8}},
            {"name": "fixed_uint8",  "type":
                {"type": "fixed", "name": "uint8_t", "size": 1}},
            {"name": "fixed_uint16", "type":
                {"type": "fixed", "name": "uint16_t", "size": 2}},
            {"name": "fixed_uint32", "type":
                {"type": "fixed", "name": "uint32_t", "size": 4}},
            {"name": "fixed_uint64", "type":
                {"type": "fixed", "name": "uint64_t", "size": 8}},
            {"name": "fixed_uint64_2", "type": "uint64_t"},
        ]
    }

    records = [
        {
            "fixed_int8": 127,
            "fixed_int16": -32768,
            "fixed_int32": 2147483647,
            "fixed_int64": 9223372036854775807,
            "fixed_uint8": 2**8 - 1,
            "fixed_uint16": 2**16 - 1,
            "fixed_uint32": 2**32 - 1,
            "fixed_uint64": 2**64 - 1,
            "fixed_uint64_2": 0,
        }, {
            "fixed_int8": 1,
            "fixed_int16": -2,
            "fixed_int32": 3,
            "fixed_int64": -4,
            "fixed_uint8": 10,
            "fixed_uint16": 20,
            "fixed_uint32": 30,
            "fixed_uint64": 40,
            "fixed_uint64_2": 1000,
        }
    ]

    fastavro.writer(fo, schema, records)

    fo.seek(0)
    new_reader = fastavro.reader(fo)

    assert new_reader.schema == schema

    new_records = list(new_reader)
    assert new_records == records
