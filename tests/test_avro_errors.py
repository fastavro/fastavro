from fastavro.reader import read_data
from fastavro.writer import write_data

from fastavro.six import MemoryIO
from fastavro.errors import AvroValueError


def test_fastavro_errors_write_record():
    fo = MemoryIO()

    schema = {
        "type": "record",
        "name": "extension_test",
        "doc": "Complex schema with avro extensions",
        "fields": [
            {"name": "x",
             "type": {
                "type": "record",
                "name": "inner",
                "fields": [
                    {"name": "y", "type": "int"}
                ]
             }}
        ]
    }

    given = {"x": {"y": "hello, world"}}
    try:
        write_data(fo, given, schema)
        assert False, 'bad schema did not raise exception!'
    except AvroValueError as e:
        assert "<record>.x.<record>.y" in str(e)


def test_fastavro_errors_read_record():
    fo = MemoryIO()

    writer_schema = {
        "type": "record",
        "name": "extension_test",
        "doc": "Complex schema with avro extensions",
        "fields": [
            {"name": "x",
             "type": {
                "type": "record",
                "name": "inner",
                "fields": [
                    {"name": "y", "type": "int"}
                ]
             }}
        ]
    }

    reader_schema = {
        "type": "record",
        "name": "extension_test",
        "doc": "Complex schema with avro extensions",
        "fields": [
            {"name": "x",
             "type": {
                "type": "record",
                "name": "inner",
                "fields": [
                    {"name": "y", "type": "float"}
                ]
             }}
        ]
    }

    given = {"x": {"y": 0}}

    write_data(fo, given, writer_schema)
    fo.seek(0)
    try:
        read_data(fo, reader_schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        assert '<record>.x.<record>.y' in str(e)


def test_fastavro_errors_write_map():
    fo = MemoryIO()

    schema = {
        "type": "map",
        "values": "float"
    }

    given = {"x": "asdf"}

    try:
        write_data(fo, given, schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        assert '<map>.x' in str(e)


def test_fastavro_errors_read_map():
    fo = MemoryIO()

    writer_schema = {
        "type": "map",
        "values": "float"
    }

    reader_schema = {
        "type": "map",
        "values": "double"
    }

    given = {"x": 0}

    write_data(fo, given, writer_schema)
    fo.seek(0)
    try:
        read_data(fo, reader_schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        assert '<map>.x.<double>' in str(e)


def test_fastavro_errors_write_enum():
    fo = MemoryIO()

    schema = {
        "type": "enum",
        "name": "Suit",
        "symbols": [
            "SPADES",
            "HEARTS",
            "DIAMONDS",
            "CLUBS",
        ]
    }

    given = "POTS"

    try:
        write_data(fo, given, schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        assert '<enum>' in str(e)


def test_fastavro_errors_read_enum():
    fo = MemoryIO()

    writer_schema = {
        "type": "enum",
        "name": "Suit",
        "symbols": [
            "SPADES",
            "HEARTS",
            "DIAMONDS",
            "CLUBS",
        ]
    }

    reader_schema = {
        "type": "enum",
        "name": "Suit",
        "symbols": [
            "SPADES",
            "HEARTS",
            "DIAMONDS",
        ]
    }

    given = "CLUBS"

    write_data(fo, given, writer_schema)
    fo.seek(0)
    try:
        read_data(fo, reader_schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        assert '<enum>' in str(e)


def test_fastavro_errors_write_array():
    fo = MemoryIO()

    schema = {
        "type": "array",
        "items": "int",
    }

    given = [0, "hello"]

    try:
        write_data(fo, given, schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        assert '<array>.[1]' in str(e)


def test_fastavro_errors_read_array():
    fo = MemoryIO()

    writer_schema = {
        "type": "array",
        "items": "int",
    }

    reader_schema = {
        "type": "array",
        "items": "float",
    }

    given = [10, 20, 30]

    write_data(fo, given, writer_schema)
    fo.seek(0)
    try:
        read_data(fo, reader_schema)
        assert False, 'bad schema did not raise!'
    except AvroValueError as e:
        # .[1] because the first element is read succesfully
        # (but would be corrupt)
        assert '<array>.[1].<float>' in str(e)
