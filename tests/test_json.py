from copy import deepcopy
from io import StringIO
import json

import pytest

from fastavro import json_writer, json_reader
from fastavro.schema import parse_schema
from fastavro.validation import ValidationError
from fastavro.io.json_decoder import AvroJSONDecoder
from fastavro.io.json_encoder import AvroJSONEncoder
from fastavro.io.symbols import MapKeyMarker, String


def roundtrip(schema, records, *, reader_schema=None, writer_kwargs={}):
    new_file = StringIO()
    json_writer(new_file, schema, records, **writer_kwargs)
    new_file.seek(0)

    reader = json_reader(new_file, schema, reader_schema)

    new_records = list(reader)
    return new_records


def test_json():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "null", "type": "null"},
            {"name": "boolean", "type": "boolean"},
            {"name": "string", "type": "string"},
            {"name": "bytes", "type": "bytes"},
            {"name": "int", "type": "int"},
            {"name": "long", "type": "long"},
            {"name": "float", "type": "float"},
            {"name": "double", "type": "double"},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            },
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
            },
            {"name": "array", "type": {"type": "array", "items": "string"}},
            {"name": "map", "type": {"type": "map", "values": "int"}},
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
            },
        ],
    }

    records = [
        {
            "null": None,
            "boolean": True,
            "string": "foo",
            "bytes": b"\xe2\x99\xa5",
            "int": 1,
            "long": 1 << 33,
            "float": 2.2,
            "double": 3.3,
            "fixed": b"\x61\x61\x61\x61\x61",
            "union": None,
            "enum": "BAR",
            "array": ["a", "b"],
            "map": {"c": 1, "d": 2},
            "record": {"sub_int": 123},
        },
        {
            "null": None,
            "boolean": True,
            "string": "foo",
            "bytes": b"\xe2\x99\xa5",
            "int": 1,
            "long": 1 << 33,
            "float": 2.2,
            "double": 3.3,
            "fixed": b"\x61\x61\x61\x61\x61",
            "union": 321,
            "enum": "BAR",
            "array": ["a", "b"],
            "map": {"c": 1, "d": 2},
            "record": {"sub_int": 123},
        },
        {
            "null": None,
            "boolean": True,
            "string": "foo",
            "bytes": b"\xe2\x99\xa5",
            "int": 1,
            "long": 1 << 33,
            "float": 2.2,
            "double": 3.3,
            "fixed": b"\x61\x61\x61\x61\x61",
            "union": {"union_record_field": "union_field"},
            "enum": "BAR",
            "array": ["a", "b"],
            "map": {"c": 1, "d": 2},
            "record": {"sub_int": 123},
        },
    ]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_more_than_one_record():
    schema = {
        "type": "record",
        "name": "test_more_than_one_record",
        "namespace": "test",
        "fields": [
            {"name": "string", "type": "string"},
            {"name": "int", "type": "int"},
        ],
    }

    records = [{"string": "foo", "int": 1}, {"string": "bar", "int": 2}]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_encoded_union_output():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            }
        ],
    }

    # A null value is encoded as just null
    records = [{"union": None}]
    new_file = StringIO()
    json_writer(new_file, schema, records)
    assert new_file.getvalue().strip() == json.dumps({"union": None})

    # A non-null, non-named type is encoded as an object with a key for the
    # type
    records = [{"union": 321}]
    new_file = StringIO()
    json_writer(new_file, schema, records)
    assert new_file.getvalue().strip() == json.dumps({"union": {"int": 321}})

    # A non-null, named type is encoded as an object with a key for the name
    records = [{"union": {"union_record_field": "union_field"}}]
    new_file = StringIO()
    json_writer(new_file, schema, records)
    expected = json.dumps(
        {"union": {"test.union_record": {"union_record_field": "union_field"}}}
    )
    assert new_file.getvalue().strip() == expected


def test_union_output_without_type():
    """https://github.com/fastavro/fastavro/issues/420"""
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
            }
        ],
    }

    # A null value is encoded as just null
    records = [{"union": None}]
    new_file = StringIO()
    json_writer(new_file, schema, records, write_union_type=False)
    assert new_file.getvalue().strip() == json.dumps({"union": None})

    # A non-null, non-named type is encoded as just the value
    records = [{"union": 321}]
    new_file = StringIO()
    json_writer(new_file, schema, records, write_union_type=False)
    assert new_file.getvalue().strip() == json.dumps({"union": 321})

    # A non-null, named type is encoded as an object
    records = [{"union": {"union_record_field": "union_field"}}]
    new_file = StringIO()
    json_writer(new_file, schema, records, write_union_type=False)
    expected = json.dumps({"union": {"union_record_field": "union_field"}})
    assert new_file.getvalue().strip() == expected


def test_union_string_and_bytes():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "union", "type": ["string", "bytes"]}],
    }

    records = [{"union": "asdf"}, {"union": b"asdf"}]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_simple_type():
    schema = {"type": "string"}

    records = ["foo", "bar"]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_array_type_simple():
    schema = {"type": "array", "items": "string"}

    records = [["foo", "bar"], ["a", "b"]]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_array_type_records():
    schema = {
        "type": "array",
        "items": {
            "type": "record",
            "name": "test_array_type",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"},
            ],
        },
    }

    records = [[{"field1": "foo", "field2": 1}], [{"field1": "bar", "field2": 2}]]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_empty_maps():
    """https://github.com/fastavro/fastavro/issues/380"""
    schema = {"type": "map", "values": "int"}

    records = [{"foo": 1}, {}]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_empty_arrays():
    """https://github.com/fastavro/fastavro/issues/380"""
    schema = {"type": "array", "items": "int"}

    records = [[1], []]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_union_in_array():
    """https://github.com/fastavro/fastavro/issues/399"""
    schema = {
        "type": "array",
        "items": [
            {
                "type": "record",
                "name": "rec1",
                "fields": [{"name": "field1", "type": ["string", "null"]}],
            },
            {
                "type": "record",
                "name": "rec2",
                "fields": [{"name": "field2", "type": ["string", "null"]}],
            },
            "null",
        ],
    }

    records = [
        [{"field1": "foo"}, {"field2": None}, None],
    ]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_union_in_array2():
    """https://github.com/fastavro/fastavro/issues/399"""
    schema = {
        "type": "record",
        "name": "Inbox",
        "fields": [
            {"type": "string", "name": "id"},
            {"type": "string", "name": "msg_title"},
            {
                "name": "msg_content",
                "type": {
                    "type": "array",
                    "items": [
                        {
                            "type": "record",
                            "name": "LimitedTime",
                            "fields": [
                                {
                                    "type": ["string", "null"],
                                    "name": "type",
                                    "default": "now",
                                }
                            ],
                        },
                        {
                            "type": "record",
                            "name": "Text",
                            "fields": [{"type": ["string", "null"], "name": "text"}],
                        },
                    ],
                },
            },
        ],
    }

    records = [
        {
            "id": 1234,
            "msg_title": "Hi",
            "msg_content": [{"type": "now"}, {"text": "hi from here!"}],
        },
    ]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_union_in_map():
    """https://github.com/fastavro/fastavro/issues/399"""
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {
                "name": "map",
                "type": {"type": "map", "values": ["string", "null"]},
            }
        ],
    }

    records = [{"map": {"c": "1", "d": None}}]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_with_dependent_schema():
    """Tests a schema with dependent schema
    https://github.com/fastavro/fastavro/issues/418"""
    dependency = {
        "type": "record",
        "name": "Dependency",
        "namespace": "test",
        "fields": [{"name": "_name", "type": "string"}],
    }

    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [
            {"name": "_name", "type": "string"},
            {"name": "_dependency", "type": "Dependency"},
        ],
    }

    records = [{"_name": "parent", "_dependency": {"_name": "child"}}]

    parsed_schema = parse_schema([dependency, schema])

    new_records = roundtrip(parsed_schema, records)
    assert records == new_records


def test_enum_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_enum_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {"type": "enum", "name": "my_enum", "symbols": ["FOO", "BAR"]},
            },
            {"name": "test2", "type": "my_enum"},
        ],
    }

    records = [{"test1": "FOO", "test2": "BAR"}]
    parsed_schema = parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_fixed_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_fixed_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {"type": "fixed", "name": "my_fixed", "size": 4},
            },
            {"name": "test2", "type": "my_fixed"},
        ],
    }

    records = [{"test1": b"1234", "test2": b"4321"}]
    parsed_schema = parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_record_named_type():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "type": "record",
        "name": "test_record_named_type",
        "fields": [
            {
                "name": "test1",
                "type": {
                    "type": "record",
                    "name": "my_record",
                    "fields": [{"name": "field1", "type": "string"}],
                },
            },
            {"name": "test2", "type": "my_record"},
        ],
    }

    records = [{"test1": {"field1": "foo"}, "test2": {"field1": "bar"}}]
    parsed_schema = parse_schema(schema)
    assert records == roundtrip(parsed_schema, records)


def test_default_union_values():
    """https://github.com/fastavro/fastavro/issues/485"""
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "long"},
            {
                "name": "pets",
                "type": {"type": "array", "items": "string"},
            },
            {
                "name": "accounts",
                "type": {"type": "map", "values": "long"},
            },
            {
                "name": "favorite_colors",
                "type": {
                    "type": "enum",
                    "name": "favorite_color",
                    "symbols": ["BLUE", "YELLOW", "GREEN"],
                },
            },
            {"name": "country", "type": ["string", "null"], "default": "Argentina"},
            {"name": "address", "type": ["null", "string"], "default": None},
        ],
        "doc": "An User",
        "namespace": "User.v1",
        "aliases": ["user-v1", "super user"],
    }

    record = {
        "name": "MgXqfDAqzbgJSTTHDXtN",
        "age": 551,
        "pets": ["aRvwODwbOWfrkxYYkJiI"],
        "accounts": {"DQSZRzofFrNCiOhhIOvX": 4431},
        "favorite_colors": "GREEN",
        "address": {"string": "YgmVDKhXctMgODKkhNHJ"},
    }

    new_file = StringIO(json.dumps(record))
    read_record = next(json_reader(new_file, schema))

    assert read_record["country"] == "Argentina"


def test_all_default_values():
    """https://github.com/fastavro/fastavro/issues/485"""
    default_boolean = True
    default_string = "default_string"
    default_bytes = "default_bytes"
    default_int = -1
    default_long = -2
    default_float = 1.1
    default_double = 2.2
    default_fixed = "12345"
    default_union = None
    default_enum = "FOO"
    default_array = ["a", "b"]
    default_map = {"a": 1, "b": 2}
    default_record = {"sub_int": -3}
    schema = {
        "type": "record",
        "name": "test_all_default_values",
        "fields": [
            {"name": "boolean", "type": "boolean", "default": default_boolean},
            {"name": "string", "type": "string", "default": default_string},
            {"name": "bytes", "type": "bytes", "default": default_bytes},
            {"name": "int", "type": "int", "default": default_int},
            {"name": "long", "type": "long", "default": default_long},
            {"name": "float", "type": "float", "default": default_float},
            {"name": "double", "type": "double", "default": default_double},
            {
                "name": "fixed",
                "type": {"type": "fixed", "name": "fixed_field", "size": 5},
                "default": default_fixed,
            },
            {
                "name": "union",
                "type": [
                    "null",
                    "int",
                    {
                        "type": "record",
                        "name": "union_record",
                        "fields": [{"name": "union_record_field", "type": "string"}],
                    },
                ],
                "default": default_union,
            },
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "name": "enum_field",
                    "symbols": ["FOO", "BAR"],
                },
                "default": default_enum,
            },
            {
                "name": "array",
                "type": {"type": "array", "items": "string"},
                "default": deepcopy(default_array),
            },
            {
                "name": "map",
                "type": {"type": "map", "values": "int"},
                "default": deepcopy(default_map),
            },
            {
                "name": "record",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [{"name": "sub_int", "type": "int"}],
                },
                "default": default_record,
            },
        ],
    }

    record = {}

    new_file = StringIO(json.dumps(record))
    read_record = next(json_reader(new_file, schema))

    assert read_record["boolean"] == default_boolean
    assert read_record["string"] == default_string
    assert read_record["bytes"] == default_bytes.encode("iso-8859-1")
    assert read_record["int"] == default_int
    assert read_record["long"] == default_long
    assert read_record["float"] == default_float
    assert read_record["double"] == default_double
    assert read_record["fixed"] == default_fixed.encode("iso-8859-1")
    assert read_record["union"] == default_union
    assert read_record["enum"] == default_enum
    assert read_record["array"] == default_array
    assert read_record["map"] == default_map
    assert read_record["record"] == default_record


def test_default_value_missing():
    """https://github.com/fastavro/fastavro/issues/485"""
    schema = {
        "type": "record",
        "name": "test_default_value_missing",
        "fields": [{"name": "string", "type": "string"}],
    }

    record = {}

    new_file = StringIO(json.dumps(record))
    with pytest.raises(ValueError, match="no value and no default"):
        next(json_reader(new_file, schema))


def test_map_of_union_of_array_and_map():
    """https://github.com/fastavro/fastavro/issues/572"""
    schema = {
        "name": "Test",
        "namespace": "test",
        "type": "record",
        "fields": [
            {
                "name": "metadata",
                "type": {
                    "type": "map",
                    "values": [
                        {"type": "array", "items": "string"},
                        {"type": "map", "values": ["string"]},
                    ],
                },
            }
        ],
    }

    records = [{"metadata": {"map1": {"map2": "str"}}}]

    new_records = roundtrip(schema, records)
    assert records == new_records


def test_json_writer_with_validation():
    """https://github.com/fastavro/fastavro/issues/580"""
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": "last day"},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111.9, "time": 1433275478},
    ]

    new_file = StringIO()
    with pytest.raises(ValidationError):
        json_writer(new_file, schema, records, validator=True)


def test_custom_encoder_and_decoder():
    """https://github.com/fastavro/fastavro/pull/579"""

    class CustomJSONEncoder(AvroJSONEncoder):
        """Encoder that will prepend an underscore to all string values"""

        def write_utf8(self, value):
            self._parser.advance(String())
            if self._parser.stack[-1] == MapKeyMarker():
                self._parser.advance(MapKeyMarker())
                self.write_object_key(value)
            else:
                self.write_value("_" + value)

    class CustomJSONDecoder(AvroJSONDecoder):
        """Decoder that will prepend an underscore to all string values"""

        def read_utf8(self):
            symbol = self._parser.advance(String())
            if self._parser.stack[-1] == MapKeyMarker():
                self._parser.advance(MapKeyMarker())
                for key in self._current:
                    self._key = key
                    break
                return self._key
            else:
                return "_" + self.read_value(symbol)

    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433269389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111.9, "time": 1433275478},
    ]

    new_file = StringIO()
    json_writer(new_file, schema, records, encoder=CustomJSONEncoder)
    assert "_011990-99999" in new_file.getvalue()

    new_file.seek(0)
    for record in json_reader(new_file, schema, decoder=CustomJSONDecoder):
        assert record["station"].startswith("__")


def test_strict_option():
    """https://github.com/fastavro/fastavro/issues/549"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {"name": "field_1", "type": "boolean"},
            {"name": "field_2", "type": ["null", "string"], "default": None},
        ],
    }

    test_record1 = {"field_1": True, "field_2": "foo", "field_3": "something"}
    test_record2 = {"field_1": True}
    test_record3 = {"field_2": "foo"}

    with pytest.raises(ValueError, match="field_3"):
        roundtrip(schema, [test_record1], writer_kwargs={"strict": True})

    with pytest.raises(ValueError, match="field_2 is specified .*? but missing"):
        roundtrip(schema, [test_record2], writer_kwargs={"strict": True})

    with pytest.raises(ValueError, match="field_1 is specified .*? but missing"):
        roundtrip(schema, [test_record3], writer_kwargs={"strict": True})


def test_strict_allow_default_option():
    """https://github.com/fastavro/fastavro/issues/549"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {"name": "field_1", "type": "boolean"},
            {"name": "field_2", "type": ["null", "string"], "default": None},
        ],
    }

    test_record1 = {"field_1": True, "field_2": "foo", "field_3": "something"}
    test_record2 = {"field_1": True}
    test_record3 = {"field_2": "foo"}

    with pytest.raises(ValueError, match="field_3"):
        roundtrip(schema, [test_record1], writer_kwargs={"strict_allow_default": True})

    roundtrip(schema, [test_record2], writer_kwargs={"strict_allow_default": True})

    with pytest.raises(ValueError, match="field_1 is specified .*? but missing"):
        roundtrip(schema, [test_record3], writer_kwargs={"strict_allow_default": True})


def test_json_with_map():
    """https://github.com/fastavro/fastavro/issues/629"""
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "long"},
            {
                "name": "addresses",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "street_number", "type": "long"},
                        ],
                        "doc": "An Address",
                    },
                    "name": "address",
                },
            },
        ],
        "doc": "User with multiple Address",
    }

    payload = {
        "name": "TogYzVenzFrgcVunpkfo",
        "age": 5694,
        "addresses": {
            "HGbZkCCabEbwaTXjbTEA": {
                "street": "tNXiLgAYswaCPLazSfus",
                "street_number": 1316,
            }
        },
    }

    roundtrip(schema, [payload])


def test_disable_tuple_notation_option():
    """https://github.com/fastavro/fastavro/issues/548"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [
            {"name": "foo", "type": ["string", {"type": "array", "items": "string"}]}
        ],
    }

    new_records = roundtrip(
        schema,
        [{"foo": ("string", "0")}],
        writer_kwargs={"disable_tuple_notation": True},
    )
    assert new_records == [{"foo": ["string", "0"]}]


def test_strict_allow_default_bug():
    """https://github.com/fastavro/fastavro/issues/638"""
    schema = {
        "namespace": "namespace",
        "name": "name",
        "type": "record",
        "fields": [{"name": "some_field", "type": "string", "default": "test"}],
    }

    test_record = {"eggs": "eggs"}

    with pytest.raises(ValueError, match="record contains more fields .*? eggs"):
        roundtrip(schema, [test_record], writer_kwargs={"strict_allow_default": True})


def test_json_aliases():
    """https://github.com/fastavro/fastavro/issues/669"""
    schema = {
        "name": "test_json_aliases",
        "type": "record",
        "fields": [
            {"name": "field_1", "type": "string"},
            {"name": "field_2", "type": "long"},
        ],
    }

    reader_schema = {
        "name": "test_json_aliases",
        "type": "record",
        "fields": [
            {"name": "new_field_1", "type": "string", "aliases": ["field_1"]},
            {"name": "field_2", "type": "long"},
        ],
    }

    records = [{"field_1": "foo", "field_2": 10}]

    output_records = roundtrip(schema, records, reader_schema=reader_schema)
    assert output_records == [{"new_field_1": "foo", "field_2": 10}]
