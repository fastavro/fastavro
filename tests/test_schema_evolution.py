from fastavro import writer as fastavro_writer
from fastavro.read import SchemaResolutionError
import fastavro

import pytest

from io import BytesIO

schema_dict_a = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [{"name": "a", "type": "int"}],
}

record_a = {"a": 123}

schema_dict_a_b = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [
        {"name": "a", "type": "int"},
        {"name": "b", "type": ["null", "int"], "default": None},
    ],
}

record_a_b = {"a": 234, "b": 345}

schema_dict_a_c = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [{"name": "a", "type": "int"}, {"name": "c", "type": ["null", "int"]}],
}


def avro_to_bytes_with_schema(avro_schema, avro_dict):
    with BytesIO() as bytes_io:
        fastavro_writer(bytes_io, avro_schema, [avro_dict])
        return bytes_io.getvalue()


def bytes_with_schema_to_avro(avro_read_schema, binary):
    with BytesIO(binary) as bytes_io:
        reader = fastavro.reader(bytes_io, avro_read_schema)
        return next(reader)


def test_evolution_drop_field():
    record_bytes_a_b = avro_to_bytes_with_schema(schema_dict_a_b, record_a_b)
    record_a = bytes_with_schema_to_avro(schema_dict_a, record_bytes_a_b)
    assert "b" not in record_a


def test_evolution_add_field_with_default():
    record_bytes_a = avro_to_bytes_with_schema(schema_dict_a, record_a)
    record_b = bytes_with_schema_to_avro(schema_dict_a_b, record_bytes_a)
    assert "b" in record_b
    assert record_b.get("b") is None


def test_evolution_add_field_without_default():
    with pytest.raises(SchemaResolutionError):
        record_bytes_a = avro_to_bytes_with_schema(schema_dict_a, record_a)
        bytes_with_schema_to_avro(schema_dict_a_c, record_bytes_a)


def test_enum_evolution_no_default_failure():
    original_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["FOO", "BAR"],
    }

    new_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["BAZ", "BAR"],
    }

    original_records = ["FOO"]

    bio = BytesIO()
    fastavro.writer(bio, original_schema, original_records)
    bio.seek(0)

    with pytest.raises(fastavro.read.SchemaResolutionError):
        list(fastavro.reader(bio, new_schema))


def test_enum_evolution_using_default():
    original_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["A", "B"],
    }

    new_schema = {
        "type": "enum",
        "name": "test",
        "symbols": ["C", "D"],
        "default": "C",
    }

    original_records = ["A"]

    bio = BytesIO()
    fastavro.writer(bio, original_schema, original_records)
    bio.seek(0)

    new_records = list(fastavro.reader(bio, new_schema))
    assert new_records == ["C"]


def test_schema_matching_with_records_in_arrays():
    """https://github.com/fastavro/fastavro/issues/363"""
    original_schema = {
        "type": "record",
        "name": "DataRecord",
        "fields": [
            {
                "name": "string1",
                "type": "string",
            },
            {
                "name": "subrecord",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "SubRecord",
                        "fields": [
                            {
                                "name": "string2",
                                "type": "string",
                            }
                        ],
                    },
                },
            },
        ],
    }

    new_schema = {
        "type": "record",
        "name": "DataRecord",
        "fields": [
            {
                "name": "string1",
                "type": "string",
            },
            {
                "name": "subrecord",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "SubRecord",
                        "fields": [
                            {
                                "name": "string2",
                                "type": "string",
                            },
                            {
                                "name": "logs",
                                "default": None,
                                "type": [
                                    "null",
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "record",
                                            "name": "LogRecord",
                                            "fields": [
                                                {
                                                    "name": "msg",
                                                    "type": "string",
                                                    "default": "",
                                                }
                                            ],
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                },
            },
        ],
    }

    record = {
        "string1": "test",
        "subrecord": [{"string2": "foo"}],
    }

    binary = avro_to_bytes_with_schema(original_schema, record)

    output_using_original_schema = bytes_with_schema_to_avro(original_schema, binary)
    assert output_using_original_schema == record

    output_using_new_schema = bytes_with_schema_to_avro(new_schema, binary)
    assert output_using_new_schema == {
        "string1": "test",
        "subrecord": [{"string2": "foo", "logs": None}],
    }


def test_schema_migrate_record_to_union():
    """https://github.com/fastavro/fastavro/issues/406"""
    original_schema = {
        "name": "Item",
        "type": "record",
        "fields": [
            {
                "name": "category",
                "type": {
                    "type": "record",
                    "name": "Category",
                    "fields": [{"name": "name", "type": "string"}],
                },
            }
        ],
    }

    new_schema_record_first = {
        "name": "Item",
        "type": "record",
        "fields": [
            {
                "name": "category",
                "type": [
                    {
                        "type": "record",
                        "name": "Category",
                        "fields": [{"name": "name", "type": "string"}],
                    },
                    "null",
                ],
            }
        ],
    }

    new_schema_null_first = {
        "name": "Item",
        "type": "record",
        "fields": [
            {
                "name": "category",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Category",
                        "fields": [{"name": "name", "type": "string"}],
                    },
                ],
            }
        ],
    }

    record = {"category": {"name": "my-category"}}

    binary = avro_to_bytes_with_schema(original_schema, record)

    output_using_original_schema = bytes_with_schema_to_avro(original_schema, binary)
    assert output_using_original_schema == record

    output_using_new_schema_record_first = bytes_with_schema_to_avro(
        new_schema_record_first, binary
    )
    assert output_using_new_schema_record_first == record

    output_using_new_schema_null_first = bytes_with_schema_to_avro(
        new_schema_null_first, binary
    )
    assert output_using_new_schema_null_first == record


def test_union_of_lists_evolution_with_doc():
    """https://github.com/fastavro/fastavro/issues/486"""
    original_schema = {
        "name": "test_union_of_lists_evolution_with_doc",
        "type": "record",
        "fields": [
            {
                "name": "id",
                "type": [
                    "null",
                    {
                        "name": "some_record",
                        "type": "record",
                        "fields": [{"name": "field", "type": "string"}],
                    },
                ],
            }
        ],
    }

    new_schema = {
        "name": "test_union_of_lists_evolution_with_doc",
        "type": "record",
        "fields": [
            {
                "name": "id",
                "type": [
                    "null",
                    {
                        "name": "some_record",
                        "type": "record",
                        "doc": "some documentation",
                        "fields": [{"name": "field", "type": "string"}],
                    },
                ],
            }
        ],
    }

    record = {"id": {"field": "foo"}}

    binary = avro_to_bytes_with_schema(original_schema, record)

    output_using_new_schema = bytes_with_schema_to_avro(new_schema, binary)
    assert output_using_new_schema == record


def test_union_of_lists_evolution_with_extra_type():
    """https://github.com/fastavro/fastavro/issues/486"""
    original_schema = {
        "name": "test_union_of_lists_evolution_with_extra_type",
        "type": "record",
        "fields": [
            {
                "name": "id",
                "type": [
                    "null",
                    {
                        "name": "some_record",
                        "type": "record",
                        "fields": [{"name": "field", "type": "string"}],
                    },
                ],
            }
        ],
    }

    new_schema = {
        "name": "test_union_of_lists_evolution_with_extra_type",
        "type": "record",
        "fields": [
            {
                "name": "id",
                "type": [
                    "null",
                    {
                        "name": "some_record",
                        "type": "record",
                        "fields": [{"name": "field", "type": "string"}],
                    },
                    "string",
                ],
            }
        ],
    }

    record = {"id": {"field": "foo"}}

    binary = avro_to_bytes_with_schema(original_schema, record)

    output_using_new_schema = bytes_with_schema_to_avro(new_schema, binary)
    assert output_using_new_schema == record
