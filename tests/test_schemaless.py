from io import BytesIO
import fastavro

import pytest


def roundtrip(schema, record, *, writer_kwargs={}):
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, record, **writer_kwargs)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    return new_record


def test_schemaless_writer_and_reader():
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field", "type": {"type": "string"}}],
    }
    record = {"field": "test"}
    assert record == roundtrip(schema, record)


def test_schemaless_writer_and_reader_with_union():
    """Testing basic functionality of reader with union when option to
    return_record_name is true.
    """
    schema = {
        "name": "Message",
        "type": "record",
        "namespace": "test",
        "fields": [
            {"name": "id", "type": "long"},
            {
                "name": "payload",
                "type": [
                    {
                        "name": "ApplicationCreated",
                        "type": "record",
                        "fields": [
                            {"name": "applicationId", "type": "string"},
                            {"name": "data", "type": "string"},
                        ],
                    },
                    {
                        "name": "ApplicationSubmitted",
                        "type": "record",
                        "fields": [
                            {"name": "applicationId", "type": "string"},
                            {"name": "data", "type": "string"},
                        ],
                    },
                ],
            },
        ],
    }
    record = {
        "id": 123,
        "payload": (
            "test.ApplicationSubmitted",
            {"applicationId": "123456789UT", "data": "..."},
        ),
    }
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema, return_record_name=True)
    assert record == new_record


def test_boolean_roundtrip():
    schema = {
        "type": "record",
        "name": "test_boolean_roundtrip",
        "fields": [{"name": "field", "type": "boolean"}],
    }
    record = {"field": True}
    assert record == roundtrip(schema, record)

    record = {"field": False}
    assert record == roundtrip(schema, record)


def test_default_values_in_reader():
    writer_schema = {
        "name": "name1",
        "type": "record",
        "namespace": "namespace1",
        "fields": [{"doc": "test", "type": "int", "name": "good_field"}],
        "doc": "test",
    }

    reader_schema = {
        "name": "name1",
        "doc": "test",
        "namespace": "namespace1",
        "fields": [
            {"name": "good_field", "doc": "test", "type": "int"},
            {
                "name": "good_compatible_field",
                "doc": "test",
                "default": 1,
                "type": "int",
            },
        ],
        "type": "record",
    }

    record = {"good_field": 1}
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, writer_schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(
        new_file,
        writer_schema,
        reader_schema,
    )
    assert new_record == {"good_field": 1, "good_compatible_field": 1}


def test_newer_versions_of_named_schemas():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema_v1 = [
        {
            "name": "Location",
            "type": "record",
            "fields": [{"name": "city", "type": "string"}],
        },
        {
            "name": "Weather",
            "type": "record",
            "fields": [{"name": "of", "type": "Location"}],
        },
    ]

    schema_v2 = [
        {
            "name": "Location",
            "type": "record",
            "fields": [{"name": "city", "type": "long"}],
        },
        {
            "name": "Weather",
            "type": "record",
            "fields": [{"name": "of", "type": "Location"}],
        },
    ]

    example_1 = {"of": {"city": "London"}}
    example_2 = {"of": {"city": 123}}

    parse_v1 = fastavro.parse_schema(schema_v1)
    parse_v2 = fastavro.parse_schema(schema_v2)

    fastavro.schemaless_writer(BytesIO(), parse_v2, example_2)
    fastavro.schemaless_writer(BytesIO(), parse_v1, example_1)


def test_newer_versions_of_named_schemas_2():
    """https://github.com/fastavro/fastavro/issues/450"""
    schema = {
        "name": "Weather",
        "type": "record",
        "fields": [
            {
                "name": "place1",
                "type": {
                    "name": "Location",
                    "type": "record",
                    "fields": [{"name": "city", "type": "string"}],
                },
            },
            {
                "name": "place2",
                "type": "Location",
            },
        ],
    }

    example_1 = {"place1": {"city": "London"}, "place2": {"city": "Berlin"}}
    parsed_schema = fastavro.parse_schema(schema)

    assert example_1 == roundtrip(parsed_schema, example_1)


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
        roundtrip(schema, test_record1, writer_kwargs={"strict": True})

    with pytest.raises(ValueError, match="field_2 is specified .*? but missing"):
        roundtrip(schema, test_record2, writer_kwargs={"strict": True})

    with pytest.raises(ValueError, match="field_1 is specified .*? but missing"):
        roundtrip(schema, test_record3, writer_kwargs={"strict": True})


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
        roundtrip(schema, test_record1, writer_kwargs={"strict_allow_default": True})

    roundtrip(schema, test_record2, writer_kwargs={"strict_allow_default": True})

    with pytest.raises(ValueError, match="field_1 is specified .*? but missing"):
        roundtrip(schema, test_record3, writer_kwargs={"strict_allow_default": True})


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

    new_record = roundtrip(
        schema, {"foo": ("string", "0")}, writer_kwargs={"disable_tuple_notation": True}
    )
    assert new_record == {"foo": ["string", "0"]}


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
        roundtrip(schema, test_record, writer_kwargs={"strict_allow_default": True})
