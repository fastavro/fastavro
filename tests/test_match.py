import pytest

from fastavro._read_common import SchemaResolutionError
from fastavro import _read_py

try:
    from fastavro._read import match_types, match_schemas
except ImportError:
    from fastavro._read_py import match_types, match_schemas  # type: ignore

MATCH_SCHEMAS_FNS = [match_schemas]
if match_schemas is not _read_py.match_schemas:
    MATCH_SCHEMAS_FNS.append(_read_py.match_schemas)


def _default_named_schemas():
    return {"writer": {}, "reader": {}}


@pytest.mark.parametrize(
    "writer,reader",
    [
        ("int", "int"),
        ("int", "long"),
        ("int", "float"),
        ("int", "double"),
        ("long", "long"),
        ("long", "float"),
        ("long", "double"),
        ("float", "float"),
        ("float", "double"),
        ("string", "string"),
        ("string", "bytes"),
        ("bytes", "bytes"),
        ("bytes", "string"),
        (["any"], ["dontcare"]),
        ({"type": "int"}, {"type": "int"}),
    ],
)
def test_match_types_returns_true(writer, reader):
    assert match_types(writer, reader, _default_named_schemas())


@pytest.mark.parametrize(
    "writer,reader",
    [
        ("int", "string"),
        ("long", "int"),
        ("float", "long"),
        ("string", "int"),
        ("bytes", "int"),
        ({"type": "int"}, {"type": "string"}),
    ],
)
def test_match_types_returns_false(writer, reader):
    assert not match_types(writer, reader, _default_named_schemas())


@pytest.mark.parametrize(
    "writer,reader,named_schemas",
    [
        ({"type": "int"}, {"type": "int"}, None),
        (
            "test.Writer",
            "test.Reader",
            {
                "writer": {
                    "test.Writer": "int",
                },
                "reader": {
                    "test.Reader": "int",
                },
            },
        ),
    ],
)
def test_match_schemas_returns_right_schema(writer, reader, named_schemas):
    assert reader == match_schemas(writer, reader, named_schemas)


@pytest.mark.parametrize(
    "writer,reader",
    [
        ({"type": "int"}, {"type": "string"}),
    ],
)
def test_match_schemas_raises_exception(writer, reader):
    with pytest.raises(SchemaResolutionError) as err:
        match_schemas(writer, reader, _default_named_schemas())

    error_msg = f"Schema mismatch: {writer} is not {reader}"
    assert str(err.value) == error_msg


@pytest.mark.parametrize(
    "writer,reader,named_schemas,expected",
    [
        (
            {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]},
            "Color",
            {
                "writer": {},
                "reader": {
                    "Color": {
                        "type": "enum",
                        "name": "Color",
                        "symbols": ["RED", "GREEN"],
                    }
                },
            },
            {
                "type": "enum",
                "name": "Color",
                "symbols": ["RED", "GREEN"],
            },
        ),
        (
            {
                "type": "record",
                "name": "Sub",
                "fields": [{"name": "val", "type": "int"}],
            },
            "Sub",
            {
                "writer": {},
                "reader": {
                    "Sub": {
                        "type": "record",
                        "name": "Sub",
                        "fields": [{"name": "val", "type": "int"}],
                    }
                },
            },
            {
                "type": "record",
                "name": "Sub",
                "fields": [{"name": "val", "type": "int"}],
            },
        ),
        (
            {"type": "fixed", "name": "Hash", "size": 8},
            "Hash",
            {
                "writer": {},
                "reader": {"Hash": {"type": "fixed", "name": "Hash", "size": 8}},
            },
            {"type": "fixed", "name": "Hash", "size": 8},
        ),
    ],
)
@pytest.mark.parametrize("match_schemas_fn", MATCH_SCHEMAS_FNS)
def test_match_schemas_writer_inline_reader_name_reference(
    match_schemas_fn, writer, reader, named_schemas, expected
):
    assert match_schemas_fn(writer, reader, named_schemas) == expected


@pytest.mark.parametrize(
    "writer,reader,named_schemas",
    [
        (
            {"type": "fixed", "name": "Hash", "size": 4},
            "Hash",
            {
                "writer": {},
                "reader": {"Hash": {"type": "fixed", "name": "Hash", "size": 8}},
            },
        ),
        (
            {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]},
            "UnknownType",
            {
                "writer": {},
                "reader": {
                    "Color": {
                        "type": "enum",
                        "name": "Color",
                        "symbols": ["RED", "GREEN"],
                    }
                },
            },
        ),
    ],
)
@pytest.mark.parametrize("match_schemas_fn", MATCH_SCHEMAS_FNS)
def test_match_schemas_writer_inline_reader_name_reference_raises(
    match_schemas_fn, writer, reader, named_schemas
):
    with pytest.raises(SchemaResolutionError):
        match_schemas_fn(writer, reader, named_schemas)
