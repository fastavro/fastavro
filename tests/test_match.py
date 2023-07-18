import pytest

try:
    from fastavro._read import match_types, match_schemas
except ImportError:
    from fastavro._read_py import match_types, match_schemas  # type: ignore

from fastavro._read_common import SchemaResolutionError


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
