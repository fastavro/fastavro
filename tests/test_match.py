import pytest

try:
    from fastavro._read import match_types, match_schemas
except ImportError:
    from fastavro._read_py import match_types, match_schemas  # type: ignore

from fastavro._read_common import SchemaResolutionError


@pytest.mark.parametrize(
    "left,right",
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
        ("int", "string"),
    ],
)
def test_match_types_returns_true(left, right):
    assert match_types(left, right)


@pytest.mark.parametrize(
    "left,right",
    [
        ("int", "string"),
        ("long", "int"),
        ("float", "long"),
        ("string", "int"),
        ("bytes", "int"),
        ({"type": "int"}, {"type": "string"}),
    ],
)
def test_match_types_returns_false(left, right):
    assert not match_types(left, right)


@pytest.mark.parametrize(
    "left,right",
    [
        ({"type": "int"}, {"type": "int"}),
    ],
)
def test_match_schemas_returns_right_schema(left, right):
    assert right == match_schemas(left, right)


@pytest.mark.parametrize(
    "left,right",
    [
        ({"type": "int"}, {"type": "string"}),
    ],
)
def test_match_schemas_raises_exception(left, right):
    with pytest.raises(SchemaResolutionError) as err:
        match_schemas(left, right)
    assert str(err.value) == "Some"
