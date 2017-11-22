from ._schema_public import UnknownType  # noqa: F401

try:
    from ._schema import acquaint_schema, load_schema  # noqa: F401
except ImportError:
    from ._schema_py import acquaint_schema, load_schema  # noqa: F401
