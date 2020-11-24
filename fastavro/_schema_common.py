PRIMITIVES = {
    "boolean",
    "bytes",
    "double",
    "float",
    "int",
    "long",
    "null",
    "string",
}

RESERVED_PROPERTIES = {
    "type",
    "name",
    "namespace",
    "fields",  # Record
    "items",  # Array
    "size",  # Fixed
    "symbols",  # Enum
    "values",  # Map
    "doc",
}

OPTIONAL_FIELD_PROPERTIES = {
    "doc",
    "aliases",
    "default",
}

RESERVED_FIELD_PROPERTIES = {"type", "name"} | OPTIONAL_FIELD_PROPERTIES


class UnknownType(ValueError):
    def __init__(self, name):
        super(UnknownType, self).__init__(name)
        self.name = name


class SchemaParseException(Exception):
    pass
