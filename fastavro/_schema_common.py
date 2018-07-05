PRIMITIVES = {
    'boolean',
    'bytes',
    'double',
    'float',
    'int',
    'long',
    'null',
    'string',
}

# A mapping of named schemas to their actual schema definition
SCHEMA_DEFS = {}

RESERVED_PROPERTIES = {
    'type',
    'name',
    'namespace',
    'fields',  # Record
    'items',  # Array
    'size',  # Fixed
    'symbols',  # Enum
    'values',  # Map
    'doc',
}


class UnknownType(ValueError):
    def __init__(self, name):
        super(UnknownType, self).__init__(name)
        self.name = name


class SchemaParseException(Exception):
    pass
