# cython: auto_cpdef=True

from os import path

import json

from .six import iteritems
from ._schema_common import (
    PRIMITIVES, UnknownType, SchemaParseException, RESERVED_PROPERTIES,
    SCHEMA_DEFS, OPTIONAL_FIELD_PROPERTIES, RESERVED_FIELD_PROPERTIES,
)


def extract_record_type(schema):
    if isinstance(schema, dict):
        return schema['type']

    if isinstance(schema, list):
        return 'union'

    return schema


def extract_logical_type(schema):
    if not isinstance(schema, dict):
        return None
    d_schema = schema
    rt = d_schema['type']
    lt = d_schema.get('logicalType')
    if lt:
        # TODO: Building this string every time is going to be relatively slow.
        return '{}-{}'.format(rt, lt)
    return None


def schema_name(schema, parent_ns):
    try:
        name = schema['name']
    except KeyError:
        msg = (
            '"name" is a required field missing from '
            + 'the schema: {}'.format(schema)
        )
        raise SchemaParseException(msg)

    namespace = schema.get('namespace', parent_ns)
    if not namespace:
        return namespace, name

    return namespace, '{}.{}'.format(namespace, name)


def parse_schema(schema, _write_hint=True, _force=False):
    """Returns a parsed avro schema

    It is not necessary to call parse_schema but doing so and saving the parsed
    schema for use later will make future operations faster as the schema will
    not need to be reparsed.

    Parameters
    ----------
    schema: dict
        Input schema
    _write_hint: bool
        Internal API argument specifying whether or not the __fastavro_parsed
        marker should be added to the schema
    _force: bool
        Internal API argument. If True, the schema will always be parsed even
        if it has been parsed and has the __fastavro_parsed marker


    Example::

        from fastavro import parse_schema
        from fastavro import writer

        parsed_schema = parse_schema(original_schema)
        with open('weather.avro', 'wb') as out:
            writer(out, parsed_schema, records)
    """
    if _force:
        return _parse_schema(schema, "", _write_hint, set())
    elif isinstance(schema, dict) and "__fastavro_parsed" in schema:
        return schema
    else:
        return _parse_schema(schema, "", _write_hint, set())


def _parse_schema(schema, namespace, _write_hint, named_schemas):
    # union schemas
    if isinstance(schema, list):
        return [
            _parse_schema(
                s, namespace, False, named_schemas
            ) for s in schema
        ]

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(schema, dict):
        if schema in PRIMITIVES:
            return schema

        if '.' not in schema and namespace:
            schema = namespace + '.' + schema

        if schema not in SCHEMA_DEFS:
            raise UnknownType(schema)
        else:
            return schema

    else:
        # Remaining valid schemas must be dict types
        schema_type = schema["type"]

        parsed_schema = {
            key: value
            for key, value in iteritems(schema)
            if key not in RESERVED_PROPERTIES
        }
        parsed_schema["type"] = schema_type

        # Correctness checks for logical types
        logical_type = parsed_schema.get("logicalType")
        if logical_type == "decimal":
            scale = parsed_schema.get("scale")
            if scale and not isinstance(scale, int):
                raise SchemaParseException(
                    "decimal scale must be a postive integer, "
                    + "not {}".format(scale)
                )
            precision = parsed_schema.get("precision")
            if precision and not isinstance(precision, int):
                raise SchemaParseException(
                    "decimal precision must be a postive integer, "
                    + "not {}".format(precision)
                )

        if schema_type == "array":
            parsed_schema["items"] = _parse_schema(
                schema["items"],
                namespace,
                False,
                named_schemas,
            )

        elif schema_type == "map":
            parsed_schema["values"] = _parse_schema(
                schema["values"],
                namespace,
                False,
                named_schemas,
            )

        elif schema_type == "enum":
            _, fullname = schema_name(schema, namespace)
            if fullname in named_schemas:
                raise SchemaParseException(
                    "redefined named type: {}".format(fullname)
                )
            named_schemas.add(fullname)

            SCHEMA_DEFS[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["symbols"] = schema["symbols"]

        elif schema_type == "fixed":
            _, fullname = schema_name(schema, namespace)
            if fullname in named_schemas:
                raise SchemaParseException(
                    "redefined named type: {}".format(fullname)
                )
            named_schemas.add(fullname)

            SCHEMA_DEFS[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["size"] = schema["size"]

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, fullname = schema_name(schema, namespace)
            if fullname in named_schemas:
                raise SchemaParseException(
                    "redefined named type: {}".format(fullname)
                )
            named_schemas.add(fullname)

            SCHEMA_DEFS[fullname] = parsed_schema

            fields = []
            for field in schema.get('fields', []):
                fields.append(
                    parse_field(field, namespace, named_schemas)
                )

            parsed_schema["name"] = fullname
            parsed_schema["fields"] = fields

            # Hint that we have parsed the record
            if _write_hint:
                parsed_schema["__fastavro_parsed"] = True

        elif schema_type in PRIMITIVES:
            parsed_schema["type"] = schema_type

        else:
            raise UnknownType(schema)

        return parsed_schema


def parse_field(field, namespace, named_schemas):
    parsed_field = {
        key: value
        for key, value in iteritems(field)
        if key not in RESERVED_FIELD_PROPERTIES
    }

    for prop in OPTIONAL_FIELD_PROPERTIES:
        if prop in field:
            parsed_field[prop] = field[prop]

    # Aliases must be a list
    aliases = parsed_field.get("aliases", [])
    if not isinstance(aliases, list):
        msg = "aliases must be a list, not {}".format(aliases)
        raise SchemaParseException(msg)

    parsed_field["name"] = field["name"]
    parsed_field["type"] = _parse_schema(
        field["type"], namespace, False, named_schemas
    )

    return parsed_field


def load_schema(schema_path):
    '''
    Returns a schema loaded from the file at `schema_path`.

    Will recursively load referenced schemas assuming they can be found in
    files in the same directory and named with the convention
    `<type_name>.avsc`.
    '''
    with open(schema_path) as fd:
        schema = json.load(fd)
    schema_dir, schema_file = path.split(schema_path)
    return _load_schema(schema, schema_dir)


def _load_schema(schema, schema_dir):
    try:
        return parse_schema(schema)
    except UnknownType as e:
        try:
            avsc = path.join(schema_dir, '%s.avsc' % e.name)
            sub_schema = load_schema(avsc)
        except IOError:
            raise e

        if isinstance(schema, dict):
            return _load_schema([sub_schema, schema], schema_dir)
        else:
            # schema is already a list
            schema.insert(0, sub_schema)
            return _load_schema(schema, schema_dir)
