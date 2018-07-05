# cython: auto_cpdef=True

from os import path

import json

from ._six import iteritems
from ._schema_common import (
    PRIMITIVES, UnknownType, SchemaParseException, RESERVED_PROPERTIES,
    SCHEMA_DEFS,
)


cpdef inline extract_record_type(schema):
    if isinstance(schema, dict):
        return schema['type']

    if isinstance(schema, list):
        return 'union'

    return schema


cpdef inline str extract_logical_type(schema):
    if not isinstance(schema, dict):
        return None
    rt = schema['type']
    lt = schema.get('logicalType')
    if lt:
        # TODO: Building this string every time is going to be relatively slow.
        return '{}-{}'.format(rt, lt)
    return None


def schema_name(schema, parent_ns):
    try:
        name = schema['name']
    except KeyError:
        msg = (
            '"name" is a required field missing from ' +
            'the schema: {}'.format(schema)
        )
        raise SchemaParseException(msg)

    namespace = schema.get('namespace', parent_ns)
    if not namespace:
        return namespace, name

    return namespace, '{}.{}'.format(namespace, name)


def parse_schema(schema, _write_hint=True):
    if isinstance(schema, dict) and "__fastavro_parsed" in schema:
        return schema
    else:
        return _parse_schema(schema, "", _write_hint)


cdef _parse_schema(schema, namespace, _write_hint):
    # union schemas
    if isinstance(schema, list):
        return [_parse_schema(s, namespace, _write_hint) for s in schema]

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

        if schema_type == "array":
            parsed_schema["items"] = _parse_schema(
                schema["items"],
                namespace,
                _write_hint
            )

        elif schema_type == "map":
            parsed_schema["values"] = _parse_schema(
                schema["values"],
                namespace,
                _write_hint
            )

        elif schema_type == "enum":
            _, fullname = schema_name(schema, namespace)
            SCHEMA_DEFS[fullname] = schema

            parsed_schema["name"] = fullname
            parsed_schema["symbols"] = schema["symbols"]

        elif schema_type == "fixed":
            _, fullname = schema_name(schema, namespace)
            SCHEMA_DEFS[fullname] = schema

            parsed_schema["name"] = fullname
            parsed_schema["size"] = schema["size"]

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, fullname = schema_name(schema, namespace)
            SCHEMA_DEFS[fullname] = schema

            fields = []
            for field in schema.get('fields', []):
                fields.append(
                    parse_field(field, namespace, _write_hint)
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


def parse_field(field, namespace, _write_hint):
    parsed_field = {
        "name": field["name"],
    }

    if "default" in field:
        parsed_field["default"] = field["default"]

    parsed_field["type"] = _parse_schema(field["type"], namespace, _write_hint)

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
            schema.insert(sub_schema, 0)
            return _load_schema(schema, schema_dir)
