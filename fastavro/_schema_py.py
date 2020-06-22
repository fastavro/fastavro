# cython: auto_cpdef=True
import math
from os import path
from copy import deepcopy
import json

from ._schema_common import (
    PRIMITIVES, UnknownType, SchemaParseException, RESERVED_PROPERTIES,
    OPTIONAL_FIELD_PROPERTIES, RESERVED_FIELD_PROPERTIES,
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


def fullname(schema):
    """Returns the fullname of a schema

    Parameters
    ----------
    schema: dict
        Input schema


    Example::

        from fastavro.schema import fullname

        schema = {
            'doc': 'A weather reading.',
            'name': 'Weather',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'station', 'type': 'string'},
                {'name': 'time', 'type': 'long'},
                {'name': 'temp', 'type': 'int'},
            ],
        }

        fname = fullname(schema)
        assert fname == "test.Weather"
    """
    return schema_name(schema, "")[1]


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


def expand_schema(schema):
    """Returns a schema where all named types are expanded to their real schema

    NOTE: The output of this function produces a schema that can include
    multiple definitions of the same named type (as per design) which are not
    valid per the avro specification. Therefore, the output of this should not
    be passed to the normal `writer`/`reader` functions as it will likely
    result in an error.

    Parameters
    ----------
    schema: dict
        Input schema


    Example::

        from fastavro.schema import expand_schema

        original_schema = {
            "name": "MasterSchema",
            "namespace": "com.namespace.master",
            "type": "record",
            "fields": [{
                "name": "field_1",
                "type": {
                    "name": "Dependency",
                    "namespace": "com.namespace.dependencies",
                    "type": "record",
                    "fields": [
                        {"name": "sub_field_1", "type": "string"}
                    ]
                }
            }, {
                "name": "field_2",
                "type": "com.namespace.dependencies.Dependency"
            }]
        }

        expanded_schema = expand_schema(original_schema)

        assert expanded_schema == {
            "name": "com.namespace.master.MasterSchema",
            "type": "record",
            "fields": [{
                "name": "field_1",
                "type": {
                    "name": "com.namespace.dependencies.Dependency",
                    "type": "record",
                    "fields": [
                        {"name": "sub_field_1", "type": "string"}
                    ]
                }
            }, {
                "name": "field_2",
                "type": {
                    "name": "com.namespace.dependencies.Dependency",
                    "type": "record",
                    "fields": [
                        {"name": "sub_field_1", "type": "string"}
                    ]
                }
            }]
        }
    """
    return parse_schema(schema, expand=True, _write_hint=False)


def parse_schema(
    schema, expand=False, _write_hint=True, _force=False, _named_schemas=None,
):
    """Returns a parsed avro schema

    It is not necessary to call parse_schema but doing so and saving the parsed
    schema for use later will make future operations faster as the schema will
    not need to be reparsed.

    Parameters
    ----------
    schema: dict
        Input schema
    expand: bool
        NOTE: This option should be considered a keyword only argument and may
        get enforced as such when Python 2 support is dropped.

        If true, named schemas will be fully expanded to their true schemas
        rather than being represented as just the name. This format should be
        considered an output only and not passed in to other reader/writer
        functions as it does not conform to the avro specification and will
        likely cause an exception
    _write_hint: bool
        Internal API argument specifying whether or not the __fastavro_parsed
        marker should be added to the schema
    _force: bool
        Internal API argument. If True, the schema will always be parsed even
        if it has been parsed and has the __fastavro_parsed marker
    _named_schemas: dict
        Internal API argument. Dictionary of named schemas to their schema
        definition


    Example::

        from fastavro import parse_schema
        from fastavro import writer

        parsed_schema = parse_schema(original_schema)
        with open('weather.avro', 'wb') as out:
            writer(out, parsed_schema, records)
    """
    if _named_schemas is None:
        _named_schemas = {}

    if _force or expand:
        return _parse_schema(
            schema, "", expand, _write_hint, set(), _named_schemas
        )
    elif isinstance(schema, dict) and "__fastavro_parsed" in schema:
        for key, value in schema["__named_schemas"].items():
            _named_schemas[key] = value
        return schema
    elif isinstance(schema, list):
        # If we are given a list we should make sure that the immediate sub
        # schemas have the hint in them
        return [
            parse_schema(s, expand, _write_hint, _force, _named_schemas)
            for s in schema
        ]
    else:
        return _parse_schema(
            schema, "", expand, _write_hint, set(), _named_schemas
        )


def _parse_schema(
    schema, namespace, expand, _write_hint, names, named_schemas
):
    # union schemas
    if isinstance(schema, list):
        return [
            _parse_schema(
                s, namespace, expand, False, names, named_schemas
            ) for s in schema
        ]

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(schema, dict):
        if schema in PRIMITIVES:
            return schema

        if '.' not in schema and namespace:
            schema = namespace + '.' + schema

        if schema not in named_schemas:
            raise UnknownType(schema)
        elif expand:
            # If `name` is in the schema, it has been fully resolved and so we
            # can include the full schema. If `name` is not in the schema yet,
            # then we are still recursing that schema and must use the named
            # schema or else we will have infinite recursion when printing the
            # final schema
            if "name" in named_schemas[schema]:
                return named_schemas[schema]
            else:
                return schema
        else:
            return schema

    else:
        # Remaining valid schemas must be dict types
        schema_type = schema["type"]

        parsed_schema = {
            key: value
            for key, value in schema.items()
            if key not in RESERVED_PROPERTIES
        }
        parsed_schema["type"] = schema_type

        if "doc" in schema:
            parsed_schema["doc"] = schema["doc"]

        # Correctness checks for logical types
        logical_type = parsed_schema.get("logicalType")
        if logical_type == "decimal":
            scale = parsed_schema.get("scale")
            if scale and (not isinstance(scale, int) or scale < 0):
                raise SchemaParseException(
                    "decimal scale must be a postive integer, "
                    + "not {}".format(scale)
                )

            precision = parsed_schema.get("precision")
            if precision:
                if not isinstance(precision, int) or precision <= 0:
                    raise SchemaParseException(
                        "decimal precision must be a postive integer, "
                        + "not {}".format(precision)
                    )
                if schema_type == "fixed":
                    # https://avro.apache.org/docs/current/spec.html#Decimal
                    size = schema["size"]
                    max_precision = int(
                        math.floor(math.log10(2) * (8 * size - 1))
                    )
                    if precision > max_precision:
                        msg = "decimal precision of {} doesn't fit into " \
                              "array of length {}"
                        raise SchemaParseException(
                            msg.format(precision, size)
                        )

            if scale and precision and precision < scale:
                raise SchemaParseException(
                    "decimal scale must be less than or equal to "
                    + "the precision of {}".format(precision)
                )

        if schema_type == "array":
            parsed_schema["items"] = _parse_schema(
                schema["items"],
                namespace,
                expand,
                False,
                names,
                named_schemas,
            )

        elif schema_type == "map":
            parsed_schema["values"] = _parse_schema(
                schema["values"],
                namespace,
                expand,
                False,
                names,
                named_schemas,
            )

        elif schema_type == "enum":
            _, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(
                    "redefined named type: {}".format(fullname)
                )
            names.add(fullname)

            named_schemas[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["symbols"] = schema["symbols"]

        elif schema_type == "fixed":
            _, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(
                    "redefined named type: {}".format(fullname)
                )
            names.add(fullname)

            named_schemas[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["size"] = schema["size"]

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(
                    "redefined named type: {}".format(fullname)
                )
            names.add(fullname)

            named_schemas[fullname] = parsed_schema

            fields = []
            for field in schema.get('fields', []):
                fields.append(
                    parse_field(field, namespace, expand, names, named_schemas)
                )

            parsed_schema["name"] = fullname
            parsed_schema["fields"] = fields

            # Hint that we have parsed the record
            if _write_hint:
                # Make a copy of parsed_schema so that we don't have a cyclical
                # reference. Using deepcopy is pretty slow, and we don't need a
                # true deepcopy so this works good enough
                named_schemas[fullname] = {
                    k: v for k, v in parsed_schema.items()
                }

                parsed_schema["__fastavro_parsed"] = True
                parsed_schema["__named_schemas"] = named_schemas

        elif schema_type in PRIMITIVES:
            parsed_schema["type"] = schema_type

        else:
            raise UnknownType(schema)

        return parsed_schema


def parse_field(field, namespace, expand, names, named_schemas):
    parsed_field = {
        key: value
        for key, value in field.items()
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
        field["type"], namespace, expand, False, names, named_schemas
    )

    return parsed_field


def load_schema(schema_path, _named_schemas=None):
    '''
    Returns a schema loaded from the file at `schema_path`.

    Will recursively load referenced schemas assuming they can be found in
    files in the same directory and named with the convention
    `<type_name>.avsc`.
    '''
    if _named_schemas is None:
        _named_schemas = {}

    with open(schema_path) as fd:
        schema = json.load(fd)
    schema_dir, schema_file = path.split(schema_path)
    return _load_schema(schema, schema_dir, _named_schemas)


def _load_schema(schema, schema_dir, named_schemas):
    try:
        schema_copy = deepcopy(named_schemas)
        return parse_schema(schema, _named_schemas=named_schemas)
    except UnknownType as e:
        try:
            avsc = path.join(schema_dir, '%s.avsc' % e.name)
            sub_schema = load_schema(avsc, schema_copy)
        except IOError:
            raise e

        if isinstance(schema, dict):
            if isinstance(sub_schema, list):
                return _load_schema(
                    sub_schema + [schema], schema_dir, schema_copy
                )
            else:
                return _load_schema(
                    [sub_schema, schema], schema_dir, schema_copy
                )
        else:
            # schema is already a list
            if isinstance(sub_schema, list):
                return _load_schema(
                    sub_schema + schema, schema_dir, schema_copy
                )
            else:
                schema.insert(0, sub_schema)
                return _load_schema(schema, schema_dir, schema_copy)
