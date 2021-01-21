# cython: language_level=3
# cython: auto_cpdef=True

from os import path
from copy import deepcopy
import json
from libc.math cimport floor, log10

from ._schema_common import (
    PRIMITIVES,
    UnknownType,
    SchemaParseException,
    RESERVED_PROPERTIES,
    OPTIONAL_FIELD_PROPERTIES,
    RESERVED_FIELD_PROPERTIES,
)


cpdef inline extract_record_type(schema):
    if isinstance(schema, dict):
        return schema["type"]

    if isinstance(schema, list):
        return "union"

    return schema


cpdef inline str extract_logical_type(schema):
    if not isinstance(schema, dict):
        return None
    rt = schema["type"]
    lt = schema.get("logicalType")
    if lt:
        # TODO: Building this string every time is going to be relatively slow.
        return f"{rt}-{lt}"
    return None


cpdef fullname(schema):
    return schema_name(schema, "")[1]


cpdef schema_name(schema, parent_ns):
    try:
        name = schema["name"]
    except KeyError:
        raise SchemaParseException(
            f'"name" is a required field missing from the schema: {schema}'
        )

    namespace = schema.get("namespace", parent_ns)
    if not namespace:
        return namespace, name
    elif "." in name:
        return "", name
    else:
        return namespace, f"{namespace}.{name}"


cpdef expand_schema(schema):
    return parse_schema(schema, expand=True, _write_hint=False)


def parse_schema(
    schema, expand=False, _write_hint=True, _force=False, _named_schemas=None
):
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


cdef _parse_schema(schema, namespace, expand, _write_hint, names, named_schemas):
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

        if "." not in schema and namespace:
            schema = namespace + "." + schema

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
                    f"decimal scale must be a postive integer, not {scale}"
                )

            precision = parsed_schema.get("precision")
            if precision:
                if not isinstance(precision, int) or precision <= 0:
                    raise SchemaParseException(
                        "decimal precision must be a postive integer, "
                        + f"not {precision}"
                    )
                if schema_type == "fixed":
                    # https://avro.apache.org/docs/current/spec.html#Decimal
                    size = schema["size"]
                    max_precision = int(floor(log10(2) * (8 * size - 1)))
                    if precision > max_precision:
                        raise SchemaParseException(
                            f"decimal precision of {precision} doesn't fit "
                            + f"into array of length {size}"
                        )

            if scale and precision and precision < scale:
                raise SchemaParseException(
                    "decimal scale must be less than or equal to "
                    + f"the precision of {precision}"
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
                raise SchemaParseException(f"redefined named type: {fullname}")
            names.add(fullname)

            named_schemas[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["symbols"] = schema["symbols"]

        elif schema_type == "fixed":
            _, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(f"redefined named type: {fullname}")
            names.add(fullname)

            named_schemas[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["size"] = schema["size"]

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(f"redefined named type: {fullname}")
            names.add(fullname)

            named_schemas[fullname] = parsed_schema

            fields = []
            for field in schema.get("fields", []):
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


cdef parse_field(field, namespace, expand, names, named_schemas):
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
        msg = f"aliases must be a list, not {aliases}"
        raise SchemaParseException(msg)

    parsed_field["name"] = field["name"]
    parsed_field["type"] = _parse_schema(
        field["type"], namespace, expand, False, names, named_schemas
    )

    return parsed_field


def load_schema(
    schema_path, *, _named_schemas=None, _write_hint=True, _injected_schemas=None
):
    if _named_schemas is None:
        _named_schemas = {}

    if _injected_schemas is None:
        _injected_schemas = set()

    with open(schema_path) as fd:
        schema = json.load(fd)
    schema_dir, schema_file = path.split(schema_path)
    return _load_schema(
        schema, schema_dir, _named_schemas, _write_hint, _injected_schemas
    )


cdef _load_schema(schema, schema_dir, named_schemas, write_hint, injected_schemas):
    try:
        schema_copy = deepcopy(named_schemas)
        return parse_schema(
            schema, _named_schemas=named_schemas, _write_hint=write_hint
        )
    except UnknownType as e:
        try:
            avsc = path.join(schema_dir, f"{e.name}.avsc")
            sub_schema = load_schema(
                avsc,
                _named_schemas=schema_copy,
                _write_hint=False,
                _injected_schemas=injected_schemas,
            )
        except IOError:
            raise e

        if sub_schema["name"] not in injected_schemas:
            _inject_schema(schema, sub_schema)
            injected_schemas.add(sub_schema["name"])
        return _load_schema(
            schema, schema_dir, schema_copy, write_hint, injected_schemas
        )


cdef _inject_schema(outer_schema, inner_schema, namespace="", is_injected=False):
    # Once injected, we can stop checking to see if we need to inject since it
    # should only be done once at most
    if is_injected is True:
        return outer_schema, is_injected

    # union schemas
    if isinstance(outer_schema, list):
        union = []
        for each_schema in outer_schema:
            if is_injected:
                union.append(each_schema)
            else:
                return_schema, injected = _inject_schema(
                    each_schema, inner_schema, namespace, is_injected
                )
                union.append(return_schema)
                if injected is True:
                    is_injected = injected
        return union, is_injected

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(outer_schema, dict):
        if outer_schema in PRIMITIVES:
            return outer_schema, is_injected

        if "." not in outer_schema and namespace:
            outer_schema = namespace + "." + outer_schema

        if outer_schema == inner_schema["name"]:
            return inner_schema, True
        else:
            # Hit a named schema that has already been loaded previously. Return
            # the outer_schema so we keep looking
            return outer_schema, is_injected
    else:
        # Remaining valid schemas must be dict types
        schema_type = outer_schema["type"]

        if schema_type == "array":
            return_schema, injected = _inject_schema(
                outer_schema["items"], inner_schema, namespace, is_injected
            )
            outer_schema["items"] = return_schema
            return outer_schema, injected

        elif schema_type == "map":
            return_schema, injected = _inject_schema(
                outer_schema["values"], inner_schema, namespace, is_injected
            )
            outer_schema["values"] = return_schema
            return outer_schema, injected

        elif schema_type == "enum":
            return outer_schema, is_injected

        elif schema_type == "fixed":
            return outer_schema, is_injected

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, _ = schema_name(outer_schema, namespace)
            fields = []
            for field in outer_schema.get("fields", []):
                if is_injected:
                    fields.append(field)
                else:
                    return_schema, injected = _inject_schema(
                        field["type"], inner_schema, namespace, is_injected
                    )
                    field["type"] = return_schema
                    fields.append(field)

                    if injected is True:
                        is_injected = injected
            if fields:
                outer_schema["fields"] = fields

            return outer_schema, is_injected

        elif schema_type in PRIMITIVES:
            return outer_schema, is_injected

        else:
            raise Exception(
                "Internal error; "
                + "You should raise an issue in the fastavro github repository"
            )


def load_schema_ordered(ordered_schemas, *, _write_hint=True):
    loaded_schemas = []
    _named_schemas = {}
    for idx, schema_path in enumerate(ordered_schemas):
        # _write_hint is always False except maybe the outer most schema
        _last = _write_hint if idx + 1 == len(ordered_schemas) else False
        schema = load_schema(
            schema_path, _named_schemas=_named_schemas, _write_hint=_last
        )
        loaded_schemas.append(schema)

    top_first_order = loaded_schemas[::-1]
    outer_schema = top_first_order.pop(0)

    while top_first_order:
        sub_schema = top_first_order.pop(0)
        _inject_schema(outer_schema, sub_schema)

    return outer_schema
