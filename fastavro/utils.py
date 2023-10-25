from hashlib import md5
import random
from string import ascii_letters
from typing import Any, Iterator, Dict, List, cast

from .const import INT_MIN_VALUE, INT_MAX_VALUE, LONG_MIN_VALUE, LONG_MAX_VALUE
from .schema import extract_record_type, parse_schema
from .types import (
    AnySchema,
    Array,
    Enum,
    Field,
    Fixed,
    Map,
    PrimitiveDict,
    Record,
    Schema,
    NamedSchemas,
)
from ._schema_common import PRIMITIVES


def _randbytes(num: int) -> bytes:
    # TODO: Use random.randbytes when this library is Python 3.9+ only
    return random.getrandbits(num * 8).to_bytes(num, "little")


def _md5(string: str) -> str:
    return md5(string.encode()).hexdigest()


def _gen_utf8() -> str:
    return "".join(random.choices(ascii_letters, k=10))


def gen_data(schema: Schema, named_schemas: NamedSchemas, index: int) -> Any:
    record_type = extract_record_type(schema)

    if record_type == "null":
        return None
    elif record_type == "string":
        return _gen_utf8()
    elif record_type == "int":
        return random.randint(INT_MIN_VALUE, INT_MAX_VALUE)
    elif record_type == "long":
        return random.randint(LONG_MIN_VALUE, LONG_MAX_VALUE)
    elif record_type == "float":
        return random.random()
    elif record_type == "double":
        return random.random()
    elif record_type == "boolean":
        return index % 2 == 0
    elif record_type == "bytes":
        return _randbytes(10)
    elif record_type == "fixed":
        fixed_schema = cast(Dict[str, Any], schema)
        return _randbytes(fixed_schema["size"])
    elif record_type == "enum":
        enum_schema = cast(Dict[str, Any], schema)
        real_index = index % len(enum_schema["symbols"])
        return enum_schema["symbols"][real_index]
    elif record_type == "array":
        array_schema = cast(Dict[str, Schema], schema)
        return [
            gen_data(array_schema["items"], named_schemas, index) for _ in range(10)
        ]
    elif record_type == "map":
        map_schema = cast(Dict[str, Schema], schema)
        return {
            _gen_utf8(): gen_data(map_schema["values"], named_schemas, index)
            for _ in range(10)
        }
    elif record_type == "union" or record_type == "error_union":
        union_schema = cast(List[Schema], schema)
        real_index = index % len(union_schema)
        return gen_data(union_schema[real_index], named_schemas, index)
    elif record_type == "record" or record_type == "error":
        record_schema = cast(Dict[str, Any], schema)
        return {
            field["name"]: gen_data(field["type"], named_schemas, index)
            for field in record_schema["fields"]
        }
    else:
        named_schema = cast(str, schema)
        return gen_data(named_schemas[named_schema], named_schemas, index)


def generate_one(schema: Schema) -> Any:
    """
    Returns a single instance of arbitrary data that conforms to the schema.

    Parameters
    ----------
    schema
        Schema that data should conform to


    Example::

        from fastavro import schemaless_writer
        from fastavro.utils import generate_one

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

        with open('weather.avro', 'wb') as out:
            schemaless_writer(out, schema, generate_one(schema))
    """
    return next(generate_many(schema, 1))


def generate_many(schema: Schema, count: int) -> Iterator[Any]:
    """
    A generator that yields arbitrary data that conforms to the schema. It will
    yield a number of data structures equal to what is given in the count

    Parameters
    ----------
    schema
        Schema that data should conform to
    count
        Number of objects to generate


    Example::

        from fastavro import writer
        from fastavro.utils import generate_many

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

        with open('weather.avro', 'wb') as out:
            writer(out, schema, generate_many(schema, 5))
    """
    named_schemas: NamedSchemas = {}
    parsed_schema = parse_schema(schema, named_schemas)
    for index in range(count):
        yield gen_data(parsed_schema, named_schemas, index)


def anonymize_schema(schema: Schema) -> Schema:
    """Returns an anonymized schema

    Parameters
    ----------
    schema
        Schema to anonymize


    Example::

        from fastavro.utils import anonymize_schema

        anonymized_schema = anonymize_schema(original_schema)
    """
    named_schemas: NamedSchemas = {}
    parsed_schema = parse_schema(schema, named_schemas)
    return _anonymize_schema(parsed_schema, named_schemas)


def _anonymize_schema(schema: Schema, named_schemas: NamedSchemas) -> Schema:
    # union schemas
    if isinstance(schema, list):
        return [cast(AnySchema, _anonymize_schema(s, named_schemas)) for s in schema]

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(schema, dict):
        if schema in PRIMITIVES:
            return schema
        else:
            return f"A_{_md5(schema)}"

    else:
        # Remaining valid schemas must be dict types
        if schema["type"] == "array":
            array_schema: Array = {
                "type": schema["type"],
                "items": _anonymize_schema(schema["items"], named_schemas),
            }
            return array_schema

        elif schema["type"] == "map":
            map_schema: Map = {
                "type": schema["type"],
                "values": _anonymize_schema(schema["values"], named_schemas),
            }
            return map_schema

        elif schema["type"] == "enum":
            enum_schema: Enum = {
                "type": schema["type"],
                "name": f"A_{_md5(schema['name'])}",
                "symbols": [f"A_{_md5(symbol)}" for symbol in schema["symbols"]],
            }
            if "doc" in schema:
                enum_schema["doc"] = _md5(schema["doc"])
            return enum_schema

        elif schema["type"] == "fixed":
            fixed_schema: Fixed = {
                "type": schema["type"],
                "name": f"A_{_md5(schema['name'])}",
                "size": schema["size"],
            }
            return fixed_schema

        elif schema["type"] == "record" or schema["type"] == "error":
            record_schema: Record = {
                "type": schema["type"],
                "name": f"A_{_md5(schema['name'])}",
                "fields": [
                    anonymize_field(field, named_schemas)
                    for field in schema.get("fields", [])
                ],
            }
            if "doc" in schema:
                record_schema["doc"] = _md5(schema["doc"])
            return record_schema

        elif schema["type"] in PRIMITIVES:
            parsed_schema: PrimitiveDict = {"type": schema["type"]}
            return parsed_schema

        else:
            raise Exception(f"Unhandled schema: {schema}")


def anonymize_field(field: Field, named_schemas: NamedSchemas) -> Field:
    parsed_field: Field = {
        "name": _md5(field["name"]),
        "type": _anonymize_schema(field["type"], named_schemas),
    }

    if "doc" in field:
        parsed_field["doc"] = _md5(field["doc"])
    if "aliases" in field:
        parsed_field["aliases"] = [_md5(alias) for alias in field["aliases"]]
    if "default" in field:
        parsed_field["default"] = field["default"]

    # TODO: Defaults for enums should be hashed. Maybe others too?
    return parsed_field
