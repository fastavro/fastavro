import copy
import dataclasses
import hashlib
import json
from typing import Any, Callable, List

from ._schema_common import PRIMITIVES

TYPES_WITH_ATTRIBUTES = {"record", "array", "map", "enum", "fixed"}
CANONICAL_FIELDS_ORDER = [
    "name",
    "type",
    "fields",
    "symbols",
    "items",
    "values",
    "size",
]


def hash_schema(schema: Any) -> str:
    """
    Dumps to json without whitespace and calculate
    md5 hexdigest.
    """

    return hashlib.md5(json.dumps(schema, separators=(",", ":")).encode()).hexdigest()


def depth_first_walk_schema(
    schema, cb_before: Callable[[Any], Any], cb_after: Callable[[Any], Any]
) -> Any:
    """
    Walks the schema, calling cb_before on each node before recursing and cb_after after
    recursing. The return on the callback is used in the recursion (for cb_before)
    and stored in the place of the original (for cb_after).

    We need to recurse for anything inside schema that represents
    a type of some kind. This includes:
        - logicalType
        - list
        - record, array, map
        - any dict with "type" that is a type by its own right
    Effectively we recurse on anything that is allowed except:
        - enum
        - fixed
        - schema is a string
    """

    def recurse(s):
        return depth_first_walk_schema(s, cb_before, cb_after)

    schema = cb_before(copy.copy(schema))
    if isinstance(schema, dict):
        if "type" in schema:
            if "logicalType" in schema:
                # need to check it is well-formed. Can't be
                # record, array, map, enum or fixed (as there require further attributes)
                if (
                    isinstance(schema["type"], str)
                    and schema["type"] in TYPES_WITH_ATTRIBUTES
                ):
                    raise ValueError(
                        f"Logical type {schema['logicalType']} "
                        "cannot be applied to "
                        f"{schema['type']} on same level, need sub-dict."
                    )
                schema["type"] = recurse(schema["type"])
            else:
                if isinstance(schema["type"], str):
                    if schema["type"] == "record":
                        new_fields = []
                        for field in schema["fields"]:
                            field = copy.copy(field)
                            field["type"] = recurse(field["type"])
                            new_fields.append(field)
                        schema["fields"] = new_fields
                    elif schema["type"] == "array":
                        schema["items"] = recurse(schema["items"])
                    elif schema["type"] == "map":
                        schema["values"] = recurse(schema["values"])
                    elif schema["type"] in {"enum", "fixed"}:
                        # does not need recursion; dict-schema is the type
                        pass
                    else:
                        # for others, the "str" reprsents type, so we recurse
                        schema["type"] = recurse(schema["type"])
                elif isinstance(schema["type"], (list, dict)):
                    # also needs recursion as represent type
                    schema["type"] = recurse(schema["type"])
                else:
                    raise ValueError(f"Unknown schema type {type(schema['type'])}")
        else:
            raise ValueError("Schema dict does not have a 'type' key.")
    elif isinstance(schema, list):  # union type
        schema = [recurse(s) for s in schema]
    elif isinstance(schema, str):
        pass
    else:
        raise ValueError(f"Unknown schema type {type(schema)}")
    schema = cb_after(schema)
    return schema


def create_fullname(ns: str, name: str) -> str:
    if ns == "":
        return name
    elif name != "":
        return f"{ns}.{name}"
    else:
        raise ValueError("Namespace and name can't both be null.")


def simplify(schema: Any) -> Any:
    """Simplify a schema if it is just a dict with a type keyword
    and nothing else."""
    if isinstance(schema, dict) and len(schema) == 1 and "type" in schema:
        return schema["type"]
    else:
        return schema


def ensure_integer_fixed(schema: Any) -> Any:
    if isinstance(schema, dict) and schema.get("type") == "fixed":
        schema["size"] = int(schema["size"])
    return schema


def sort_attr_logicalType(keys: List[str]) -> Any:
    keys = copy.copy(keys)
    keys.remove("logicalType")
    keys.remove("type")
    keys.sort()
    return ["type", "logicalType"] + keys


def remove_unicode_escapes(schema: Any) -> Any:
    if isinstance(schema, dict):
        return {key: remove_unicode_escapes(value) for key, value in schema.items()}
    elif isinstance(schema, str):
        return schema.encode("utf-8").decode()
    else:
        return schema


@dataclasses.dataclass
class RemoveNonCanonicalAndSortAttributes:
    keep_logicalType: bool

    def before(self, schema: Any) -> Any:
        if isinstance(schema, dict):
            if "logicalType" in schema:
                if self.keep_logicalType:
                    return {
                        key: schema[key]
                        for key in sort_attr_logicalType(list(schema.keys()))
                    }
                else:
                    return {"type": schema["type"]}
            else:
                return {
                    key: schema[key] for key in CANONICAL_FIELDS_ORDER if key in schema
                }
        else:
            return schema


@dataclasses.dataclass
class ResolveFullname:
    ns_stack: List[str] = dataclasses.field(default_factory=list)

    def before(self, schema: Any) -> Any:
        """
        First we put the namespace on the stack, then we resolve the fullname.
        """
        # named items are "record", "enum" and "fixed"; here we resolve the
        # "name" attribute and set the "namespace"
        prev_ns = self.ns_stack[-1] if len(self.ns_stack) > 0 else ""
        if (
            isinstance(schema, dict)
            and "type" in schema
            and isinstance(schema["type"], str)
            and schema["type"] in ("record", "enum", "fixed")
        ):
            ns_from_name, _, name = schema["name"].rpartition(".")
            if ns_from_name == "":
                # no namespace used in name
                if "namespace" in schema:
                    ns = schema["namespace"]
                else:
                    ns = prev_ns
                schema["name"] = create_fullname(ns, name)
            else:
                ns = ns_from_name
            self.ns_stack.append(ns)
        else:
            self.ns_stack.append(prev_ns)
            if isinstance(schema, str) and schema not in PRIMITIVES:
                ns_from_name, _, name = schema.rpartition(".")
                if ns_from_name == "":
                    schema = create_fullname(prev_ns, name)
                else:
                    # already a fully resolved name
                    pass
        return schema

    def after(self, schema: Any) -> Any:
        self.ns_stack.pop()
        return schema


@dataclasses.dataclass
class Decomposer:
    schemas_dict: dict = dataclasses.field(default_factory=dict)
    referenced_schema_names: set[str] = dataclasses.field(default_factory=set)
    named_schemas: set[str] = dataclasses.field(default_factory=set)
    hashed_schema_names: set[str] = dataclasses.field(default_factory=set)
    missing_schema_names: set[str] = dataclasses.field(default_factory=set)

    def before(self, schema: Any) -> Any:
        if isinstance(schema, dict) and "name" in schema:
            self.named_schemas.add(schema["name"])
        return schema

    def after(self, schema: Any) -> Any:
        def add_hashed_schema(schema: Any, prefix: str) -> Any:
            schema_name = f"{prefix}_{hash_schema(schema)}"
            self.schemas_dict[schema_name] = schema
            schema = schema_name
            self.hashed_schema_names.add(schema_name)
            return schema

        if isinstance(schema, dict):
            if "logicalType" in schema:
                schema = add_hashed_schema(schema, "logical")
            elif "name" in schema:
                # named schemas can be decomposed
                self.schemas_dict[schema["name"]] = schema
                schema = schema["name"]
            elif "type" in schema:
                # need to deal with unnamed dict schemas
                if isinstance(schema["type"], str) and schema["type"] in (
                    "enum",
                    "array",
                ):
                    # for this need to calculate a hashed name and insert
                    # into schemas_dict
                    schema = add_hashed_schema(schema, schema["type"])
                else:
                    raise ValueError(f"Unexpected schema for decomposition {schema}")
            else:
                raise ValueError(f"Dict schema without hike {schema}")
        elif isinstance(schema, list):
            # should all be decomposed into named references
            for s in schema:
                if not isinstance(s, str):
                    raise ValueError(
                        f"Unexpected schema in list {schema}. Should be refernce."
                    )
            schema = add_hashed_schema(schema, "union")
        elif isinstance(schema, str):
            pass
        else:
            raise ValueError(f"Unexpected schema {schema}")

        if isinstance(schema, str):
            if schema not in PRIMITIVES:
                if (
                    schema not in self.named_schemas
                    and schema not in self.hashed_schema_names
                ):
                    self.missing_schema_names.add(schema)
                else:
                    self.referenced_schema_names.add(schema)
        return schema


def parse_to_canonical(
    schema: Any, keep_logicalType: bool, keep_attributes: bool
) -> Any:
    """
    Parse a schema to a canonical form. This involves
    doing the operations as described in section
    "Transforming into Parsing Canonical Form" of the Avro specs.
    """
    name_resolver = ResolveFullname()
    attribute_remover = RemoveNonCanonicalAndSortAttributes(keep_logicalType)

    def cb_before(schema):
        schema = name_resolver.before(schema)
        if not keep_attributes:
            schema = attribute_remover.before(schema)
        schema = remove_unicode_escapes(schema)
        schema = ensure_integer_fixed(schema)
        return schema

    def cb_after(schema):
        schema = name_resolver.after(schema)
        schema = simplify(schema)
        return schema

    return depth_first_walk_schema(schema, cb_before, cb_after)


def decompose_schema(schema: Any) -> Any:
    """
    Decompose a schema into a set of named schemas and references.
    """
    decomposer = Decomposer()
    schema = depth_first_walk_schema(schema, decomposer.before, decomposer.after)
    return (
        schema,
        decomposer.schemas_dict,
        decomposer.referenced_schema_names,
        decomposer.missing_schema_names,
    )
