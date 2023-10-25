import decimal
from typing import Union, List, Dict, Any, Literal, TypedDict

PrimitiveStringSchema = Literal[
    "boolean",
    "bytes",
    "double",
    "float",
    "int",
    "long",
    "null",
    "string",
]


class PrimitiveDictSchemaBase(TypedDict):
    type: PrimitiveStringSchema


class PrimitiveDictSchema(PrimitiveDictSchemaBase, total=False):
    logicalType: str


PrimitiveSchema = Union[PrimitiveStringSchema, PrimitiveDictSchema]
NamedSchema = str
AnySchema = Union[PrimitiveSchema, NamedSchema, "ComplexSchema"]
UnionSchema = List[AnySchema]
Schema = Union[AnySchema, UnionSchema]

NamedSchemas = Dict[str, Union["RecordSchema", "EnumSchema", "FixedSchema"]]


class FieldSchemaBase(TypedDict):
    name: str
    type: Schema


class FieldSchema(FieldSchemaBase, total=False):
    doc: str
    default: Any
    order: str
    aliases: List[str]


class RecordSchemaBase(TypedDict):
    name: str
    type: Literal["record", "error"]


class RecordSchema(RecordSchemaBase, total=False):
    namespace: str
    doc: str
    aliases: List[str]
    fields: List[FieldSchema]
    __fastavro_parsed: bool
    __named_schemas: NamedSchemas


class EnumSchemaBase(TypedDict):
    name: str
    type: Literal["enum"]
    symbols: List[str]


class EnumSchema(EnumSchemaBase, total=False):
    namespace: str
    doc: str
    aliases: List[str]
    default: str


class ArraySchemaBase(TypedDict):
    type: Literal["array"]
    items: Schema


class ArraySchema(ArraySchemaBase, total=False):
    default: List[Any]


class MapSchemaBase(TypedDict):
    type: Literal["map"]
    values: Schema


class MapSchema(MapSchemaBase, total=False):
    default: Dict[str, Any]


class FixedSchemaBase(TypedDict):
    name: str
    type: Literal["fixed"]
    size: int


class FixedSchema(FixedSchemaBase, total=False):
    namespace: str
    aliases: List[str]
    default: Dict[str, Any]
    logicalType: str


ComplexSchema = Union[RecordSchema, EnumSchema, ArraySchema, MapSchema, FixedSchema]
NamedComplexSchema = Union[RecordSchema, EnumSchema, FixedSchema]

_SimpleOutputs = Union[
    None,  # 'null' Avro type
    str,  # 'string' and 'enum'
    float,  # 'float' and 'double'
    int,  # 'int' and 'long'
    decimal.Decimal,  # 'fixed'
    bool,  # 'boolean'
    bytes,  # 'bytes'
]
AvroMessage = Union[_SimpleOutputs, List["AvroMessage"], Dict[str, "AvroMessage"]]
