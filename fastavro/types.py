import decimal
from typing import Union, List, Dict, Any, Literal, TypedDict

PrimitiveString = Literal[
    "boolean",
    "bytes",
    "double",
    "float",
    "int",
    "long",
    "null",
    "string",
]


class PrimitiveDictBase(TypedDict):
    type: PrimitiveString


class PrimitiveDict(PrimitiveDictBase, total=False):
    logicalType: str


PrimitiveSchema = Union[PrimitiveString, PrimitiveDict]
NamedSchema = str
AnySchema = Union[PrimitiveSchema, NamedSchema, "ComplexSchema"]
UnionSchema = List[AnySchema]
Schema = Union[AnySchema, UnionSchema]

NamedSchemas = Dict[str, Union["Record", "Enum", "Fixed"]]


class FieldBase(TypedDict):
    name: str
    type: Schema


class Field(FieldBase, total=False):
    doc: str
    default: Any
    order: str
    aliases: List[str]


class RecordBase(TypedDict):
    name: str
    type: Literal["record", "error"]


class Record(RecordBase, total=False):
    namespace: str
    doc: str
    aliases: List[str]
    fields: List[Field]
    __fastavro_parsed: bool
    __named_schemas: NamedSchemas


class EnumBase(TypedDict):
    name: str
    type: Literal["enum"]
    symbols: List[str]


class Enum(EnumBase, total=False):
    namespace: str
    doc: str
    aliases: List[str]
    default: str


class ArrayBase(TypedDict):
    type: Literal["array"]
    items: Schema


class Array(ArrayBase, total=False):
    default: List[Any]


class MapBase(TypedDict):
    type: Literal["map"]
    values: Schema


class Map(MapBase, total=False):
    default: Dict[str, Any]


class FixedBase(TypedDict):
    name: str
    type: Literal["fixed"]
    size: int


class Fixed(FixedBase, total=False):
    namespace: str
    aliases: List[str]
    default: Dict[str, Any]
    logicalType: str


ComplexSchema = Union[Record, Enum, Array, Map, Fixed]
NamedComplexSchema = Union[Record, Enum, Fixed]

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
