import decimal
from typing import Any, Callable, Dict, Iterator, IO, Optional, Union, Tuple
from .types import AvroMessage

class reader:
    fo: IO
    return_record_name: bool
    metadata: Dict[str, bytes]
    codec: str
    reader_schema: Optional[Dict]
    writer_schema: Optional[Dict]
    def __init__(
        self, fo: IO, reader_schema: Optional[Dict], return_record_name: bool
    ): ...
    def __iter__(self) -> Iterator[AvroMessage]: ...
    def next(self) -> AvroMessage: ...
    def __next__(self) -> AvroMessage: ...

class block_reader:
    fo: IO
    return_record_name: bool
    metadata: Dict[str, bytes]
    codec: str
    reader_schema: Optional[Dict]
    writer_schema: Optional[Dict]
    def __init__(
        self, fo: IO, reader_schema: Optional[Dict], return_record_name: bool
    ): ...
    def __iter__(self) -> Iterator[Block]: ...
    def next(self) -> Block: ...
    def __next__(self) -> Block: ...

class Block:
    num_records: int
    writer_schema: Dict
    reader_schema: Dict
    offset: int
    size: int
    def __init__(
        self,
        bytes_: bytes,
        num_records: int,
        codec: str,
        reader_schema: Dict,
        writer_schema: Dict,
        offset: int,
        size: int,
        return_record_name: bool,
    ): ...
    def __iter__(self) -> Iterator[AvroMessage]: ...
    def __str__(self) -> str: ...

def json_reader(fo: IO, schema: Dict) -> reader: ...
def schemaless_reader(
    fo: IO, writer_schema: Dict, reader_schema: Optional[Dict], return_record_name: bool
) -> AvroMessage: ...
def is_avro(path_or_buffer: Union[str, IO]) -> bool: ...

logical_reader = Callable[[Any, Optional[Dict], Optional[Dict]], Any]
LOGICAL_READERS: Dict[str, logical_reader]

class SchemaResolutionError(Exception): ...

BLOCK_READERS: Dict[str, Callable]
