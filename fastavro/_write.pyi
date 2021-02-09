from typing import Callable, Dict, IO, Iterable, Optional, Union
from .io.binary_encoder import BinaryEncoder
from .types import AvroMessage

def writer(
    fo: IO,
    schema: Dict,
    records: Iterable,
    codec: str,
    sync_interval: int,
    metadata: Optional[Dict],
    validator: Union[Callable, bool, None],
    sync_marker: Optional[bytes],
    codec_compression_level: Optional[int],
) -> None: ...

class GenericWriter:
    schema: Dict
    validate_fn: Callable
    metadata: Dict

class Writer(GenericWriter):
    encoder: BinaryEncoder
    io: BinaryEncoder
    block_count: int
    sync_interval: int
    compression_level: Optional[int]
    block_writer: Callable
    def __init__(
        self,
        fo: IO,
        records: Iterable,
        codec: str,
        sync_interval: int,
        metadata: Optional[Dict],
        validator: Union[Callable, bool, None],
        sync_marker: Optional[bytes],
        codec_compression_level: Optional[int],
    ): ...
    def dump(self) -> None: ...
    def write(self, record: AvroMessage) -> None: ...
    def write_block(self, block) -> None: ...
    def flush(self) -> None: ...

def schemaless_writer(fo: IO, schema: Dict, record: Dict) -> None: ...
