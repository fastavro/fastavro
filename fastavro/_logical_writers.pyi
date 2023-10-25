from typing import Any, Dict, Callable

from fastavro.types import Schema

LOGICAL_WRITERS: Dict[str, Callable[[Any, Schema], Any]]
