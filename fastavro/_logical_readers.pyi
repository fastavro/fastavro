from typing import Any, Dict, Callable, Optional

from fastavro.types import Schema

LOGICAL_READERS: Dict[str, Callable[[Any, Optional[Schema], Optional[Schema]], Any]]
