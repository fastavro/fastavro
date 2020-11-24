from typing import IO, Dict
from ._read import reader

def json_reader(fo: IO, schema: Dict) -> reader: ...
