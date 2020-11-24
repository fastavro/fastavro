from typing import Any, Dict, Iterable, Optional

def validate(
    datum: Any, schema: Dict, field: Optional[str], raise_errors: bool
) -> bool: ...
def validate_many(
    records: Iterable[Dict], schema: Dict, raise_errors: bool
) -> bool: ...
