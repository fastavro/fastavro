try:
    from . import _validation
except ImportError:
    from . import _validation_py as _validation  # type: ignore
from ._validate_common import (
    ValidationErrorData,
    ValidationError,
    ValidationValueErrorData,
    ValidationValueError,
)

# Private API
_validate = _validation._validate  # type: ignore

# Public API
validate = _validation.validate
validate_many = _validation.validate_many

__all__ = [
    "ValidationError",
    "ValidationErrorData",
    "ValidationValueErrorData",
    "ValidationValueError",
    "validate",
    "validate_many",
]
