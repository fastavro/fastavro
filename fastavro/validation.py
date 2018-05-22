try:
    from . import _validation
except ImportError:
    from . import _validation_py as _validation
from ._validate_common import ValidationErrorData, ValidationError

validate = _validation.validate
validate_many = _validation.validate_many

__all__ = [
    'ValidationError', 'ValidationErrorData', 'validate', 'validate_many'
]
