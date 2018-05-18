'''Fast Avro file iteration.

Example usage::

    # Validating
    from fastavro import validator

    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
        ],
    }

    # 'records' can be an iterable (including generator)
    records = [
        {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
        {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
        {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
        {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
    ]

    writer(out, schema, records)
'''
try:
    from . import _validate
except ImportError:
    from . import _validate_py as _validate
from ._validate_common import ValidationErrorData, ValidationError

validate = _validate.validate
register_validator = _validate.register_validator
get_validator = _validate.get_validator
validate_many = _validate.validate_many

__all__ = ['ValidationError', 'ValidationErrorData', 'validate',
           'validate_many', 'register_validator', 'get_validator']
