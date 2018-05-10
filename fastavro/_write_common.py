
class ValidationException(Exception):
    def __init__(self, datum, schema, field):
        message = '{field} is {datum} of type ' \
                  '{given_type} expected {schema}'. \
            format(datum=datum, given_type=type(datum),
                   schema=schema, field=field)
        super(ValidationException, self).__init__(message)
        self.message = message
        self.datum = datum
        self.schema = schema
        self.field = field


class ValidationErrors(ValidationException):
    def __init__(self, errors):
        super(Exception, self).__init__(errors)
        self.errors = errors
