from collections import namedtuple
import json


class ValidationErrorData(namedtuple('ValidationErrorData',
                                     ['datum', 'schema', 'field'])):
    def __str__(self):
        if self.field is None:
            self.field = ''

        if self.datum is None:
            return 'Field({field}) is None' \
                   ' expected {schema}'.format(field=self.field,
                                               schema=self.schema)
        return '{field} is <{datum}> of type ' \
               '{given_type} expected {schema}'. \
            format(datum=self.datum, given_type=type(self.datum),
                   schema=self.schema, field=self.field)


class ValidationError(Exception):
    def __init__(self, *errors):
        message = json.dumps([str(e) for e in errors],
                             indent=2,
                             ensure_ascii=False)
        super(ValidationError, self).__init__(message)
        self.errors = errors
