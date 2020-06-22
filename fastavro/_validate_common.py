from collections import namedtuple
import json


class ValidationErrorData(namedtuple('ValidationErrorData',
                                     ['datum', 'schema', 'field'])):
    def __str__(self):
        # update field for prettier printing
        field = self.field
        if field is None:
            field = ''

        if self.datum is None:
            return 'Field({field}) is None' \
                   ' expected {schema}'.format(field=field,
                                               schema=self.schema)

        return u'{field} is <{datum}> of type ' \
               u'{given_type} expected {schema}'. \
            format(datum=self.datum, given_type=type(self.datum),
                   schema=self.schema, field=field)


class ValidationError(Exception):
    def __init__(self, *errors):
        message = json.dumps([str(e) for e in errors],
                             indent=2,
                             ensure_ascii=False)
        super(ValidationError, self).__init__(message)
        self.errors = errors
