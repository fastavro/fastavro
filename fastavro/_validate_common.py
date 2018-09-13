from collections import namedtuple
import json
import sys


def python_2_unicode_compatible(klass):
    """
    A decorator that defines __unicode__ and __str__ methods under Python 2.
    Under Python 3 it does nothing.
    To support Python 2 and 3 with a single code base, define a __str__ method
    returning text and apply this decorator to the class.
    """
    if sys.version_info[0] == 2:
        klass.__unicode__ = klass.__str__
        klass.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return klass


@python_2_unicode_compatible
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
