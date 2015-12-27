# cython: auto_cpdef=True
'''Utility functions'''

import sys
from functools import wraps

if sys.version_info < (3,):
    INTEGER_TYPES = (int, long,)  # flake8: noqa
else:
    INTEGER_TYPES = (int,)


def push_path(path, name):
    if path is None:
        return [name]
    else:
        path.append(name)
        return path


def pop_path(path):
    if path is None:
        return None
    else:
        path.pop()
        return path


def path_string(path):
    def format_item(item):
        if isinstance(item, INTEGER_TYPES):
            return "[%s]" % str(item)
        else:
            return str(item)

    if path is None:
        return ''
    else:
        return '.'.join(format_item(item) for item in path)


def _get_type_symbol(func):
    name_parts = func.__name__.split('_')
    type_name = '_'.join(name_parts[1:])
    type_symbol = '<' + type_name + '>'
    return type_symbol


def tracked_writer(f):
    type_symbol = _get_type_symbol(f)

    @wraps(f)
    def wrapper(fo, datum, schema=None, path=None):
        path = push_path(path, type_symbol)
        result = f(fo, datum, schema, path)
        pop_path(path)
        return result

    return wrapper


def tracked_reader(f):
    type_symbol = _get_type_symbol(f)

    @wraps(f)
    def wrapper(fo, writer_schema=None, reader_schema=None, path=None):
        path = push_path(path, type_symbol)
        result = f(fo, writer_schema, reader_schema, path)
        pop_path(path)
        return result

    return wrapper
