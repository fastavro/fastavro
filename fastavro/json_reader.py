import json

from .schema import extract_record_type
from .six import iteritems
from .write import acquaint_schema


def _read_json(datum, schema):
    record_type = extract_record_type(schema)

    passthrough_schemas = (
        'null',
        'boolean',
        'string',
        'int',
        'long',
        'float',
        'double',
        'enum',
    )

    if record_type in passthrough_schemas:
        return datum

    elif record_type == 'bytes':
        return datum.encode('iso-8859-1')

    elif record_type == 'fixed':
        return datum.encode('iso-8859-1')

    elif record_type == 'union':
        if datum is None:
            return None

        dtype, value = datum.popitem()
        if dtype in (
            'null',
            'boolean',
            'string',
            'int',
            'long',
            'float',
            'double',
            'enum',
            'bytes',
            'map',
            'array',
        ):
            return _read_json(value, dtype)

        for single_schema in schema:
            if (extract_record_type(single_schema) in ('record, enum, fixed')
                    and single_schema.get('name') == dtype):
                return _read_json(value, single_schema)

    elif record_type == 'array':
        dtype = schema['items']
        return [_read_json(item, dtype) for item in datum]

    elif record_type == 'map':
        vtype = schema['values']
        result = {}
        for key, value in iteritems(datum):
            result[key] = _read_json(value, vtype)
        return result

    elif record_type in ('record', 'error', 'request',):
        result = {}
        for field in schema['fields']:
            result[field['name']] = _read_json(
                datum.get(field['name'], field.get('default')),
                field['type'],
            )
        return result

    else:
        raise ValueError('unknown record type - %s' % record_type)


def reader(fo, writer_schema):
    """Iterator over avro json file.

    Parameters
    ----------
    fo: file-like
        Input stream
    writer_schema: dict
        Schema used to write the json file



    Example::

        from fastavro.json_reader import reader
        with open('some-file.json', 'r') as fo:
            for record in reader(fo, schema):
                print(record)
    """
    acquaint_schema(writer_schema)
    for line in fo:
        json_loaded = json.loads(line.strip())
        yield _read_json(json_loaded, writer_schema)
