import json

from .schema import extract_record_type
from .six import iteritems, btou
from .write import acquaint_schema
from ._write_py import validate


def _write_json(datum, schema):
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
        return btou(datum, encoding='iso-8859-1')

    elif record_type == 'fixed':
        return btou(datum, encoding='iso-8859-1')

    elif record_type == 'union':
        best_match_index = -1
        most_fields = -1
        for index, candidate in enumerate(schema):
            if validate(datum, candidate):
                if extract_record_type(candidate) == 'record':
                    fields = len(candidate['fields'])
                    if fields > most_fields:
                        best_match_index = index
                        most_fields = fields
                else:
                    best_match_index = index
                    break
        if best_match_index < 0:
            pytype = type(datum)
            msg = '%r (type %s) do not match %s' % (datum, pytype, schema)
            raise ValueError(msg)

        best_match_schema = schema[best_match_index]

        if best_match_schema == 'null':
            return None
        else:
            best_records_type = extract_record_type(best_match_schema)
            if best_records_type in ('record, enum, fixed'):
                key = best_match_schema['name']
            else:
                key = best_match_schema
            return {key: _write_json(datum, best_match_schema)}

    elif record_type == 'array':
        dtype = schema['items']
        return [_write_json(item, dtype) for item in datum]

    elif record_type == 'map':
        vtype = schema['values']
        result = {}
        for key, value in iteritems(datum):
            result[key] = _write_json(value, vtype)
        return result

    elif record_type in ('record', 'error', 'request',):
        result = {}
        for field in schema['fields']:
            result[field['name']] = _write_json(
                datum.get(field['name'], field.get('default')),
                field['type'],
            )
        return result

    else:
        raise ValueError('unknown record type - %s' % record_type)


def writer(fo,
           schema,
           records):
    """Write records in json form to fo (stream) according to schema

    Parameters
    ----------
    fo: file-like
        Output stream
    schema: dict
        Schema of the records
    records: iterable
        Records to write



    Example::

        from fastavro.json_writer import writer

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

        records = [
            {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
            {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
            {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
            {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
        ]

        with open('weather.json', 'w') as out:
            writer(out, schema, records)
    """
    acquaint_schema(schema)
    for record in records:
        json.dump(_write_json(record, schema), fo)
        fo.write('\n')
