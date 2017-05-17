# -*- coding: utf-8 -*-
from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals
)

import os
import sys
import tempfile
from collections import namedtuple


temp_folder = tempfile.mkdtemp()
Fruit = namedtuple('Fruit', ['name', 'type'])


test_cases = [
    # dict test
    [
        # path
        'tc1.avro',
        # record
        {'name': 'apple', 'type': 'fruit'},
        # schema
        {
            'name': 'tc2 schema',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'type', 'type': 'string'},
            ]
        },
        # expected
        '\napple\nfruit' if sys.version.startswith('2') else b'\napple\nfruit',
    ],

    # tuple test
    [
        # path
        'tc2.avro',
        # record
        ('apple', 'fruit'),
        # schema
        {
            'name': 'tc2 schema',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'type', 'type': 'string'},
            ]
        },
        # expected
        '\napple\nfruit' if sys.version.startswith('2') else b'\napple\nfruit',
    ],

    # list test
    [
        # path
        'tc3.avro',
        # record
        ['apple', 'fruit'],
        # schema
        {
            'name': 'tc2 schema',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'type', 'type': 'string'},
            ]
        },
        # expected
        '\napple\nfruit' if sys.version.startswith('2') else b'\napple\nfruit',
    ],

    # named tuple test
    [
        # path
        'tc4.avro',
        # record
        Fruit('apple', 'fruit'),
        # schema
        {
            'name': 'tc2 schema',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'type', 'type': 'string'},
            ]
        },
        # expected
        '\napple\nfruit' if sys.version.startswith('2') else b'\napple\nfruit',
    ],

]


def test_write_record():
    from fastavro.writer import write_record

    for path, record, schema, expected in test_cases:
        filepath = os.path.join(temp_folder, path)
        with open(filepath, mode='wb') as stream_out:
            write_record(fo=stream_out, datum=record, schema=schema)

        with open(filepath, mode='rb') as stream_in:
            data = stream_in.read()
            assert data == expected


def test_benchmark_write_record():
    from fastavro.writer import write_record, write_data
    from timeit import Timer

    # percentage lower/upper bounds
    # observed_lower_bound = 5
    # observed_upper_bound = 25

    # Pre update method
    def old_method(fo, datum, schema):
        for field in schema['fields']:
            name = field['name']
            if name not in datum and 'default' not in field and\
                    'null' not in field['type']:
                raise ValueError('no value and no default for %s' % name)
            write_data(fo, datum.get(
                name, field.get('default')), field['type'])

    timing_data = []
    iterations = 10000
    for path, record, schema, expected in test_cases[0:1]:
        filepath = os.path.join(temp_folder, path)
        stream_out = open(filepath, mode='wb')
        t1 = Timer(lambda: old_method(
            fo=stream_out, datum=record, schema=schema)
        )
        t2 = Timer(lambda: write_record(
            fo=stream_out, datum=record, schema=schema)
        )
        timing_data.append(t1.timeit(number=iterations))
        timing_data.append(t2.timeit(number=iterations))
        stream_out.close()

    # timing_old = timing_data[0]
    # timing_new = timing_data[1]
    # percentage_increase = (float(timing_new - timing_old) / timing_new) * 100
    # assert observed_lower_bound <= percentage_increase
    # assert percentage_increase <= observed_upper_bound
