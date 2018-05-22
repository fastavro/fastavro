from io import BytesIO
import datetime
import time

from fastavro import writer, reader
from fastavro._timezone import utc

from fastavro.validation import validate, validate_many


def write(schema, records, runs=1):
    times = []
    for _ in range(runs):
        iostream = BytesIO()
        start = time.time()
        writer(iostream, schema, records)
        end = time.time()
        times.append(end - start)
    print('... {0} runs averaged {1} seconds'.format(runs, (sum(times) / runs)))
    return iostream


def validater(schema, records, runs=1):
    times = []
    valid = []
    for _ in range(runs):
        start = time.time()
        valid = validate_many(records, schema)
        end = time.time()
        times.append(end - start)
    print('... {0} runs averaged {1} seconds'.format(runs, (sum(times) / runs)))
    return valid


def read(iostream, runs=1):
    times = []
    for _ in range(runs):
        iostream.seek(0)
        start = time.time()
        records = list(reader(iostream))
        end = time.time()
        times.append(end - start)
    print('... {0} runs averaged {1} seconds'.format(runs, (sum(times) / runs)))
    return records


small_schema = {
    "type": "record",
    "name": "Test",
    "namespace": "test",
    "fields": [{
        "name": "field",
        "type": {"type": "string"}
    }]
}

big_schema = {
    "type": "record",
    "name": "userInfo",
    "namespace": "my.example",
    "fields": [{
        "name": "username",
        "type": "string",
        "default": "NONE"
    }, {
        "name": "age",
        "type": "int",
        "default": -1
    }, {
        "name": "phone",
        "type": "string",
        "default": "NONE"
    }, {
        "name": "housenum",
        "type": "string",
        "default": "NONE"
    }, {
        "name": "address",
        "type": {
            "type": "record",
            "name": "mailing_address",
            "fields": [{
                "name": "street",
                "type": "string",
                "default": "NONE"
            }, {
                "name": "city",
                "type": "string",
                "default": "NONE"
            }, {
                "name": "state_prov",
                "type": "string",
                "default": "NONE"
            }, {
                "name": "country",
                "type": "string",
                "default": "NONE"
            }, {
                "name": "zip",
                "type": "string",
                "default": "NONE"
            }]
        },
        "default": {}
    }]
}

timestamp_schema = {
    "fields": [
        {
            "name": "timestamp-micros",
            "type": {'type': 'long', 'logicalType': 'timestamp-micros'}
        },
    ],
    "namespace": "namespace",
    "name": "name",
    "type": "record"
}

small_record = {'field': 'foo'}
big_record = {
    'username': 'username',
    'age': 10,
    'phone': '000000000',
    'housenum': '0000',
    'address': {
        'street': 'street',
        'city': 'city',
        'state_prov': 'state_prov',
        'country': 'country',
        'zip': 'zip',
    },
}

timestamp_record = {
    'timestamp-micros': datetime.datetime.now().replace(tzinfo=utc),
}

# Configuration is a tuple of (schema, single_record, num_records, num_runs)
configurations = [
    ('Small - 1 rec - 100,000 runs', small_schema, small_record, 1, 100000),
    ('Small - 100 rec - 1,000 runs', small_schema, small_record, 100, 1000),
    ('Small - 10,000 rec - 10 runs', small_schema, small_record, 10000, 10),
    ('Big - 1 rec - 100,000 runs', big_schema, big_record, 1, 100000),
    ('Big - 100 rec - 1,000 runs', big_schema, big_record, 100, 1000),
    ('Big - 10,000 rec - 10 runs', big_schema, big_record, 10000, 10),
    ('Timestamp - 100,000 rec - 10 runs', timestamp_schema, timestamp_record, 100000, 10),
]

for desc, schema, single_record, num_records, num_runs in configurations:
    print('')
    print(desc)
    original_records = [single_record for _ in range(num_records)]
    print('Writing {0} records to one file...'.format(num_records))
    bytesio = write(schema, original_records, runs=num_runs)

    print('Reading {0} records from one file...'.format(num_records))
    records = read(bytesio, runs=num_runs)

    print('Validating {0} records from one file...'.format(num_records))
    valid = validater(schema, original_records, runs=num_runs)

    assert records == original_records
