from io import BytesIO
import time

from fastavro import writer, reader

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

def write_fastavro(schema, records, runs=1):
    times = []
    for _ in range(runs):
        iostream = BytesIO()
        start = time.time()
        writer(iostream, schema, records)
        end = time.time()
        times.append(end - start)
    print(f'... {runs} runs averaged {sum(times) / runs} seconds')
    return iostream

def write_avro(schema, records, runs=1):
    times = []
    for _ in range(runs):
        iostream = BytesIO()
        start = time.time()
        writer = DataFileWriter(iostream,
                                DatumWriter(),
                                avro.schema.SchemaFromJSONData(schema))
        for record in records:
            writer.append(record)
        writer.flush()
        end = time.time()
        times.append(end - start)
    print(f'... {runs} runs averaged {sum(times) / runs} seconds')
    return iostream

def read_fastavro(iostream, runs=1):
    times = []
    for _ in range(runs):
        iostream.seek(0)
        start = time.time()
        records = list(reader(iostream))
        end = time.time()
        times.append(end - start)
    print(f'... {runs} runs averaged {sum(times) / runs} seconds')
    return records

def read_avro(iostream, runs=1):
    times = []
    for _ in range(runs):
        iostream.seek(0)
        start = time.time()
        records = list(DataFileReader(iostream, DatumReader()))
        end = time.time()
        times.append(end - start)
    print(f'... {runs} runs averaged {sum(times) / runs} seconds')
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

# Configuration is a tuple of (schema, single_record, num_records, num_runs)
configurations = [
    (small_schema, small_record, 1, 100000),
    (small_schema, small_record, 100, 1000),
    (small_schema, small_record, 10000, 10),
    (big_schema, big_record, 1, 100000),
    (big_schema, big_record, 100, 1000),
    (big_schema, big_record, 10000, 10),
]

for schema, single_record, num_records, num_runs in configurations:
    print('')
    print('### fastavro ###')
    original_records = [single_record for _ in range(num_records)]
    print(f'Writing {num_records} records to one file...')
    bytesio = write_fastavro(schema, original_records, runs=num_runs)

    print(f'Reading {num_records} records from one file...')
    records = read_fastavro(bytesio, runs=num_runs)

    assert records == original_records

    print('')
    print('### standard avro ###')
    original_records = [single_record for _ in range(num_records)]
    print(f'Writing {num_records} records to one file...')
    bytesio = write_avro(schema, original_records, runs=num_runs)

    print(f'Reading {num_records} records from one file...')
    records = read_avro(bytesio, runs=num_runs)

    assert records == original_records
