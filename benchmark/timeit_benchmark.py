import argparse
import array
import datetime
import timeit
from io import BytesIO

import avro
import avro.datafile
import fastavro
from pytz import utc

TIMEIT_FORMAT = "{:25} {:6} records, best of {:6}: {:10.6f} ms"


def write(schema, record, num_records, runs):
    schema = fastavro.parse_schema(schema)
    records = [record] * num_records
    iostream = BytesIO()
    writer = fastavro.writer
    duration = timeit.repeat(
        "iostream.seek(0);"
        "writer(iostream, schema, records);",
        number=1,
        repeat=runs,
        globals=locals())
    print(
        TIMEIT_FORMAT.format("write fastavro", num_records, runs,
                             min(duration) * 1e3))
    return iostream


def write_avro(schema, record, num_records, runs):
    records = [record] * num_records
    iostream = BytesIO()

    avro_writer = avro.datafile.DataFileWriter
    schema = avro.schema.make_avsc_object(schema)
    datum_writer = avro.io.DatumWriter(schema)

    duration = timeit.repeat("""
iostream.seek(0)
writer = avro_writer(iostream, datum_writer, schema)
for record in records:
    writer.append(record);
writer.flush()
""",
                             number=1,
                             repeat=runs,
                             globals=locals())
    print(
        TIMEIT_FORMAT.format("write avro", num_records, runs,
                             min(duration) * 1e3))
    return iostream


def write_schemaless(schema, record, num_records, runs):
    schema = fastavro.parse_schema(schema)
    iostream = BytesIO()
    schemaless_writer = fastavro.schemaless_writer
    duration = timeit.repeat(
        "iostream.seek(0);"
        "schemaless_writer(iostream, schema, record);",
        number=num_records,
        repeat=runs,
        globals=locals())
    print(
        TIMEIT_FORMAT.format("write schemaless fastavro", num_records, runs,
                             min(duration) * 1e3))
    return iostream


def write_schemaless_avro(schema, record, num_records, runs):
    iostream = BytesIO()

    schema = avro.schema.make_avsc_object(schema)
    avro_encoder = avro.io.BinaryEncoder(iostream)
    datum_writer = avro.io.DatumWriter(schema)

    duration = timeit.repeat("""
iostream.seek(0)
datum_writer.write(record, avro_encoder)
""",
                             number=num_records,
                             repeat=runs,
                             globals=locals())
    print(
        TIMEIT_FORMAT.format("write schemaless avro", num_records, runs,
                             min(duration) * 1e3))
    return iostream


def validater(schema, record, num_records, runs):
    schema = fastavro.parse_schema(schema)
    records = [record] * num_records
    validate_many = fastavro.validation.validate_many
    duration = timeit.repeat("validate_many(records, schema);",
                             number=1,
                             repeat=runs,
                             globals=locals())
    print(
        TIMEIT_FORMAT.format("validate fastavro", num_records, runs,
                             min(duration) * 1e3))
    return validate_many(records, schema)


def read(iostream, num_records, runs):
    reader = fastavro.reader
    duration = timeit.repeat("iostream.seek(0);"
                             "[*reader(iostream)];",
                             number=1,
                             repeat=runs,
                             globals=locals())
    print(
        TIMEIT_FORMAT.format("read  fastavro", num_records, runs,
                             min(duration) * 1e3))
    iostream.seek(0)
    return [*reader(iostream)]


def read_avro(iostream, num_records, runs):
    avro_reader = avro.datafile.DataFileReader
    datum_reader = avro.io.DatumReader()

    duration = timeit.repeat(
        "iostream.seek(0);"
        "[*avro_reader(iostream, datum_reader)];",
        number=1,
        repeat=runs,
        globals=locals())
    print(
        TIMEIT_FORMAT.format("read  avro", num_records, runs,
                             min(duration) * 1e3))
    iostream.seek(0)
    return [*avro_reader(iostream, datum_reader)]


def read_schemaless(iostream, schema, num_records, runs):
    schema = fastavro.parse_schema(schema)
    schemaless_reader = fastavro.schemaless_reader
    duration = timeit.repeat(
        "iostream.seek(0);"
        "schemaless_reader(iostream, schema);",
        number=num_records,
        repeat=runs,
        globals=locals())
    print(
        TIMEIT_FORMAT.format("read  schemaless fastavro", num_records, runs,
                             min(duration) * 1e3))
    iostream.seek(0)
    return schemaless_reader(iostream, schema)


def read_schemaless_avro(iostream, schema, num_records, runs):
    schema = avro.schema.make_avsc_object(schema)
    avro_decoder = avro.io.BinaryDecoder(iostream)
    datum_reader = avro.io.DatumReader(schema)

    duration = timeit.repeat(
        "iostream.seek(0);"
        "datum_reader.read(avro_decoder);",
        number=num_records,
        repeat=runs,
        globals=locals())
    print(
        TIMEIT_FORMAT.format("read  schemaless avro", num_records, runs,
                             min(duration) * 1e3))
    iostream.seek(0)
    return datum_reader.read(avro_decoder)


def test_case(name,
              schema,
              record,
              num_records,
              num_runs,
              run_avro=True,
              test_record=None):
    print("\n{:25} {:6} records, best of {:6}\n{:=>56}".format(
        name, num_records, num_runs, '='))

    if test_record is None:
        test_record = record

    if run_avro:
        avro_bytesio = write_avro(schema, record, num_records, runs=num_runs)
    fastavro_bytesio = write(schema, record, num_records, runs=num_runs)

    if run_avro:
        avro_records = read_avro(avro_bytesio, num_records, runs=num_runs)
        assert avro_records == [test_record] * num_records
    fastavro_records = read(fastavro_bytesio, num_records, runs=num_runs)
    assert fastavro_records == [test_record] * num_records

    if run_avro:
        avro_bytesio = write_schemaless_avro(schema,
                                             record,
                                             num_records,
                                             runs=num_runs)
    fastavro_bytesio = write_schemaless(schema,
                                        record,
                                        num_records,
                                        runs=num_runs)
    if run_avro:
        assert avro_bytesio.getbuffer() == fastavro_bytesio.getbuffer()

    if run_avro:
        avro_record = read_schemaless_avro(avro_bytesio,
                                           schema,
                                           num_records,
                                           runs=num_runs)
        assert avro_record == test_record
    fastavro_record = read_schemaless(fastavro_bytesio,
                                      schema,
                                      num_records,
                                      runs=num_runs)
    assert fastavro_record == test_record

    assert validater(schema, record, num_records, runs=num_runs)


small_schema = {
    "type": "record",
    "name": "Test",
    "namespace": "test",
    "fields": [{
        "name": "field",
        "type": {
            "type": "string"
        }
    }]
}

big_schema = {
    "type":
    "record",
    "name":
    "userInfo",
    "namespace":
    "my.example",
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
            "type":
            "record",
            "name":
            "mailing_address",
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
            "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        },
    ],
    "namespace":
    "namespace",
    "name":
    "name",
    "type":
    "record"
}

array_schema = {"type": "array", "items": "long"}

small_record = {"field": "foo"}
big_record = {
    "username": "username",
    "age": 10,
    "phone": "000000000",
    "housenum": "0000",
    "address": {
        "street": "street",
        "city": "city",
        "state_prov": "state_prov",
        "country": "country",
        "zip": "zip",
    },
}

timestamp_record = {
    "timestamp-micros": datetime.datetime.now().replace(tzinfo=utc),
}

list_record = [*range(-512, 512)]
array_record = array.array("i", list_record)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("--run-avro", action="store_true")
    args = parser.parse_args()

    test_case("Small", small_schema, small_record, 1, 100,
              run_avro=args.run_avro)
    test_case("Small", small_schema, small_record, 100, 100,
              run_avro=args.run_avro)
    test_case("Big", big_schema, big_record, 1, 100,
              run_avro=args.run_avro)
    test_case("Big", big_schema, big_record, 100, 100,
              run_avro=args.run_avro)
    test_case("Timestamp", timestamp_schema, timestamp_record, 1, 100,
              run_avro=args.run_avro)
    test_case("Timestamp", timestamp_schema, timestamp_record, 100, 100,
              run_avro=args.run_avro)
    test_case("Array from list", array_schema, list_record, 1, 100,
              run_avro=args.run_avro)
    test_case("Array from list", array_schema, list_record, 100, 10,
              run_avro=args.run_avro)
    for length in (1, 10, 100, 1000):
        list_record = [*range(-length//2, length//2)]
        array_record = array.array("i", list_record)

        test_case(f"Array from list({length})",
                  array_schema, list_record, 100, 100,
                  run_avro=False)
        test_case(f"Array from array({length})",
                  array_schema, array_record, 100, 100,
                  test_record=list_record,
                  run_avro=False)
