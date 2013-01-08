#!/usr/bin/env python

import fastavro
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema


def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(description='')
    parser.add_argument('type', choices=['fast', 'avro'], default='fast')
    parser.add_argument('--no-debug', '-nd', action='store_false',
                        dest='debug', default=True)
    parser.add_argument('--both', '-b', action='store_true', default=False)
    args = parser.parse_args(argv[1:])


    reader = fastavro.reader(open('tests/avro-files/weather.avro', 'rb'))
    records = [
        {
            'station': 'station1',
            'time': 7,
            'temp': 38,
        }
    ]

    schema = reader.schema
    schema.pop('doc')

    if args.both or args.type == 'fast':
        with open('w-fast.avro', 'wb') as out:
            if args.debug:
                import pdb; pdb.set_trace()
            fastavro.write(out, schema, records)
    elif args.both or args.type == 'avro':
        schema = avro.schema.parse(json.dumps(schema))
        with open('w-avro.avro', 'wb') as out:
            if args.debug:
                import pdb; pdb.set_trace()
            writer = DataFileWriter(out, DatumWriter(), schema)
            for record in records:
                writer.append(record)
            writer.close()


if __name__ == '__main__':
    main()
