#!/usr/bin/env python

import sys
from time import time


def timeit(name, reader):
    start = time()
    num_records = sum(1 for record in reader)
    duration = time() - start

    print('{0}: {1} [{2} records]'.format(name, duration, num_records))


def main(argv=None):
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(description='Run timing info')
    parser.add_argument('avro_file', help='Avro file for iterate')
    parser.add_argument('--pyavro', default=False, action='store_true',
                        help='run the avro python benchmark as well')

    args = parser.parse_args(argv[1:])

    from fastavro import reader
    print('Using {0}'.format(reader))
    with open(args.avro_file, 'rb') as fo:
        timeit('fastavro', reader(fo))

    if args.pyavro:
        import avro.io
        import avro.datafile
        with open(args.avro_file, 'rb') as fo:
            reader = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
            timeit('avro', reader)


if __name__ == '__main__':
    main()
