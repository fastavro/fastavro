#!/usr/bin/env python

import sys
from os import environ
from time import time

def main(argv=None):
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(description='Run timing info')
    parser.add_argument('avro_file', help='Avro file for iterate')
    parser.add_argument('--pyavro', default=False, action='store_true',
                        help='run the avro python benchmark as well')

    args = parser.parse_args(argv[1:])


    from fastavro import reader
    print('Using {}'.format(reader))
    start = time()
    for i, record in enumerate(reader(open(args.avro_file, 'rb')), 1):
        pass
    t = time() - start
    print('fastavro: {0} [{1} records]'.format(t, i))

    if args.pyavro:
        import avro.io, avro.datafile
        fo = open(args.avro_file, 'rb')
        df = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
        start = time()
        for i, record in enumerate(df, 1):
            pass

        t = time() - start
        print('avro: {0} [{1} records]'.format(t, i))

if __name__ == '__main__':
    main()
