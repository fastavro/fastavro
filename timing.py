#!/usr/bin/env python

import sys
from os import environ
from time import time

def main():
    filename = sys.argv[1]

    sys.path.append('{HOME}/work/bdw-etl'.format(**environ))
    try:
        from bushido.common.avro import java_iter_avro
        start = time()
        for i, record in enumerate(java_iter_avro(filename), 1):
            pass
        t = time() - start
        print('javro: {0} [{1} records]'.format(t, i))
    except ImportError:
        print('Skipping javro')

    from fastavro import iter_avro
    start = time()
    for i, record in enumerate(iter_avro(open(filename, 'rb')), 1):
        pass
    t = time() - start
    print('fastavro: {0} [{1} records]'.format(t, i))

    import avro.io, avro.datafile
    fo = open(filename, 'rb')
    df = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
    start = time()
    for i, record in enumerate(df, 1):
        pass

    t = time() - start
    print('avro: {0} [{1} records]'.format(t, i))

if __name__ == '__main__':
    main()
