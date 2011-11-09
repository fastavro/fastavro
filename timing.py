#!/usr/bin/env python

import sys
from os import environ
from time import time

sys.path.append('{HOME}/work/bdw-etl'.format(**environ))
from bushido.common.avro import java_iter_avro

filename = sys.argv[1]


start = time()
for record in java_iter_avro(filename):
    pass
t = time() - start
print('javro: {0}'.format(t))

from fastavro import iter_avro
start = time()
for record in iter_avro(open(filename, 'rb')):
    pass
t = time() - start
print('fastavro: {0}'.format(t))

import avro.io, avro.datafile
fo = open(filename, 'rb')
df = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
start = time()
for record in df:
    pass

t = time() - start
print('avro: {0}'.format(t))

