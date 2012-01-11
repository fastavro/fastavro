#!/bin/bash
# Test installment, requires virtualenv

rm -fr build dist fastavro.egg-info
dest=/tmp/ve
if [ -e $dest ]; then
    rm -rf $dest
fi
#virtualenv --no-site-packages /tmp/ve
virtualenv  /tmp/ve
source /tmp/ve/bin/activate
python setup.py install
(cd /tmp && python -c 'import fastavro; print fastavro.iter_avro')
