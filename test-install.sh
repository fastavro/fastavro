#!/bin/bash
# Test installment, requires virtualenv

set -e
python=${1-python}
virtualenv=${2-virtualenv}

rm -fr build dist fastavro.egg-info
dest=/tmp/ve
if [ -e $dest ]; then
    rm -rf $dest
fi
$virtualenv  $dest
source ${dest}/bin/activate
$python setup.py install
(cd /tmp && python -c 'import fastavro; print(fastavro.iter_avro)')
(cd /tmp && fastavro --help)
(cd /tmp && fastavro --codecs)
