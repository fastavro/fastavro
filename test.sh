#!/bin/bash

set -e

python=${1-python}

echo "Using $python interpreter"
echo

for avro in avro-files/*.avro;
do
    echo $avro
    $python ./main.py -q "$avro"
done
