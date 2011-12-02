#!/bin/bash

set -e

python=${PYTHON:python}

echo "Using $python interpreter"
echo

for avro in avro-files/*.avro;
do
    echo $avro
    $python ./fastavro.py -q "$avro"
done
