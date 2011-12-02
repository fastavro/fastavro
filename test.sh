#!/bin/bash

for avro in avro-files/*.avro;
do
    echo $avro
    ./fastavro.py -q "$avro"
done
