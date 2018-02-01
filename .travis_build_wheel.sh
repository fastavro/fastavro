#!/bin/bash

cd `dirname $0`

for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" install cython
    "${PYBIN}/python" setup.py bdist_wheel
done
