#!/bin/bash

cd `dirname $0`

# Don't do anything on pypy
if [[ ${1} == *"pypy"* ]]; then
    exit 0
fi

# The version comes in as something like 3.6, but we need it to be 36 for the
# path in the container so we remove the period
TRAVIS_PYTHON_VERSION=$1
TRAVIS_PYTHON_VERSION=${TRAVIS_PYTHON_VERSION/.}

/opt/python/*${TRAVIS_PYTHON_VERSION}*m/bin/pip install cython
FASTAVRO_USE_CYTHON=1 /opt/python/*${TRAVIS_PYTHON_VERSION}*m/bin/python setup.py bdist_wheel

# Fix wheel
for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist
done

# Remove unfixed wheel
rm -rf dist/fastavro-*-linux_*
