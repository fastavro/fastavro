#!/bin/bash

cd `dirname $0`

# The version comes in as something like 3.6, but we need it to be 36 for the
# path in the container so we remove the period
TRAVIS_PYTHON_VERSION=$1
TRAVIS_PYTHON_VERSION=${TRAVIS_PYTHON_VERSION/.}

versions="cp${TRAVIS_PYTHON_VERSION}-cp${TRAVIS_PYTHON_VERSION}
cp${TRAVIS_PYTHON_VERSION}-cp${TRAVIS_PYTHON_VERSION}m
cp${TRAVIS_PYTHON_VERSION}-cp${TRAVIS_PYTHON_VERSION}mu"

for version in ${versions}; do
    echo $version
    if ls /opt/python/${version} &> /dev/null; then
        /opt/python/${version}/bin/pip install cython
        FASTAVRO_USE_CYTHON=1 /opt/python/${version}/bin/python setup.py bdist_wheel
    fi
done

# Fix wheel
for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist
done

# Remove unfixed wheel
rm -rf dist/fastavro-*-linux_*
# Remove the manylinux1 wheel
rm -rf dist/fastavro-*-manylinux1_*
