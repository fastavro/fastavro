#!/bin/bash

cd `dirname $0`

# The version comes in as something like 3.6, but we need it to be 36 for the
# path in the container so we remove the period
TRAVIS_PYTHON_VERSION=$1
TRAVIS_PYTHON_VERSION=${TRAVIS_PYTHON_VERSION/.}

"/opt/python/*${TRAVIS_PYTHON_VERSION}*m/pip" install cython
"/opt/python/*${TRAVIS_PYTHON_VERSION}*m/python" setup.py bdist_wheel
