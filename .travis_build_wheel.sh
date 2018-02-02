#!/bin/bash

cd `dirname $0`
TRAVIS_PYTHON_VERSION=$1

"/opt/python/$TRAVIS_PYTHON_VERSION/pip" install cython
"/opt/python/$TRAVIS_PYTHON_VERSION/python" setup.py bdist_wheel
