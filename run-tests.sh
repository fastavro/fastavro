#!/bin/bash
# Run tests suite

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(2>&1 python --version)"
echo

find . -name '*.pyc' -exec rm {} \;

echo "running flake8"
flake8 fastavro tests

check-manifest  --ignore 'fastavro/_*.c'

# Build Cython modules
python setup.py build_ext --inplace

PYTHONPATH=${PWD} python -m pytest -v $@ tests
