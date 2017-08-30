#!/bin/bash
# Run tests suite

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(2>&1 python --version)"
echo

find . -name '*.pyc' -exec rm {} \;

pyver=$(python -c 'import sys; print("%s.%s" % sys.version_info[:2])')
if [ "${pyver}" != "2.6" ]; then
    echo "running flake8"
    flake8 fastavro tests
fi

check-manifest  --ignore 'fastavro/_*.c'

PYTHONPATH=${PWD} python -m pytest -v $@ tests
