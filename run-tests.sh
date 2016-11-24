#!/bin/bash
# Run tests suite, you can specify which nose to run by setting environment
# variable `nose` (running without will use `nosetests`).
#   nose=nosetests-3.2 ./run-tests.sh

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(python --version)"
echo

find . -name '*.pyc' -exec rm {} \;

pyver=$(python -c 'import sys; print("%s.%s" % sys.version_info[:2])')
if [ "${pyver}" != "2.6" ]; then
    echo "running flake8"
    flake8 fastavro tests
fi

check-manifest  --ignore 'fastavro/_*.c'

nose=${nose-nosetests}
echo "nose is $nose"

$nose -vd $@ tests
