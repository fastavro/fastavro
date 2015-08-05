#!/bin/bash
# Run tests suite, you can specify which nose to run by setting environment
# variable `nose` (running without will use `nosetests`).
#   nose=nosetests-3.2 ./run-tests.sh

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(python --version)"
echo


echo "running flake8"
flake8 fastavro tests

nose=${nose-nosetests}
echo "nose is $nose"

$nose -vd $@ tests
