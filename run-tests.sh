#!/bin/bash
# Run tests suite, you can specify which nose to run by setting environment
# variable `nose` (running without will use `nosetests`).
#   nose=nosetests-3.2 ./run-tests.sh

echo "[$(date)] Running tests on $(hostname) [user=${USER}, pwd=${PWD}]"
echo

nose=${nose-nosetests}
echo "nose is $nose"


$nose -vd $@ tests
