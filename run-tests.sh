#!/bin/bash
# Run tests suite

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(2>&1 python --version)"
echo

find . -name '*.pyc' -exec rm {} \;

echo "running flake8"
flake8 fastavro tests
flake8 --config=.flake8.cython fastavro


RUN_MYPY=$(python <<EOF
import sys
if sys.implementation.name != "pypy":
    sys.stdout.write("yes")
else:
    sys.stdout.write("no")
EOF
)

if [[ "$RUN_MYPY" = "yes" ]]; then
    echo "running mypy"
    mypy ./fastavro
else
    echo "skipping mypy"
fi

check-manifest

# Build Cython modules
FASTAVRO_USE_CYTHON=1 python setup.py build_ext --inplace
pip install -e .

PYTHONPATH=${PWD} python -m pytest --cov=fastavro -v --cov-report=term-missing --cov-report=html:build/htmlcov $@
