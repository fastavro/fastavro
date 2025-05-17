#!/bin/bash
# Run tests suite

# Exit on error
set -e

echo "[$(date +%Y%m%dT%H%M%S)] ${USER}@$(hostname) :: $(2>&1 python --version)"
echo

find . -name '*.pyc' -exec rm {} \;

if [[ "$SKIP_BLACK" = "1" ]]; then
	true
else
	echo "running black"
	black --target-version py39 --diff fastavro/ tests/ setup.py
	black --target-version py39 --check fastavro/ tests/ setup.py
fi

echo "running flake8"
flake8 --max-line-length=90 --extend-ignore=E203,E501 fastavro tests
flake8 --config=.flake8.cython fastavro

# Build Cython modules
python setup.py build_ext --inplace
pip install -e .

PYTHONPATH=${PWD} PYTHON_GIL=0 python -m pytest --cov=fastavro -v --cov-report=term-missing --cov-report=html:build/htmlcov $@
