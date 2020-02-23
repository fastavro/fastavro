@echo off
setlocal

for /F "tokens=* USEBACKQ" %%F in (`python --version 2^>^&1`) do set PYTHON_VERSION=%%F
echo [%date%:%time%] %USERNAME% :: %PYTHON_VERSION%

del /S *.pyc
del /S *.pyd

echo "running flake8"
flake8 fastavro tests
flake8 --config=.flake8.cython fastavro

check-manifest

set FASTAVRO_USE_CYTHON=1
python setup.py build_ext --inplace

set PYTHONPATH=%cd%
python -m pytest --cov=fastavro -v --cov-report=term-missing --cov-report=html:build/htmlcov tests
endlocal
