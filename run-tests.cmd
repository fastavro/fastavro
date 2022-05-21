@echo off
setlocal

for /F "tokens=* USEBACKQ" %%F in (`python --version 2^>^&1`) do set PYTHON_VERSION=%%F
echo [%date%:%time%] %USERNAME% :: %PYTHON_VERSION%

del /S *.pyc
del /S *.pyd

if "%SKIP_BLACK%" == "1" (
	echo "skipping black"
) else (
	echo "running black"
	black --target-version py36 --diff fastavro/ tests/ setup.py
	black --target-version py36 --check fastavro/ tests/ setup.py
)

echo "running flake8"
flake8 --max-line-length=90 --extend-ignore=E203,E501 fastavro tests
flake8 --config=.flake8.cython fastavro

check-manifest

python setup.py build_ext --inplace

set PYTHONPATH=%cd%
python -m pytest --cov=fastavro -v --cov-report=term-missing --cov-report=html:build/htmlcov tests
endlocal
