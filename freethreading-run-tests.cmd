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
	black --target-version py39 --diff fastavro/ tests/ setup.py
	black --target-version py39 --check fastavro/ tests/ setup.py
)

python setup.py build_ext --inplace

set PYTHONPATH=%cd%
set PYTHON_GIL=0
python -m pytest --cov=fastavro -v --cov-report=term-missing --cov-report=html:build/htmlcov tests || EXIT \B 1
endlocal
