#!/bin/bash
# Push to pypi, tag and push to bitbucket

pyver=$(python -c 'import sys; print("%s%s" % sys.version_info[:2])')
ver=$(python setup.py --version)

# Fail on 1'st error
set -e
set -x

# Make sure we have made the tag
if git rev-parse $ver >/dev/null 2>&1; then
    true # Tag found
else
    echo "$ver git tag not found. Make sure you run 'make tag' first"
    exit 1
fi

make fresh
FASTAVRO_USE_CYTHON=1 python setup.py sdist

windows_wheels_url="https://ci.appveyor.com/project/scottbelden/fastavro"
if [ ! -f dist/fastavro-${ver}-cp27-cp27m-win_amd64.whl ]; then
    echo "Make sure to download the Python 2.7 wheel from $windows_wheels_url"
    exit 1
fi
if [ ! -f dist/fastavro-${ver}-cp35-cp35m-win_amd64.whl ]; then
    echo "Make sure to download the Python 3.5 wheel from $windows_wheels_url"
    exit 1
fi
if [ ! -f dist/fastavro-${ver}-cp36-cp36m-win_amd64.whl ]; then
    echo "Make sure to download the Python 3.6 wheel from $windows_wheels_url"
    exit 1
fi

linux_wheels_url="https://github.com/tebeka/fastavro/releases/tag/${ver}"
if [ ! -f dist/fastavro-${ver}-cp27-cp27m-manylinux1_x86_64.whl ]; then
    echo "Make sure to download the Python 2.7 wheel from $linux_wheels_url"
    exit 1
fi
if [ ! -f dist/fastavro-${ver}-cp34-cp34m-manylinux1_x86_64.whl ]; then
    echo "Make sure to download the Python 3.4 wheel from $linux_wheels_url"
    exit 1
fi
if [ ! -f dist/fastavro-${ver}-cp35-cp35m-manylinux1_x86_64.whl ]; then
    echo "Make sure to download the Python 3.5 wheel from $linux_wheels_url"
    exit 1
fi
if [ ! -f dist/fastavro-${ver}-cp36-cp36m-manylinux1_x86_64.whl ]; then
    echo "Make sure to download the Python 3.6 wheel from $linux_wheels_url"
    exit 1
fi

twine upload dist/fastavro-${ver}.tar.gz
twine upload dist/fastavro-${ver}*.whl

# print sha so we can use it in conda-forge recipe
sha256sum dist/fastavro-${ver}.tar.gz
rm -fr build dist/* fastavro.egg-info/
