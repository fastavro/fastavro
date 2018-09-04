#!/bin/bash
# Push to pypi, tag and push to bitbucket

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

OSes="macosx_10_13
manylinux1"

PyVers="27
34
35
36
37"

for os in $OSes; do
    for pyver in $PyVers; do
        wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}m-${os}_x86_64.whl
    done
done

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
if [ ! -f dist/fastavro-${ver}-cp37-cp37m-win_amd64.whl ]; then
    echo "Make sure to download the Python 3.7 wheel from $windows_wheels_url"
    exit 1
fi

twine upload dist/fastavro-${ver}.tar.gz
twine upload dist/fastavro-${ver}*.whl

# print sha so we can use it in conda-forge recipe
sha256sum dist/fastavro-${ver}.tar.gz
rm -fr build dist/* fastavro.egg-info/
