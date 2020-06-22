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

OSes="win_amd64
win32
macosx_10_14_x86_64
manylinux2014_x86_64"

PyVers="35
36
37"

for os in $OSes; do
    for pyver in $PyVers; do
        wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}m-${os}.whl
    done
done

# Get the 38 versions
for os in $OSes; do
    wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp38-cp38-${os}.whl
done

make fresh
FASTAVRO_USE_CYTHON=1 python setup.py sdist

twine upload dist/fastavro-${ver}.tar.gz
twine upload dist/fastavro-${ver}*.whl

# print sha so we can use it in conda-forge recipe
sha256sum dist/fastavro-${ver}.tar.gz
rm -fr build dist/* fastavro.egg-info/
