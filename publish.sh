#!/bin/bash
# Push to pypi

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
macosx_10_9_universal2
manylinux_2_17_x86_64.manylinux2014_x86_64
manylinux_2_17_aarch64.manylinux2014_aarch64
musllinux_1_2_x86_64
musllinux_1_2_aarch64"

PyVers="39
310
311
312
313
"

for os in $OSes; do
    for pyver in $PyVers; do
        if [[ ${os} == "macosx_10_9_universal2" && ${pyver} == "312" ]]; then
            wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}-macosx_10_13_universal2.whl
        elif [[ ${os} == "macosx_10_9_universal2" && ${pyver} == "313" ]]; then
            wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}-macosx_10_13_universal2.whl
        else
            wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}-${os}.whl
        fi
    done
done

make fresh
python setup.py sdist

twine upload dist/fastavro-${ver}.tar.gz
twine upload dist/fastavro-${ver}*.whl

# print sha so we can use it in conda-forge recipe
sha256sum dist/fastavro-${ver}.tar.gz
rm -fr build dist/* fastavro.egg-info/
