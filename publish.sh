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
macosx_10_15_x86_64
manylinux_2_17_x86_64.manylinux2014_x86_64
manylinux_2_17_aarch64.manylinux2014_aarch64
musllinux_1_1_x86_64
musllinux_1_1_aarch64"

PyVers="37"

for os in $OSes; do
    for pyver in $PyVers; do
        wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}m-${os}.whl
    done
done

PyVers="38
39
310
311"

for os in $OSes; do
    for pyver in $PyVers; do
        if [[ ${os} == "macosx_10_15_x86_64" && ${pyver} == "39" ]]; then
            wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}-macosx_11_0_x86_64.whl
        elif [[ ${os} == "macosx_10_15_x86_64" && ${pyver} == "310" ]]; then
            wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}-macosx_11_0_x86_64.whl
        elif [[ ${os} == "macosx_10_15_x86_64" && ${pyver} == "311" ]]; then
            wget -q --directory-prefix=dist/ https://github.com/fastavro/fastavro/releases/download/${ver}/fastavro-${ver}-cp${pyver}-cp${pyver}-macosx_10_9_universal2.whl
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
