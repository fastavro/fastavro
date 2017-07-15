#!/bin/bash
# Push to pypi, tag and push to bitbucket

pyver=$(python -c 'import sys; print("%s%s" % sys.version_info[:2])')
ver=$(python setup.py --version)
pkg=fastavro-${ver}-py${pyver}_0.tar.bz2


# Fail on 1'st error
set -e
set -x

make
python setup.py sdist
twine upload dist/fastavro-${ver}.tar.gz
# TODO: upload fails for some reason
#conda build .
#anaconda upload /opt/anaconda3/conda-bld/linux-64/${pkg}
git tag -f $(python setup.py --version)
git push
git push --tags
# print sha so we can use it in conda-forge recipe
sha256sum dist/fastavro-${ver}.tar.gz
rm -fr build dist fastavro.egg-info/
