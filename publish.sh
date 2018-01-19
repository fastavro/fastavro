#!/bin/bash
# Push to pypi, tag and push to bitbucket

pyver=$(python -c 'import sys; print("%s%s" % sys.version_info[:2])')
ver=$(python setup.py --version)

# Fail on 1'st error
set -e
set -x

make
python setup.py sdist
twine upload dist/fastavro-${ver}.tar.gz

git tag -f $(python setup.py --version)
git push
git push --tags
# print sha so we can use it in conda-forge recipe
sha256sum dist/fastavro-${ver}.tar.gz
rm -fr build dist/* fastavro.egg-info/
