#!/bin/bash
# Push to pypi, tag and push to bitbucket

# Fail on 1'st error
set -e

make
python3 setup.py bdist_egg upload
python2 setup.py bdist_egg upload
python setup.py sdist upload
rm -fr build dist
hg tag -f $(python setup.py --version)
hg push
