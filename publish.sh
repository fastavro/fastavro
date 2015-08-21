#!/bin/bash
# Push to pypi, tag and push to bitbucket

# Fail on 1'st error
set -e
set -x

make
python3 setup.py bdist_egg upload
python2 setup.py bdist_egg upload
python setup.py sdist upload
/opt/anaconda/bin/python setup.py bdist_conda --binstar-upload
rm -fr build dist fastavro.egg-info/
git tag -f $(python setup.py --version)
git push
git push --tags
