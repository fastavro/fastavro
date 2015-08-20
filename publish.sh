#!/bin/bash
# Push to pypi, tag and push to bitbucket

# Fail on 1'st error
set -e

make
python3 setup.py bdist_egg upload
python2 setup.py bdist_egg upload
python setup.py sdist upload
/opt/anaconda/bin/conda bdist_conda --binstar-upload
rm -fr build dist fastavro.egg-info/
git tag -f $(python setup.py --version)
git push --tags
