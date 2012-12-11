#!/bin/bash
# Push to pypi, tag and push to bitbucket

make
python setup.py bdist_egg upload
python3 setup.py bdist_egg upload
python3.3 setup.py bdist_egg upload
python setup.py sdist upload
rm -fr build dist
hg tag -f $(python setup.py --version)
hg push
