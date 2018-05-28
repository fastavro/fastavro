#!/bin/bash

# Fail on 1st error
set -e
set -x

# Make a new git tag (this builds the manylinux wheels)
git tag -f $(python setup.py --version)
git push
git push --tags
