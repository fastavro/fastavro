#!/bin/bash

version=Python-3.5.0
archive=${version}.tar.xz

curl -LO https://www.python.org/ftp/python/3.5.0/${archive}
tar -xJf ${archive}
cd ${version}
./configure --prefix=${HOME}/.local && make install
