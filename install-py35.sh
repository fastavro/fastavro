#!/bin/bash

curl -LO https://www.python.org/ftp/python/3.5.0/Python-3.5.0b2.tar.xz
tar -xJf Python-3.5.0b2.tar.xz
cd Python-3.5.0b2
./configure --prefix=${HOME}/.local && make install
