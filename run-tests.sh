#!/bin/bash

nose=${1-nosetests}

$nose -vd tests
