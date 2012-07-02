#!/bin/bash

set -x

PYTHON=python

if [ "`uname`" == "SunOS" ]; then
  PYTHON=python2.6
fi

$PYTHON `dirname $0`/startapache.py $@
