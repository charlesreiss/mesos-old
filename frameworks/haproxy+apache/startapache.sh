#!/bin/bash

set -x

PYTHON=python

if [ "`uname`" == "SunOS" ]; then
  PYTHON=python2.6
fi

MESOS_BUILD_DIR=`dirname $0`/../../build-linux

DISTRIBUTE_EGG=`echo ${MESOS_SOURCE_DIR}/third_party/distribute-*/*.egg`
PROTOBUF=${MESOS_BUILD_DIR}/third_party/protobuf-2.3.0
PROTOBUF_EGG=`echo ${PROTOBUF}/python/dist/protobuf*.egg`
MESOS_EGG=`echo ${MESOS_BUILD_DIR}/src/python/dist/mesos*.egg`

PYTHONPATH="${DISTRIBUTE_EGG}:${MESOS_EGG}:${PROTOBUF_EGG}:${PYTHONPATH}"
export PYTHONPATH

$PYTHON `dirname $0`/startapache.py $@
