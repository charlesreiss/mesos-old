#!/bin/bash

# $Id: $

protoc -I=../include/mesos -I=../src \
  --python_out=. \
  ../src/usage_log/usage_log.proto \
  ../src/messages/messages.proto \
  ../include/mesos/mesos.proto

touch usage_log/__init__.py
touch messages/__init__.py
