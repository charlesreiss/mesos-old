#!/bin/bash

set -e

VALGRIND='../build-linux/libtool --mode=execute valgrind --tool=cachegrind'
SIMULATE='../build-linux/src/mesos-simulate --fake_interval=0.015625'
SCENARIO=$1

export GLOG_minloglevel=1
#export GLOG_v=2

if test -d $1; then
  mv $1 old/$1.old.$(date +%s)
fi
mkdir $1
mkdir $1/logs

$SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem \
  --allocator=norequest --usage_log_base=$SCENARIO/logs/norequest. \
  >$SCENARIO/norequest.csv &
if false; then
    $SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem \
      --allocator=norequest --norequest_aggressive \
      >$SCENARIO/norequest-aggressive.csv &
fi
$SIMULATE --json_file=$SCENARIO.json --allocator=simple \
  --usage_log_base=$SCENARIO/logs/simple-strong. \
  > $SCENARIO/simple-strong.csv &
if false; then
    $SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem \
      --allocator=simple >$SCENARIO/simple-weak.csv &
fi
