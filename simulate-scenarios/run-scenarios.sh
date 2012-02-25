#!/bin/bash
SIMULATE=../build-linux/src/mesos-simulate
SCENARIO=$1

export GLOG_minloglevel=1
#export GLOG_v=2

mv $1 $1.old.$(date +%s)
mkdir $1

$SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem --allocator=norequest | tee $SCENARIO/norequest.csv
$SIMULATE --json_file=$SCENARIO.json --allocator=simple | tee $SCENARIO/simple-strong.csv
$SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem --allocator=simple | tee $SCENARIO/simple-weak.csv
