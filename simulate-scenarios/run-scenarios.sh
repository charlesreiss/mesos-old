#!/bin/bash

set -e

renice -n +20 $$

ulimit -c unlimited

#VALGRIND='../build-linux/libtool --mode=execute valgrind --tool=callgrind --cache-sim=yes'
VALGRIND='../build-linux/libtool --mode=execute valgrind --log-file=valgrind.out --suppressions=../build-linux/src/suppressions.valgrind --track-origins=yes'
#VALGRIND=''
SIMULATE='../build-linux/src/mesos-simulate --fake_interval=0.25 --no_create_work_dir'

export GLOG_minloglevel=1
#export GLOG_v=1
#export GLOG_logtostderr=1

for SCENARIO in $@; do

    if test -d $SCENARIO; then
      mv $SCENARIO old/$SCENARIO.old.$(date +%s)
    fi
    mkdir $SCENARIO
    mkdir $SCENARIO/logs

    $SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem \
      --allocator=norequest --usage_log_base=$SCENARIO/logs/norequest. \
      >$SCENARIO/norequest.csv 2>$SCENARIO.logfile &
    if false; then
        $SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem \
          --allocator=norequest --usage_log_base=$SCENARIO/logs/rorequest-aggressive. \
          --norequest_aggressive \
          >$SCENARIO/norequest-aggressive.csv &
    fi
    if test x$GLOG_logtostderr != x1; then
        $SIMULATE --json_file=$SCENARIO.json --allocator=simple \
          --usage_log_base=$SCENARIO/logs/simple-strong. \
          > $SCENARIO/simple-strong.csv 2>$SCENARIO.logfile.ss&
        $SIMULATE --json_file=$SCENARIO.json --fake_extra_cpu --fake_extra_mem \
          --usage_log_base=$SCENARIO/logs/simple-weak. \
          --allocator=simple >$SCENARIO/simple-weak.csv 2>$SCENARIO.logfile.sw &
    fi

done
