set -x
set -e

MAKE_SCENARIO='python ../src/fake/make_scenario_trace.py'

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=10 >trace_jobs_test.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=1000 \
               --repeat=1 \
               --slave_count=3000 >trace_jobs_1000.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 \
               --repeat=1 \
               --slave_count=10 >trace_jobs_100.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 \
               --repeat=1 \
               --slave_count=100 >trace_jobs_100_100.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 \
               --repeat=1 \
               --slave_count=2 >trace_jobs_100_2.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 \
               --repeat=1 \
               --slave_count=1 >trace_jobs_100_1.json

#$MAKE_SCENARIO --max_jobs=10 >trace_jobs_test.json

#$MAKE_SCENARIO --priority=0 --priority=1 --priority=2 --priority=3 \
#               --priority=4 --priority=5 --priority=6 --priority=7 \
#               --priority=8 \
#               --scheduling_class=0 --scheduling_class=1 \
#               --max_jobs=1000 >trace_jobs_test_big.json
