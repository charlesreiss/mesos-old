set -x
set -e

MAKE_SCENARIO='python ../src/fake/make_scenario_trace.py'

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=1 --slave_count=1 \
               --min_finish_ratio=0 --min_tasks=1 >trace_k0_1_s1.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=10 --slave_count=2 \
               --min_finish_ratio=0 --min_tasks=1 >trace_k0_5_s1.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=500 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_100_s500.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=500 --slave_count=500 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_200_s1000.json


$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=500 --slave_count=1000 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_200_s500.json


$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=500 --slave_count=500 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_500_s500.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=100 \
               --min_finish_ratio=0 --min_tasks=2 >trace_k1_100_s100.json
if false; then
$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=25 --slave_count=100 \
               --min_finish_ratio=0 --min_tasks=2 >trace_k1_25_s100.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=25 --slave_count=2 \
               --min_finish_ratio=0 \
               --preserve_one_task=false >trace_k_25_s2.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=25 --slave_count=100 \
               --min_finish_ratio=0 >trace_k_25_s100.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=25 --slave_count=4000 \
               --min_finish_ratio=0 >trace_k_25_s4000.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=100 \
               --min_finish_ratio=0 >trace_k_100_s100.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=100 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_100_s100.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=50 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_100_s50.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=150 \
               --min_finish_ratio=0 \
               --task_split=false >trace_nosplit_k_100_s150.json

fi
if false; then
$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=50 --slave_count=200 >trace_50_s200.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=200 >trace_100_s200.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=400 >trace_100_s400.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 --slave_count=800 >trace_100_s800.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=200 --slave_count=200 >trace_200_s200.json


$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=200 --slave_count=800 >trace_200_s800.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=800 --slave_count=1600 >trace_800_s1600.json
fi

if false; then
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
               --slave_count=2 --slave_memory=0.25 >trace_jobs_100_2_lowmem.json

$MAKE_SCENARIO --priority=2 --priority=3 \
               --priority=4 --priority=5 --priority=6 --priority=7 \
               --priority=8 \
               --scheduling_class=0 --scheduling_class=1 \
               --max_jobs=100 \
               --repeat=1 \
               --slave_count=1 >trace_jobs_100_1.json
fi

#$MAKE_SCENARIO --max_jobs=10 >trace_jobs_test.json

#$MAKE_SCENARIO --priority=0 --priority=1 --priority=2 --priority=3 \
#               --priority=4 --priority=5 --priority=6 --priority=7 \
#               --priority=8 \
#               --scheduling_class=0 --scheduling_class=1 \
#               --max_jobs=1000 >trace_jobs_test_big.json
