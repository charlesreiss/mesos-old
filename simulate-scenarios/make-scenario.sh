MAKE_SCENARIO='python ../src/fake/make_scenario.py --repeat=20'

if false; then
    $MAKE_SCENARIO --vary_memory_round --num_background=2 --slaves=1 \
  >vary_memory_round_noexp_2.json

$MAKE_SCENARIO --vary_memory_round --num_background=3 --slaves=1 \
  >vary_memory_round_noexp_3.json

$MAKE_SCENARIO --vary_memory_round --num_background=4 --slaves=1 \
  >vary_memory_round_noexp_4.json

fi

$MAKE_SCENARIO --vary_memory_round --num_background=4 --slaves=1 \
  --cpu_max=1.0 --cpu_request=1.0 \
  >vary_memory_round_noexp_4_cpu1.json

if false; then
  $MAKE_SCENARIO --vary_memory_round --num_background=4 --slaves=10 \
    --target_memory_seconds=20000 \
    >vary_memory_round_noexp_long.json
fi

  $MAKE_SCENARIO --vary_memory_round --num_background=40 --slaves=10 \
    --target_memory_seconds=2000 --interarrival=400 \
    >vary_memory_round_interarrive_long2.json

exit


REPEAT=50
#python ../src/fake/make_scenario.py \
#    --vary_cpu --experiment_memory=5 --cpu_request=2 >vary_cpu_simple.json
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --cpu_request=2 >vary_mem_simple.json
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --stretch_time=10 --cpu_request=2 >vary_mem_simple_stretch.json

REPEAT=5
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --memory_accuracy=0.1 --cpu_request=2 \
  >vary_mem_simple_acc.json
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --memory_accuracy=0.1 --stretch_time=10 \
  --cpu_request=2 >vary_mem_simple_stretch_acc.json

REPEAT=5
$MAKE_SCENARIO --repeat=$REPEAT --num_background=0 --slaves=1 \
  --vary_memory > vary_mem_nobackground.json

REPEAT=5
$MAKE_SCENARIO --repeat=$REPEAT --num_background=0 --slaves=4 \
  --vary_memory > vary_mem_nobackground4.json

