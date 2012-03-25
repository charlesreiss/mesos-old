MAKE_SCENARIO='python ../src/fake/make_scenario.py --repeat=5'

$MAKE_SCENARIO --vary_memory_round --num_background=1 --slaves=1 \
  --cpu_max=1.0 --cpu_request=1.0 --serve_tasks=1 --num_serves=1 \
  >vm_serve.json

$MAKE_SCENARIO --vary_memory_round --num_background=4 --slaves=1 \
  --cpu_max=1.0 --cpu_request=1.0 --serve_tasks=1 --num_serves=1 \
  >vm_4_4_serve.json

$MAKE_SCENARIO --vary_memory_round --num_background=4 --slaves=2 \
  --cpu_max=1.0 --cpu_request=1.0 --serve_tasks=1 --num_serves=1 \
  >vm_4_4_serve_2m.json

$MAKE_SCENARIO --vary_memory_round --num_background=16 --slaves=4 \
  --cpu_max=1.0 --cpu_request=1.0 --serve_tasks=3 --num_serves=1 \
  >vm_4_16_serve.json

$MAKE_SCENARIO --vary_memory_round --num_background=40 --slaves=10 \
  --target_memory_seconds=2000 --interarrival=400 \
  --cpu_max=1.0 --cpu_request=1.0 \
  --serve_tasks=10 --num_serves=1 \
  >vm_10_interarrive_serve.json

$MAKE_SCENARIO --vary_memory_round --num_background=40 --slaves=10 \
  --target_memory_seconds=2000 --interarrival=400 \
  --cpu_max=1.0 --cpu_request=1.0 \
  >vm_10_interarrive.json


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

