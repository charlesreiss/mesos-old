
REPEAT=50
#python ../src/fake/make_scenario.py \
#    --vary_cpu --experiment_memory=5 --cpu_request=2 >vary_cpu_simple.json
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --cpu_request=2 >vary_mem_simple.json
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --stretch_time=10 --cpu_request=2 >vary_mem_simple_stretch.json

python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --memory_accuracy=0.1 --cpu_request=2 \
  >vary_mem_simple_acc.json
python ../src/fake/make_scenario.py --repeat=$REPEAT \
  --vary_memory --experiment_memory=5 --memory_accuracy=0.1 --stretch_time=10 \
  --cpu_request=2 >vary_mem_simple_stretch_acc.json
