import json
import random
import math
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--memory_low', default=False, type=bool,
                    help='Low-ball memory requests.')
parser.add_argument('--memory_accuracy', default=5.0, type=float,
                    help='Round interval for memory requests.')
parser.add_argument('--cpu_request', default=2.0, type=float,
                    help='CPU requested')
parser.add_argument('--cpu_max', default=2.0, type=float,
                    help='Maximum CPU used')
parser.add_argument('--memory_max', default=40.0, type=float,
                    help='')
parser.add_argument('--experiment_memory', default=8.0, type=float,
                    help='')
parser.add_argument('--target_memory_seconds', default=5000, type=float,
                    help='')
parser.add_argument('--repeat', default=10, type=int,
                    help='reptitions per scenario')
parser.add_argument('--num_background', default=4, type=int,
                    help='number of background jobs')
parser.add_argument('--slaves', default=4, type=int, help='number of slaves')
parser.add_argument('--stretch_time', default=1.0, type=float)
parser.add_argument('--vary_memory', action='store_true', default=False)
parser.add_argument('--vary_memory_round', action='store_true', default=False)
parser.add_argument('--vary_cpu', action='store_true', default=False)
parser.add_argument('--use_experiment', action='store_true',
                    default=False)
parser.add_argument('--max_offset', default=10, type=int)
parser.add_argument('--interarrival', default=0.0, type=float)
parser.add_argument('--start_experiment', default=0.0, type=float)

parser.add_argument('--serve_pattern', 
    default='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,' +
            '19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1',
    type=str)
parser.add_argument('--serve_time_unit', default=1.0, type=float)
parser.add_argument('--num_serves', default=0, type=int)
parser.add_argument('--serve_tasks', default=10, type=int)
parser.add_argument('--serve_memory', default=4.0, type=float)
parser.add_argument('--serve_request', default='cpus:2.0;mem:4')
parser.add_argument('--serve_cpu_unit', default=0.1, type=float)

args = parser.parse_args()


FACEBOOK_MAP_TASK_TIMES = [
     0, 1.383, 1.84667, 2.52, 2.93333, 3.2685,
    3.54867, 3.795, 4.021, 4.238, 4.444,
    4.6375, 4.8285, 5.018, 5.208, 5.402,
    5.604, 5.806, 6.016, 6.233,
    6.451, 6.672, 6.9, 7.14, 7.381, 7.628, 7.883, 8.14413, 8.412, 8.696,
    8.99261, 9.30167, 9.62833, 9.97, 10.337, 10.727, 11.1424, 11.592, 12.0697,
    12.609, 13.188, 13.8332, 14.543, 15.344, 16.2111, 17.1773, 18.2476, 19.407,
    20.6632, 22.0192, 23.4455, 24.989, 26.637, 28.4077, 30.3735, 32.5267,
    34.85, 37.3541, 39.9654, 42.3884, 44.6953, 46.823, 48.889, 50.8536,
    52.8298, 54.8569, 56.9627, 59.1125, 61.302, 63.5706, 65.937, 68.352,
    70.8484, 73.423, 76.1751, 79.0954, 82.422, 86.0247, 90.038, 94.5318,
    99.6733, 105.674, 112.681, 120.947, 131.24,
    143.929, 159.658, 177.267, 197.906, 223.516,
    251.726, 285.147, 327.086, 380.764, 457.387,
    576.176, 800.652, 1530.172, 7129.003, 283749
]

def sample_memory():
  return random.lognormvariate(2.0, math.sqrt(2.0))

def sample_cpu():
  return args.cpu_max

def round_exact(actual):
  return actual

def make_round_memory(factor):
  def result(actual):
    return (int(actual/factor) + 1.0) * factor
  return result

def empirical_dist(quantiles):
  def result():
    index = random.uniform(0, len(quantiles) - 1)
    fraction = index - int(index)
    low = quantiles[int(index)]
    high = quantiles[int(index) + 1]
    return low * (1.0 - fraction) + high * fraction
  return result


sample_time = empirical_dist(FACEBOOK_MAP_TASK_TIMES)

def sample_time_dist(mean):
  return random.expovariate(1.0 / mean)

class GenericJob(object):
  def __init__(self, request, const_resources, start_time = 0.0):
    self.for_json = {
        'request': request,
        'const_resources': const_resources,
        'tasks': {},
        'start_time': start_time,
    }

  def add_task(self, **kw):
    next_task_id = 't' + str(len(self.for_json['tasks']))
    self.for_json['tasks'][next_task_id] = kw

  def json_object(self):
    return self.for_json

class BatchJob(GenericJob):
  @staticmethod
  def sample(
      memory_sample_func = sample_memory,
      cpu_sample_func = sample_cpu,
      memory_round_func = round_exact,
      cpu_round_func = round_exact,
      mean_duration_func = sample_time,
      duration_func = sample_time_dist,
      target_memory_seconds = 1000.0,
      stretch_time = 1.0,
      memory_max = 40.0,
      start_time = 0.0,
      **ignored_args
  ):
    actual_memory = min(memory_max, memory_sample_func())
    request_memory = min(memory_max, memory_round_func(actual_memory))
    actual_cpu = sample_cpu()
    request_cpu = cpu_round_func(actual_cpu)
    mean_time = mean_duration_func()
    def time_dist():
      return duration_func(mean_time)

    job = BatchJob(
      request='cpus:' + str(request_cpu) + ';mem:' + str(request_memory),
      const_resources='mem:' + str(actual_memory),
      max_cpus=actual_cpu,
      start_time=start_time,
    )
    total_mem_secs = 0.0
    cpu_times = []
    while total_mem_secs < target_memory_seconds:
      cpu_time = time_dist()
      total_mem_secs += actual_memory * cpu_time
      cpu_times.append(cpu_time)
    for cpu_time in sorted(cpu_times, reverse=True):
      job.add_task(cpu_time = cpu_time * stretch_time)
    return job

    
  def __init__(self, request, const_resources, 
               max_cpus, start_time = 0.0):
    GenericJob.__init__(self, request, const_resources, start_time)
    self.for_json['max_cpus'] = max_cpus 


  def add_tasks_dist(self, size_dist, task_length_dist):
    all_tasks = {}
    num_tasks = size_dist()
    for i in xrange(num_tasks):
      task = {
          'cpu_time': task_length_dist()
      }
      all_tasks['t' + str(i)] = task
    self.for_json['tasks'] = all_tasks

  def set_resources(self, constant, request):
    self.for_json['const_resources'] = constant
    self.for_json['request'] = request

  def set_max_cpus(self, max_cpus):
    self.for_json['max_cpus'] = max_cpus
  
class ServeJob(GenericJob):
  def __init__(self, cpu_per_unit=1.0, **kwargs):
    GenericJob.__init__(self, **kwargs)
    self.for_json['cpu_per_unit'] = cpu_per_unit

  def set_pattern(self, pattern_list, duration=1.0):
    self.for_json['counts'] = pattern_list
    self.for_json['time_per_count'] = duration

class Slave(object):
  def __init__(self, resources):
    self.for_json = {
        'resources': resources
    }

  def json_object(self):
    return self.for_json

class Scenario(object):
  def __init__(self, slaves, batch_jobs, serve_jobs, label='', label_cols=''):
    self.slaves = slaves
    self.batch_jobs = batch_jobs
    self.serve_jobs = serve_jobs
    self.label = label
    self.label_cols = label_cols

  def json_object(self):
    batch_object = {}
    for i in xrange(len(self.batch_jobs)):
      batch_object['batch' + str(i)] = self.batch_jobs[i].json_object()
    serve_object = {}
    for i in xrange(len(self.serve_jobs)):
      serve_object['serve' + str(i)] = self.serve_jobs[i].json_object()
    return {
        'slaves': map(lambda s: s.json_object(), self.slaves),
        'batch': batch_object,
        'serve': serve_object,
        'label': self.label,
        'label_cols': self.label_cols,
    }

  def dump(self):
    return json.dumps(self.json_object())


def constant_dist(x):
  def result():
    return x
  return result
def sample_size():
  return int(random.expovariate(1.0/10.0))

def make_jobs(args):
  fixed_jobs = map(sample_batch_job, xrange(args.num_background))

random.seed(42)

def make_scenario(offset):
  random.seed(42)
  start_times = []
  last_time = 0.0
  for i in xrange(args.num_background):
    start_times.append(last_time)
    if args.interarrival > 0.0:
      last_time += random.expovariate(1.0 / args.interarrival)

  def sample_one(start_time, is_experiment=False):
    myargs = args
    if is_experiment:
      myargs.memory_sample_func = lambda ignored: args.experiment_memory
    else:
      myargs.memory_sample_func = sample_memory
    if is_experiment or not myargs.use_experiment:
      if args.vary_memory:
        if args.memory_low:
          myargs.memory_round_func = lambda x: (0.5 + offset / 5.0) * x
        else:
          myargs.memory_round_func = lambda x: (1.0 + offset / 5.0) * x
      elif args.vary_memory_round:
        FACTOR = 0.25 * (offset + 1)
        myargs.memory_round_func = make_round_memory(FACTOR)
      elif args.vary_cpu:
        myargs.cpu_round_func = lambda x: (0.5 + offset / 5.0) * x
    return BatchJob.sample(**vars(myargs))

  fixed_jobs = map(lambda start_time: sample_one(start_time=start_time), start_times)
  experiment_jobs = []
  if args.use_experiment:
    experiment_jobs = map(lambda ignore: sample_one(True), [0])

  serve_jobs = []
  for i in xrange(args.num_serves):
    serve_jobs.append(ServeJob(request=args.serve_request,
        const_resources='mem:' + str(args.serve_memory),
        start_time=0.0,
        cpu_per_unit=args.serve_cpu_unit))
    serve_jobs[-1].set_pattern(args.serve_pattern.split(','),
        args.serve_time_unit)

  return Scenario(
            slaves = map(lambda x: Slave(resources='mem:40;cpus:8.0'),
              xrange(args.slaves)),
            batch_jobs = fixed_jobs + experiment_jobs,
            serve_jobs = serve_jobs,
            label='%s' % (offset),
            label_cols='offset',
  )

for i in xrange(args.max_offset):
  scenario = make_scenario(i)
  for j in xrange(args.repeat):
    print(scenario.dump())
    print ""

