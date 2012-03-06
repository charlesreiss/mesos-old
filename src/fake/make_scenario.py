import json
import random
import math
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--memory_low', default=False, type=bool,
                    help='Low-ball memory requests.')
parser.add_argument('--memory_accuracy', default=5.0, type=float,
                    help='Round interval for memory requests.')
parser.add_argument('--cpu_request', default=1.0, type=float,
                    help='CPU requested')
parser.add_argument('--cpu_max', default=2.0, type=float,
                    help='Maximum CPU used')
parser.add_argument('--experiment_cpu_request', default=2.0, type=float,
                    help='')
parser.add_argument('--experiment_cpu_max', default=2.0, type=float,
                    help='')
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
parser.add_argument('--vary_cpu', action='store_true', default=False)

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


class BatchJob(object):
  def __init__(self, request, const_resources, 
               max_cpus):
    self.for_json = {
        'request': request,
        'const_resources': const_resources,
        'max_cpus': max_cpus,
        'tasks': {},
    }

  def set_label(self,label):
    self.for_json['label'] = label

  def add_tasks_dist(self, size_dist, task_length_dist):
    all_tasks = {}
    num_tasks = size_dist()
    for i in xrange(num_tasks):
      task = {
          'cpu_time': task_length_dist()
      }
      all_tasks['t' + str(i)] = task
    self.for_json['tasks'] = all_tasks

  def add_task(self, cpu_time):
    next_task_id = 't' + str(len(self.for_json['tasks']))
    self.for_json['tasks'][next_task_id] = { 'cpu_time': cpu_time }

  def set_resources(self, constant, request):
    self.for_json['const_resources'] = constant
    self.for_json['request'] = request

  def set_max_cpus(self, max_cpus):
    self.for_json['max_cpus'] = max_cpus
  
  def json_object(self):
    return self.for_json


class Slave(object):
  def __init__(self, resources):
    self.for_json = {
        'resources': resources
    }

  def json_object(self):
    return self.for_json

class Scenario(object):
  def __init__(self, slaves, batch_jobs, label='', label_cols=''):
    self.slaves = slaves
    self.batch_jobs = batch_jobs
    self.label = label
    self.label_cols = label_cols

  def json_object(self):
    batch_object = {}
    for i in xrange(len(self.batch_jobs)):
      batch_object['batch' + str(i)] = self.batch_jobs[i].json_object()
    return {
        'slaves': map(lambda s: s.json_object(), self.slaves),
        'batch': batch_object,
        'label': self.label,
        'label_cols': self.label_cols,
    }

  def dump(self):
    return json.dumps(self.json_object())


def constant_dist(x):
  def result():
    return x
  return result

def empirical_dist(quantiles):
  def result():
    index = random.uniform(0, len(quantiles) - 1)
    fraction = index - int(index)
    low = quantiles[int(index)]
    high = quantiles[int(index) + 1]
    return low * (1.0 - fraction) + high * fraction
  return result

def sample_memory():
  return random.lognormvariate(4.0, math.sqrt(4.0))


TIME_DIST = empirical_dist(FACEBOOK_MAP_TASK_TIMES)

def sample_batch_job(ignore_id, set_memory=None):
  actual_memory = min(args.memory_max, sample_memory())
  if set_memory is not None :
    actual_memory = set_memory
  request_memory = 0.0
  if args.memory_low:
    request_memory = args.memory_max * 0.9
  else:
    request_memory = min(args.memory_max,
        (int(actual_memory/args.memory_accuracy) + 1)*args.memory_accuracy)
  mean_time = TIME_DIST()
  def time_dist():
    return random.expovariate(1.0/mean_time)
  job = BatchJob(
      request='cpus:' + str(args.cpu_request) + ';mem:' + str(request_memory),
      const_resources='mem:' + str(actual_memory),
      max_cpus=args.cpu_max,
  )
  total_mem_secs = 0.0
  cpu_times = []
  while total_mem_secs < args.target_memory_seconds:
    cpu_time = time_dist()
    total_mem_secs += actual_memory * cpu_time
    cpu_times.append(cpu_time)
  for cpu_time in sorted(cpu_times, reverse=True):
    job.add_task(cpu_time = cpu_time * args.stretch_time)
  return job

def sample_size():
  return int(random.expovariate(1.0/10.0))

random.seed(42)
fixed_jobs = map(sample_batch_job, xrange(args.num_background))
extra_job = sample_batch_job(-1, set_memory=args.experiment_memory)

def print_scenario(estimate_mem, estimate_cpu):
  extra_job.set_resources(
      constant = 'mem:%(mem)s' % { 'mem': args.experiment_memory },
      request = 'mem:%(mem)s;cpus:%(cpus)s' % { 'mem': estimate_mem, 'cpus':
        estimate_cpu
      })
  extra_job.set_max_cpus(args.experiment_cpu_max)
  jobs = [extra_job] + fixed_jobs
  for i in xrange(args.repeat):
    print Scenario(
            slaves = map(lambda x: Slave(resources='mem:40;cpus:8.0'),
              xrange(args.slaves)),
            batch_jobs = jobs,
            label='%s,%s,%s,%s' % (estimate_mem, estimate_cpu, args.cpu_request,
              args.memory_accuracy),
            label_cols='estimate_mem,estimate_cpu,other_estimate_cpu,memory_accuracy',
          ).dump()
    print ""

if args.vary_memory or args.vary_cpu:
  for run in xrange(11):
    estimate_mem = args.experiment_memory
    if args.vary_memory:
      if args.memory_low:
        estimate_mem = (0.9 + run / 5.0) * args.experiment_memory
      else:
        estimate_mem = (1.0 + run / 5.0) * args.experiment_memory
    estimate_cpu = args.experiment_cpu_request
    if args.vary_cpu:
      estimate_cpu = ((run + 1) / 5.0) * args.experiment_cpu_max
    print_scenario(estimate_mem, estimate_cpu)

print_scenario(args.experiment_memory, args.experiment_cpu_max)
