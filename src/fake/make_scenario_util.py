import random
import math
import json

def round_exact(actual):
  return actual

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
      duration_func,
      memory_sample_func,
      cpu_sample_func,
      request_memory = None,
      request_cpu = None,
      target_seconds = None,
      target_memory_seconds = None,
      mean_duration_func = lambda: 1.0,
      memory_max = None,
      start_time = 0.0,
      stretch_time = 1.0,
      memory_round_func = round_exact,
      cpu_round_func = round_exact,
      sample_each_memory = False,
      sort_by_cpu_time = True,
      **ignored_args
  ):
    actual_memory = min(memory_max, memory_sample_func())
    if request_memory is None:
      request_memory = min(memory_max, memory_round_func(actual_memory))
    actual_cpu = cpu_sample_func()
    if request_cpu is None:
      request_cpu = cpu_round_func(actual_cpu)
    mean_time = mean_duration_func()
    def time_dist():
      return duration_func(mean_time)

    job = BatchJob(
      request='cpus:' + str(request_cpu) + ';mem:' + str(request_memory),
      const_resources='mem:' + str(actual_memory),
      max_cpus=str(actual_cpu),
      start_time=start_time,
    )

    if target_memory_seconds:
      total_secs = target_memory_seconds / actual_memory
      assert not sample_each_memory
    total_secs = 0.0
    tasks = []
    while total_secs < target_seconds:
      if sample_each_memory:
        actual_memory = sample_memory()
      if target_seconds:
        remaining_time = target_seconds - total_secs
        cpu_time = min(time_dist(), remaining_time + 10.0)
      total_secs += cpu_time
      task = {'cpu_time': cpu_time}
      if sample_each_memory:
        task['const_resources'] = 'mem:' + str(actual_memory)
      tasks.append(task)
    if sort_by_cpu_time:
      tasks = sorted(cpu_times, key='cpu_time', reverse=True)
    for task in tasks:
      job.add_task(**task)
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
  def __init__(self, serve_tasks, cpu_per_unit=1.0, **kwargs):
    GenericJob.__init__(self, **kwargs)
    self.for_json['cpu_per_unit'] = cpu_per_unit
    for i in xrange(serve_tasks):
      self.add_task()

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

