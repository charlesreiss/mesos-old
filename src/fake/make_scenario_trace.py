import random
import argparse
import numpy as np
import json
import sys
from make_scenario_util import *

parser = argparse.ArgumentParser()
parser.add_argument('--jobs_file', type=str,
    default='/home/eecs/charles/new_job_utils/new_job_utils.npy')
parser.add_argument('--jobs2_file', type=str,
    default='/home/eecs/charles/new_job_utils/new_job_utils2.npy')
parser.add_argument('--scheduling_class', action='append', type=int)
parser.add_argument('--priority', action='append', type=int)
parser.add_argument('--min_day', type=float, default=1.0)
parser.add_argument('--max_day', type=float, default=2.0)
parser.add_argument('--max_duration', type=float, default=0.5)
parser.add_argument('--max_fail_ratio', type=float, default=0.5)
parser.add_argument('--min_finish_ratio', type=float, default=0.5)
parser.add_argument('--min_portion', type=float, default=0.001)
parser.add_argument('--ignore_diffmachines', type=bool, default=True)
parser.add_argument('--scale_memory', type=float, default=1024.0)
parser.add_argument('--scale_cpu', type=float, default=16.0)
parser.add_argument('--scale_time', type=float, default=24.0 * 60.0 * 60.0)
parser.add_argument('--slave_count', type=int, default=10)
parser.add_argument('--max_jobs', type=int, default=None)
parser.add_argument('--repeat', type=int, default=1)
parser.add_argument('--slave_memory', type=float, default=0.5)
parser.add_argument('--slave_cpu', type=float, default=0.5)

parser.add_argument('--max_task_time_ratio', type=float, default=1.0)

DAY = 1000 * 1000 * 60 * 60 * 24

FACEBOOK_MAP_TASK_TIMES = map(lambda x: x/60./60./24., [
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
])

def diag(s):
  sys.stderr.write(s + '\n')

facebook_time_dist = empirical_dist(FACEBOOK_MAP_TASK_TIMES)

def mask_for(arr, field, values):
  mask = np.zeros(arr.shape, dtype=bool)
  for value in values:
    mask |= arr[field] == value
  assert ((mask != False).any())
  return mask

class JobConverter(object):
  def __init__(self, args):
    self.args = args

  def normalize_jobs(self, jobs):
    jobs['duration'] /= DAY
    jobs['start_time'] /= DAY
    jobs['end_time'] /= DAY
    jobs['running_time'] /= DAY
  
  def merge_jobs(self, jobs1, jobs2):
    jobs1.sort(order='name')
    jobs2.sort(order='name')
    assert (jobs1['name'] == jobs2['name']).all()
    jobs2['num_finish'] = jobs1['num_finish']
    jobs2['num_fail'] = jobs1['num_fail']
    jobs2.sort(order='start_time')
    return jobs2

  def accept_jobs(self, jobs):
    args = self.args
    mask = np.ones(jobs.shape, dtype=bool)
    assert ((mask != False).any())
    if args.priority is not None:
      mask &= mask_for(jobs, 'priority', args.priority)
    assert ((mask != False).any())
    if args.scheduling_class is not None:
      mask &= mask_for(jobs, 'scheduling_class', args.scheduling_class)
    assert ((mask != False).any())
    if args.min_day is not None:
      mask &= jobs['start_time'] >= args.min_day
    assert ((mask != False).any())
    if args.max_day is not None:
      mask &= jobs['end_time'] <= args.max_day
    assert ((mask != False).any())
    if args.max_duration is not None:
      mask &= jobs['duration'] <= args.max_duration
    assert ((mask != False).any())
    if args.max_fail_ratio is not None:
      mask &= jobs['num_fail'] / jobs['num_tasks'] <= args.max_fail_ratio
    assert ((mask != False).any())
    if args.min_finish_ratio is not None:
      mask &= jobs['num_finish'] / jobs['num_tasks'] >= args.min_finish_ratio
    assert ((mask != False).any())
    if args.ignore_diffmachines:
      # XXX: flipping bug
      mask &= jobs['different_machines'] != 0
    if args.min_portion:
      mask &= ((jobs['max_req_cpus'] > args.min_portion) &
               (jobs['max_req_memory'] > args.min_portion))
    assert ((mask != False).any())
    return jobs[mask]

  def convert_job(self, job, index=-1):
    max_time = job['duration'] * self.args.max_task_time_ratio
    def sample_time(ignored_duration):
      candidate = facebook_time_dist()
      while candidate > max_time:
        candidate = facebook_time_dist()
      return candidate * self.args.scale_time

    memory_req_limit = job['max_req_memory']
    # TODO: Strategies about removing startup usage/mem:0 tasks?
    memory_values = [
        (0.0, job['t0_pt99_mean_mem']),
        (.25, job['t25_pt99_mean_mem']),
        (.5, job['t50_pt99_mean_mem']),
        (.75, job['t75_pt99_mean_mem']),
        (.99, job['t99_pt99_mean_mem']),
        (1.0, job['tmax_pt99_mean_mem'])
    ]
    for i in xrange(len(memory_values)):
      memory_values[i] = (memory_values[i][0], min(memory_req_limit, memory_values[i][1]))
    cpu_values = [
        (0.0, job['t0_pt0_mean_cpu']),
        (.25, job['t25_pt25_mean_cpu']),
        (.5, job['t50_pt50_mean_cpu']),
        (.75, job['t75_pt75_mean_cpu']),
        (.99, job['t99_pt99_mean_cpu']),
        (1.0, job['tmax_pt99_mean_cpu'])
    ]
    def sample_table(table, scale, min_value=0.0):
      def sample():
        index = random.uniform(0, 1)
        table_index = 1
        while index > table[table_index][0]:
          table_index += 1
        offset = index - table[table_index - 1][0]
        offset /= (
            table[table_index][0] - table[table_index - 1][0]
        )
        value = (table[table_index - 1][1] * (1.0 - offset) +
                 table[table_index][1] * offset) * scale
        return max(min_value, value)
      return sample

    sample_memory = sample_table(memory_values, self.args.scale_memory)
    sample_cpu = sample_table(cpu_values, self.args.scale_cpu, 0.001)
    max_cpus = job['t99_pt99_mean_cpu'] * self.args.scale_cpu
    memory_request = job['max_req_memory'] * self.args.scale_memory
    memory_request = max(memory_request, job['tmax_pt99_mean_mem'] *
        self.args.scale_memory)

    if job['running_time'] > 0.2:
      diag(('Converting job %(index)d that takes more than 10%% of a day\n'
            '%(duration)s (duration) * %(tasks)s (tasks) = %(running_time)s\n'
            '... * %(memory_request)s (memory) = %(mem_time)s\n') %
            { 
              'index': index,
              'duration': job['duration'],
              'running_time': job['running_time'],
              'tasks': job['num_tasks'],
              'memory_request': memory_request,
              'mem_time': memory_request * job['running_time'],
            })

    return BatchJob.sample(
        duration_func = sample_time, 
        memory_sample_func = sample_memory,
        cpu_sample_func = sample_cpu,
        #cpu_sample_func = constant_dist(max_cpus),
        sort_by_cpu_time = False,
        sample_each_memory = True,
        sample_each_cpu = True,
        target_seconds = job['running_time'] * self.args.scale_time,
        memory_max = self.args.scale_memory * .99,
        request_memory = memory_request,
        request_cpu = job['max_req_cpus'] * self.args.scale_cpu,
        start_time = (job['start_time'] - self.args.min_day) * self.args.scale_time
    )
  
  def sample_jobs(self):
    jobs1 = np.load(args.jobs_file)
    jobs2 = np.load(args.jobs2_file)
    jobs = self.merge_jobs(jobs1, jobs2)
    self.normalize_jobs(jobs)
    filtered_jobs = self.accept_jobs(jobs)
    if self.args.max_jobs is not None:
      filtered_jobs = filtered_jobs[0:self.args.max_jobs]
    assert len(filtered_jobs) > 0
    return map(lambda x: self.convert_job(filtered_jobs[x], index=x), xrange(len(filtered_jobs)))
  
def gen_scenario(args, seed):
  random.seed(seed)
  converter = JobConverter(args)
  jobs = converter.sample_jobs()
  def make_slave(i):
    return Slave(resources='mem:' + str(args.slave_memory * args.scale_memory) + ';cpus:' + str(args.scale_cpu))

  slaves = map(make_slave, xrange(args.slave_count))

  assert len(jobs) > 0

  return Scenario(slaves=slaves, batch_jobs=jobs, serve_jobs=[],
      label='%s' % (seed), label_cols='seed')

if __name__ == '__main__':
  args = parser.parse_args()
  for i in xrange(args.repeat):
    scenario = gen_scenario(args, i)
    print scenario.dump()
    print ""
