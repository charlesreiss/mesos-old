#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pylab as pyp
import argparse

from usage_log import usage_log_pb2
from google.protobuf import text_format 

parser = argparse.ArgumentParser()
parser.add_argument('input', type=str, nargs='+', metavar='LABEL:FILE')
parser.add_argument('--scale_memory', type=float, default=40.0)
parser.add_argument('--scale_cpu', type=float, default=8.0)
parser.add_argument('--num_frameworks', type=int, default=3)
parser.add_argument('--out_base', type=str, default='out')
parser.add_argument('--use_memory', action='store_true', default=False)
parser.add_argument('--use_cpu', action='store_true', default=False)


args = parser.parse_args()

def base_record():
  return usage_log_pb2.UsageLogRecord()

def read_record(stream):
  line = stream.readline()
  if line == '':
    return None
  else:
    record = base_record()
    text_format.Merge(line, record)
    return record

def framework_id(usage):
  return int(usage.framework_id.value.split('-')[-1])

def utilization_from(record):
  total_cpu = 0.0
  total_memory = 0.0
  framework_cpu = map(lambda x: 0.0, xrange(args.num_frameworks))
  framework_memory = map(lambda x: 0.0, xrange(args.num_frameworks))

  for usage in record.usage:
    end = min(usage.timestamp, record.max_expect_timestamp)
    start = max(usage.timestamp - usage.duration, record.min_expect_timestamp)
    effective_duration = end - start
    for resource in usage.resources:
      if resource.name == 'cpus':
        cpu = resource.scalar.value * effective_duration / args.scale_cpu
        total_cpu += cpu
        framework_cpu[framework_id(usage)] += cpu
      elif resource.name == 'mem':
        memory = resource.scalar.value * effective_duration / args.scale_memory
        total_memory += memory
        framework_memory[framework_id(usage)] += memory
  result = [total_cpu, total_memory]
  for i in xrange(args.num_frameworks):
    result.append(framework_cpu[i])
    result.append(framework_memory[i])
  return result

def read_file(name):
  stream = open(name, 'r')
  data = []
  while True:
    record = read_record(stream)
    if record is None:
      break
    values = tuple(utilization_from(record))
    data.append(values)
  stream.close()
  datatypes = [('cpu', float), ('memory', float)]
  for i in xrange(args.num_frameworks):
    datatypes += [('cpu_' + str(i), float), ('memory_' + str(i), float)]
  return np.array(data, dtype=datatypes)

pyp.figure(1000)
for i in xrange(args.num_frameworks):
  pyp.figure(i)

for label_name in args.input:
  (label, name) = label_name.split(':')
  data = read_file(name)
  pyp.figure(1000)
  if args.use_cpu:
    pyp.plot(data['cpu'], label=label + ' cpu')
  if args.use_memory:
    pyp.plot(data['memory'], label=label + ' memory')
  for i in xrange(args.num_frameworks):
    pyp.figure(i)
    if args.use_cpu:
      pyp.plot(data['cpu_' + str(i)], label=label + ' cpu for ' + str(i))
    if args.use_memory:
      pyp.plot(data['memory_' + str(i)], label=label + ' memory for ' + str(i))

pyp.figure(1000)
pyp.ylim(0, 1.)
pyp.legend()
pyp.savefig(args.out_base + '.pdf')
for i in xrange(args.num_frameworks):
  pyp.figure(i)
  pyp.ylim(0, 1.)
  pyp.legend()
  pyp.savefig(args.out_base + '-' + str(i) + '.pdf')

