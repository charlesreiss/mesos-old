#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pylab as pyp
import argparse

import parse_usage 

parser = argparse.ArgumentParser()

def build_args_ext(parser):
  parser.add_argument('--scale_memory', type=float, default=1024.0)
  parser.add_argument('--scale_cpu', type=float, default=16.0)

def build_args(parser):
  build_args_ext(parser)
  parser.add_argument('input', type=str, metavar='LABEL:FILE', nargs='+')
  parser.add_argument('--out_base', type=str, default='out')
  parser.add_argument('--use_memory', action='store_true', default=False)
  parser.add_argument('--use_cpu', action='store_true', default=False)

args = None

def framework_id(usage):
  return str(usage.framework_id.value)

def utilization_from(record, framework_names):
  total_cpu = 0.0
  total_memory = 0.0
  framework_cpu = {}
  framework_memory = {}
  for name in framework_names:
    framework_cpu[name] = 0.0
    framework_memory[name] = 0.0

  for usage in record.usage:
    end = min(usage.timestamp, record.max_expect_timestamp)
    start = max(usage.timestamp - usage.duration, record.min_expect_timestamp)
    effective_duration = end - start
    framework = framework_id(usage)
    for resource in usage.resources:
      if resource.name == 'cpus':
        cpu = resource.scalar.value * effective_duration / args.scale_cpu
        total_cpu += cpu
        framework_cpu[framework] += cpu
      elif resource.name == 'mem':
        memory = resource.scalar.value * effective_duration / args.scale_memory
        total_memory += memory
        framework_memory[framework] += memory
  result = [total_cpu, total_memory]
  for name in framework_names:
    result.append(framework_cpu[name])
    result.append(framework_memory[name])
  return result



def read_file(name):
  stream = open(name, 'r')
  # Pass 0, find framework names
  names = set()
  for record in parse_usage.usage_from_stream(stream):
    for usage in record.usage:
      names.add(framework_id(usage))
  stream.seek(0)
  data = []
  for record in parse_usage.usage_from_stream(stream):
    values = tuple(utilization_from(record, names))
    assert len(values) == (len(names) + 1) * 2
    data.append(values)
  stream.close()
  datatypes = [('cpu', float), ('memory', float)]
  for name in names: 
    datatypes += [('cpu_' + name, float), ('memory_' + name, float)]
  print "datatypes = ", datatypes
  assert len(datatypes) == len(data[0])
  return (list(names), np.array(data, dtype=datatypes))

if __name__ == '__main__':
  build_args(parser)
  args = parser.parse_args()
  pyp.figure(1000)
  framework_names = None
  for label_name in args.input:
    (label, name) = label_name.split(':')
    (framework_names, data) = read_file(name)
    framework_names = sorted(framework_names)
    np.save(args.out_base + '-' + label + '-data.npy', data)
    pyp.figure(1000)
    if args.use_cpu:
      pyp.plot(data['cpu'], label=label + ' cpu')
    if args.use_memory:
      pyp.plot(data['memory'], label=label + ' memory')
    i = 0
    for name in framework_names[0:3]:
      pyp.figure(i)
      i += 1
      if args.use_cpu:
        pyp.plot(data['cpu_' + name], label=label + ' cpu for ' + name)
      if args.use_memory:
        pyp.plot(data['memory_' + name], label=label + ' memory for ' + name)

  pyp.figure(1000)
  pyp.ylim(0, max(data['cpu']))
  pyp.legend()
  pyp.savefig(args.out_base + '.pdf')
  i = 0
  for name in framework_names:
    pyp.figure(i)
    i += 1
    pyp.ylim(0, max(data['cpu']))
    pyp.legend()
    pyp.savefig(args.out_base + '-' + name + '.pdf')

