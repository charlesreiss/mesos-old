#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pylab as pyp
import argparse
import json

import parse_usage 

parser = argparse.ArgumentParser()

def build_args_ext(parser):
  parser.add_argument('--scale_memory', type=float, default=512.0)
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


def utilization_from(record, framework_names, slave_names):
  total_cpu = 0.0
  total_memory = 0.0
  total_req_cpu = 0.0
  total_req_memory = 0.0
  machine_tasks = {}
  for name in slave_names:
    machine_tasks[name] = 0
  framework_cpu = {}
  framework_memory = {}
  for name in framework_names:
    framework_cpu[name] = 0.0
    framework_memory[name] = 0.0
  total_tasks = 0

  for usage in record.usage:
    end = min(usage.timestamp, record.max_expect_timestamp)
    start = max(usage.timestamp - usage.duration, record.min_expect_timestamp)
    effective_duration = end - start
    framework = framework_id(usage)
    machine = usage.slave_id.value
    machine_tasks[machine] += 1
    total_tasks += 1
    for resource in usage.resources:
      if resource.name == 'cpus':
        cpu = resource.scalar.value * effective_duration / args.scale_cpu
        total_cpu += cpu
        framework_cpu[framework] += cpu
      elif resource.name == 'mem':
        memory = resource.scalar.value * effective_duration / args.scale_memory
        total_memory += memory
        framework_memory[framework] += memory
    for resource in usage.expected_resources:
      if resource.name == 'cpus':
        cpu = resource.scalar.value * effective_duration / args.scale_cpu
        total_req_cpu += cpu
      elif resource.name == 'mem':
        memory = resource.scalar.value * effective_duration / args.scale_memory
        total_req_memory += memory

  result = [total_tasks, total_cpu, total_memory, total_req_cpu, total_req_memory]
  for name in framework_names:
    result.append(framework_cpu[name])
    result.append(framework_memory[name])
  for name in slave_names:
    result.append(machine_tasks[name])
  return result



def read_file(name):
  is_binary = False
  try:
    stream = open(name, 'r')
  except IOError as e:
    is_binary = True
    stream = open(name + '.bin', 'r')
  # Pass 0, find framework names
  framework_names = set()
  slave_names = set()
  shown_example = False
  for record in parse_usage.usage_from_stream(stream, is_binary):
    for usage in record.usage:
      if not shown_example:
        print "example = ", usage
        shown_example = True
      framework_names.add(framework_id(usage))
      slave_names.add(usage.slave_id.value)
  stream.seek(0)
  data = []
  for record in parse_usage.usage_from_stream(stream, is_binary):
    values = tuple(utilization_from(record, framework_names, slave_names))
    assert len(values) == ((len(framework_names) + 1) * 2 + (len(slave_names) +
      1) + 2)
    data.append(values)
  stream.close()
  datatypes = [('tasks', float), ('cpu', float), ('memory', float),
               ('req_cpu', float), ('req_memory', float)]
  for name in framework_names: 
    datatypes += [('cpu_' + name, float), ('memory_' + name, float)]
  i = 0
  for name in slave_names:
    datatypes += [('tasks_slave' + str(i), float)]
    i += 1
  print "datatypes = ", datatypes
  assert len(datatypes) == len(data[0])
  return (list(framework_names), list(slave_names), np.array(data, dtype=datatypes))

if __name__ == '__main__':
  build_args(parser)
  args = parser.parse_args()
  pyp.figure(1000)
  framework_names = None
  for label_name in args.input:
    (label, name) = label_name.split(':')
    (framework_names, slave_names, data) = read_file(name)
    framework_names = sorted(framework_names)
    slave_names = sorted(slave_names)
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
  for name in framework_names[0:3]:
    pyp.figure(i)
    i += 1
    pyp.ylim(0, max(data['cpu']))
    pyp.legend()
    pyp.savefig(args.out_base + '-' + name + '.pdf')

