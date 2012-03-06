#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pylab as pyp
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--base')
parser.add_argument('--var', default='estimate_mem')

args = parser.parse_args()

def load_file(filename):
  return np.genfromtxt(filename, delimiter=',', names=True)

def error_bars(data, x_var, y_vars, low_percentile=25., high_percentile=75.):
  x_values = np.unique(data[x_var])
  names = [x_var]
  for y_var in y_vars:
    names += [y_var + '_min',
              y_var + '_low',
              y_var + '_median',
              y_var + '_high',
              y_var + '_max']

  result = []
  for x_value in x_values:
    y_values = data[data[x_var] == x_value]
    line = [x_value]
    for y_var in y_vars:
      line += np.percentile(y_values[y_var],
          [0., low_percentile, 50., high_percentile, 100.])
    line = tuple(line)
    print "line = ", line
    result.append(line)

  print "names = ", names

  return np.array(result, dtype={'names':names,
      'formats':[np.float]*len(names)})

def make_error_bars(data, x_var, y_var, **plot_options):
  xformed = error_bars(data, x_var, [y_var])
  print "transformed = ", xformed
  y_low = xformed[y_var + '_median'] - xformed[y_var + '_low']
  y_high = xformed[y_var + '_high'] - xformed[y_var + '_median']
  pyp.errorbar(xformed[x_var], xformed[y_var + '_median'],
      yerr=[y_low, y_high],
      **plot_options)

# FIXME: broken
def make_boxplot(data, x_var, y_var, **plot_options):
  x_values = np.unique(data[x_var])
  result = []
  for x_value in x_values:
    y_values = data[data[x_var] == x_value]
    result.append(y_values)

  pyp.boxplot(y_values, positions=x_values, **plot_options)

norequest = load_file(args.base + '/norequest.csv')
simple_strong = load_file(args.base + '/simple-strong.csv')
simple_weak = load_file(args.base + '/simple-weak.csv')
pyp.figure()
make_error_bars(norequest, args.var, 'batch0_finish_time', color='red',
    label='norequest')
make_error_bars(simple_strong, args.var, 'batch0_finish_time',
    color='green', label='simple, strong isolation')
make_error_bars(simple_weak, args.var, 'batch0_finish_time',
    color='blue', label='simple, weak isolation')
#make_error_bars(norequest, 'estimate_cpu', 'batch0_finish_time', color='red',
#    label='norequest')
#make_error_bars(simple_strong, 'estimate_cpu', 'batch0_finish_time',
#color='green', label='normal')
#make_error_bars(simple_weak, 'estimate_cpu', 'batch0_finish_time',
#color='blue', label='normal')
pyp.legend()
pyp.savefig(args.base + '-plot.pdf')

