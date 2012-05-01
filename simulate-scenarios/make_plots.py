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
parser.add_argument('--y', default='batch0_finish_time')
parser.add_argument('--plot_extra', default='times')

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
  try:
    if len(data) <= 1:
      return None
  except TypeError:
    return None
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
  if xformed is None:
    return
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

def add_plot(item, label, color):
  try:
    data = load_file(args.base + '/' + item + '.csv')
    make_error_bars(data, args.var, args.y, color=color,
        label=label)
  except IOError, e:
    pass

pyp.figure()
add_plot('norequest', 'norequest', 'red')
add_plot('norequest-aggressive', 'norequest (aggressive reoffer)', 'purple')
add_plot('simple-strong', 'simple, strong isolation', 'green')
add_plot('simple-weak', 'simple, weak isolation', 'blue')
pyp.legend()
pyp.savefig('plots/' + args.base + '/' + args.plot_extra + '-plot.pdf')

