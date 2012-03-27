#!/usr/bin/python
import numpy as np
import argparse
import parse_usage
import utilization

parser = argparse.ArgumentParser()
def build_args(parser):
  parser.add_argument('--log_base', type=str, metavar='log')
  parser.add_argument('--csv', type=str, metavar='csv')
  parser.add_argument('--out_base', type=str)

def frameworks_from_data(data):
  def is_cpu(x):
    return x.startswith('cpu_')
  def framework_name(x):
    return x[4:]
  return map(framework_name, filter(is_cpu, data.dtype.names))

def map_dict(d, f):
  result = {}
  for key, value in d.iteritems():
    result[key] = f(value)
  return result


def fairness_from(log_file, csv_line):
  (names, data) = utilization.read_file(log_file)
  return fairness_from_util(data, csv_line)


def fairness_from_util(data, csv_line):
  def extract_framework(name):
    subset = data[['cpu_' + name, 'memory_' + name]]
    subset.dtype.names = ('cpu', 'memory')
    return subset
  def shares_for(max_shares):
    def result(subsets):
      result = np.array(subsets, dtype=map(lambda k: (k, float), max_shares.keys()))
      for key in max_shares.keys():
        result[key] /= max_shares[key]
      return np.array(result)
    return result
  def dominant_resource_from_share(shares):
    return shares[['cpu', 'memory']].copy().view((float, 2)).max(axis=1)
  def active_counts(names):
    result = []
    for i in xrange(len(data)):
      count = 0
      for name in names:
        if name.startswith('serve'):
          count += 1
        elif (csv_line[name + '_start_time'] <= i and
            csv_line[name + '_finish_time'] >= i):
          count += 1
      result.append(count)
    return np.array(result)
  def jain_fairness(shares, active):
    # FIXME we need to know when frameworks are active to do this right
    print "map_dict(...).shape = ", np.array(map_dict(shares, lambda
      x:x).values()).shape
    shares_sq = np.sum(np.array(map_dict(shares, lambda x: x*x).values()), axis=0)
    print 'shares_sq.shape: ', shares_sq.shape
    n_shares_sq = shares_sq * active
    shares_arr = np.array(shares.values())
    shares_sum = np.sum(np.array(shares.values()), axis=0)
    shares_sum2 = shares_sum * shares_sum
    print 'shares_sum2.shape = ', shares_sum2.shape
    return shares_sum2 / n_shares_sq
  names = frameworks_from_data(data)
  framework_usage = {}
  for name in names:
    framework_usage[name] = extract_framework(name)
  max_shares = {'cpu': max(data['cpu']), 'memory': max(data['memory'])}
  framework_share = map_dict(framework_usage, shares_for(max_shares))
  print 'framework_share: ', np.array(framework_share.values()).shape
  framework_dom = map_dict(framework_share, dominant_resource_from_share)
  print 'framework_dom: ', np.array(framework_dom.values()).shape
  active = active_counts(names)
  print 'active: ', active.shape
  fairness = jain_fairness(framework_dom, active)
  return fairness


if __name__ == '__main__':
  build_args(parser)
  utilization.build_args_ext(parser)
  args = parser.parse_args()
  utilization.args = args
  csv = np.genfromtxt(args.csv, dtype=None, names=True, delimiter=',',
      skip_header=False)
  print 'csv = ', csv
  print 'csv_dtype = ', csv.dtype
  for i in xrange(len(csv)):
    log_file = args.log_base + '.' + str(i)
    print "Processing ", log_file
    csv_line = csv[i]
    fairness = fairness_from(log_file, csv_line)
    np.save(args.out_base + '.fairness.' + str(i), fairness)

