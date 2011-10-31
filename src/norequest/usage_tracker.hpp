/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __NOREQUEST_USAGE_TRACKER_HPP__
#define __NOREQUEST_USAGE_TRACKER_HPP__

#include <iosfwd>

#include <boost/tuple/tuple.hpp>

#include "common/hashmap.hpp"
#include "common/resources.hpp"

#include "master/allocator.hpp"

namespace mesos {
namespace internal {
namespace norequest {

Resources maxResources(Resources a, Resources b);

struct ResourceEstimates {
  Resources usedResources;
  Resources minResources;
  // usually max(used, min)
  Resources chargedResources;

  // best guess estimate of next _used_ resources.
  Resources nextUsedResources;

  Resources updateNextWithGuess(double now, Resources guess);

  Resources updateNext(double now) {
    return updateNextWithGuess(now, usedResources);
  }

  Resources updateCharged();

  double estimateTime;
  int curTasks;

  ResourceEstimates() : estimateTime(0.0), curTasks(0) {}
};

// for debugging
std::ostream& operator<<(std::ostream& out, const ResourceEstimates&);

// abstract base class to aid testing.
class UsageTracker {
public:
  virtual void recordUsage(const UsageMessage& update) = 0;
  virtual void placeUsage(const FrameworkID& frameworkId,
                          const ExecutorID& executorId,
                          const SlaveID& slaveId,
                          const Resources& minResources,
                          const Option<Resources>& estResources,
                          int numTasks) = 0;
  virtual void forgetExecutor(const FrameworkID& frameworkId,
                              const ExecutorID& executorId,
                              const SlaveID& slaveId) = 0;
  virtual void setCapacity(const SlaveID& slaveId,
                           const Resources& resources) = 0;
  virtual void timerTick(double cur_time) = 0;

  virtual hashmap<FrameworkID, ResourceEstimates> usageByFramework() const = 0;
  virtual Resources nextUsedForExecutor(const SlaveID& slaveId,
                                        const FrameworkID& frameworkId,
                                        const ExecutorID& executorId)
    const = 0;
  virtual Resources gaurenteedForExecutor(const SlaveID& slaveId,
                                          const FrameworkID& frameworkId,
                                          const ExecutorID& executorId)
    const = 0;
  virtual Resources chargeForFramework(const FrameworkID& frameworkId) const {
    return usageByFramework()[frameworkId].chargedResources;
  }
  virtual Resources nextUsedForFramework(const FrameworkID& frameworkId) const {
    return usageByFramework()[frameworkId].nextUsedResources;
  }
  virtual Resources freeForSlave(const SlaveID& slaveId) const = 0;
  virtual Resources gaurenteedFreeForSlave(const SlaveID& slaveId) const = 0;

  virtual ~UsageTracker() {}
};

struct ExecutorKey {
  ExecutorKey(const boost::tuple<FrameworkID, ExecutorID, SlaveID>& _v)
      : v(_v) {}
  boost::tuple<FrameworkID, ExecutorID, SlaveID> v;
};

inline bool operator==(const ExecutorKey& first, const ExecutorKey& second) {
  return first.v.get<0>() == second.v.get<0>() &&
         first.v.get<1>() == second.v.get<1>() &&
         first.v.get<2>() == second.v.get<2>();
}

inline std::size_t hash_value(const ExecutorKey& value) {
  std::size_t seed = 0;
  boost::hash_combine(seed, value.v.get<0>());
  boost::hash_combine(seed, value.v.get<1>());
  boost::hash_combine(seed, value.v.get<2>());
  return seed;
}

struct AllEstimates {
  ResourceEstimates* executor;
  ResourceEstimates* aggregates[2];
};

class UsageTrackerImpl : public UsageTracker {
public:
  UsageTrackerImpl() : lastTickTime(0.0) {}
  void recordUsage(const UsageMessage& update);
  void placeUsage(const FrameworkID& frameworkId,
                  const ExecutorID& executorId,
                  const SlaveID& slaveId,
                  const Resources& minResources,
                  const Option<Resources>& estResources,
                  int numTasks);
  void forgetExecutor(const FrameworkID& frameworkId,
                      const ExecutorID& executorId,
                      const SlaveID& slaveId);
  void setCapacity(const SlaveID& slaveId,
                   const Resources& resources);
  void timerTick(double curTime);

  hashmap<FrameworkID, ResourceEstimates> usageByFramework() const;
  Resources freeForSlave(const SlaveID& slaveId) const;
  Resources gaurenteedFreeForSlave(const SlaveID& slaveId) const;
  Resources nextUsedForExecutor(const SlaveID& slaveId,
                                const FrameworkID& frameworkId,
                                const ExecutorID& executorId) const;
  Resources gaurenteedForExecutor(const SlaveID& slaveId,
                                  const FrameworkID& frameworkId,
                                  const ExecutorID& executorId) const;

private:
  hashmap<FrameworkID, ResourceEstimates> frameworkEstimates;
  hashmap<SlaveID, ResourceEstimates> slaveEstimates;
  hashmap<SlaveID, Resources> slaveCapacities;
  hashmap<ExecutorKey, ResourceEstimates> estimateByExecutor;
  double lastTickTime;

  AllEstimates allEstimatesFor(const FrameworkID& frameworkId,
                               const ExecutorID& executorId,
                               const SlaveID& slaveId);
  AllEstimates allEstimatesFor(const ExecutorKey& key) {
    return allEstimatesFor(key.v.get<0>(), key.v.get<1>(), key.v.get<2>());
  }
};


} // namespace norequest {
} // namespace internal {
} // namespace mesos {

#endif
