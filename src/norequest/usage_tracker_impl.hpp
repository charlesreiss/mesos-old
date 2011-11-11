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

#ifndef __NOREQUEST_USAGE_TRACKER_IMPL_HPP__
#define __NOREQUEST_USAGE_TRACKER_IMPL_HPP__

#include <iosfwd>

#include <boost/tuple/tuple.hpp>

#include "common/hashmap.hpp"
#include "common/resources.hpp"

#include "master/allocator.hpp"

#include "norequest/usage_tracker.hpp"

namespace mesos {
namespace internal {
namespace norequest {

Resources maxResources(Resources a, Resources b);

struct ResourceEstimates {
  Resources usedResources;
  Resources minResources;
  Resources chargedResources;
  Resources nextUsedResources;

  int curTasks;
  double setTaskTime;
  double estimateTime;
  double usageDuration;


  ResourceEstimates* linked[2];

  void link(ResourceEstimates* first, ResourceEstimates* second) {
    linked[0] = first;
    linked[1] = second;
  }

  void observeUsage(double now, double duration, const Resources& usage);
  void setGuess(double now, const Resources& guess);
  // TODO(charles): call after/before set guess?
  void setTasks(double now, int tasks);
  void setMin(double now, const Resources& min);
  void setTime(double now);

  ResourceEstimates() : estimateTime(0.0), setTaskTime(0.0),
                        usageDuration(0.0), curTasks(0) {}
private:
  Resources updateNextWithGuess(double now, Resources guess);
  Resources updateCharged();

};

// for debugging
std::ostream& operator<<(std::ostream& out, const ResourceEstimates&);

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

  Resources chargeForFramework(const FrameworkID& frameworkId) const {
    return usageByFramework()[frameworkId].chargedResources;
  }
  Resources nextUsedForFramework(const FrameworkID& frameworkId) const {
    return usageByFramework()[frameworkId].nextUsedResources;
  }
  Resources usedForFramework(const FrameworkID& frameworkId) const {
    return usageByFramework()[frameworkId].usedResources;
  }
  Resources gaurenteedForFramework(const FrameworkID& frameworkId) const {
    return usageByFramework()[frameworkId].minResources;
  }
  Resources freeForSlave(const SlaveID& slaveId) const;
  Resources gaurenteedFreeForSlave(const SlaveID& slaveId) const;
  Resources nextUsedForExecutor(const SlaveID& slaveId,
                                const FrameworkID& frameworkId,
                                const ExecutorID& executorId) const;
  Resources gaurenteedForExecutor(const SlaveID& slaveId,
                                  const FrameworkID& frameworkId,
                                  const ExecutorID& executorId) const;

private:
  hashmap<FrameworkID, ResourceEstimates> usageByFramework() const;
  hashmap<FrameworkID, ResourceEstimates> frameworkEstimates;
  hashmap<SlaveID, ResourceEstimates> slaveEstimates;
  hashmap<SlaveID, Resources> slaveCapacities;
  hashmap<ExecutorKey, ResourceEstimates> estimateByExecutor;
  double lastTickTime;

  ResourceEstimates* estimateFor(const FrameworkID& frameworkId,
                                 const ExecutorID& executorId,
                                 const SlaveID& slaveId);
};

} // namespace norequest {
} // namespace internal {
} // namespace mesos {

#endif

