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

#include "stout/hashmap.hpp"
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
  Resources deadChargedResources;
  Resources nextUsedResources;

  int curTasks;
  double setTaskTime;
  double estimateTime;
  double usageDuration;

  Option<Resources> lastUsedPerTask;
  double lastUsedPerTaskTime;
  int lastUsedPerTaskTasks;
  Option<Resources> lastUsedForZero;
  double lastUsedForZeroTime;

  ResourceEstimates* linked[2];

  void link(ResourceEstimates* first, ResourceEstimates* second) {
    linked[0] = first;
    linked[1] = second;
  }

  // Expire charged resources for finished tasks.
  void expireCharge();
  void observeUsage(double now, double duration, const Resources& usage);
  void clearUsage(double now, bool clearCharge);
  void setGuess(double now, const Resources& guess);
  // TODO(charles): call after/before set guess?
  void setTasks(double now, int tasks);
  void setMin(double now, const Resources& min);
  void setTime(double now);

  ResourceEstimates() : estimateTime(0.0), curTasks(0), setTaskTime(0.0),
                        usageDuration(0.0), lastUsedPerTask(),
                        lastUsedPerTaskTasks(0), lastUsedForZero(),
                        lastUsedPerTaskTime(0.0) {
    linked[0] = linked[1] = 0;
  }
private:
  void updateEstimates(double now, double duration, const Resources& usage);
  void setUsage(double now, double duration, const Resources& usage,
      bool clearUnknown, bool keepCharge);
  void adjustLastUsedPerTask();
  Resources updateNextWithGuess(double now, Resources guess, bool clearUnknown);
  Resources updateCharged(bool clearUnknown);

};

// for debugging
std::ostream& operator<<(std::ostream& out, const ResourceEstimates&);

class UsageTrackerImpl : public UsageTracker {
public:
  UsageTrackerImpl(const Configuration& conf_);
  void recordUsage(const UsageMessage& update);
  void placeUsage(const FrameworkID& frameworkId,
                  const ExecutorID& executorId,
                  const SlaveID& slaveId,
                  const Resources& minResources,
                  const Option<Resources>& estResources,
                  int numTasks);
  void forgetExecutor(const FrameworkID& frameworkId,
                      const ExecutorID& executorId,
                      const SlaveID& slaveId,
                      bool clearCharge);
  void setCapacity(const SlaveID& slaveId,
                   const Resources& resources);
  void timerTick(double curTime);

  Resources chargeForFramework(const FrameworkID& frameworkId) const {
    return estimateForFramework(frameworkId).chargedResources;
  }
  Resources nextUsedForFramework(const FrameworkID& frameworkId) const {
    return estimateForFramework(frameworkId).nextUsedResources;
  }
  Resources usedForFramework(const FrameworkID& frameworkId) const {
    return estimateForFramework(frameworkId).usedResources;
  }
  Resources gaurenteedForFramework(const FrameworkID& frameworkId) const {
    return estimateForFramework(frameworkId).minResources;
  }
  Resources freeForSlave(const SlaveID& slaveId) const;
  Resources gaurenteedFreeForSlave(const SlaveID& slaveId) const;
  Resources nextUsedForExecutor(const SlaveID& slaveId,
                                const FrameworkID& frameworkId,
                                const ExecutorID& executorId) const;
  Resources gaurenteedForExecutor(const SlaveID& slaveId,
                                  const FrameworkID& frameworkId,
                                  const ExecutorID& executorId) const;

#if 0
  void sanityCheckAgainst(mesos::internal::master::Master* master);
#endif

private:
  const hashmap<FrameworkID, ResourceEstimates>& usageByFramework() const;
  const ResourceEstimates& estimateForFramework(const FrameworkID& framework) const {
    hashmap<FrameworkID, ResourceEstimates>::const_iterator it =
        usageByFramework().find(framework);
    if (it == usageByFramework().end()) {
      return _emptyEstimates;
    } else {
      return it->second;
    }
  }
  const ResourceEstimates _emptyEstimates;
  hashmap<FrameworkID, ResourceEstimates> frameworkEstimates;
  hashmap<SlaveID, ResourceEstimates> slaveEstimates;
  hashmap<SlaveID, Resources> slaveCapacities;
  hashmap<ExecutorKey, ResourceEstimates> estimateByExecutor;
  double lastTickTime;

  // Smoothing parameters
  bool smoothUsage;
  double smoothDecay;
  double smoothDecayMem;
  void smoothUsageUpdate(Resources* observation, double duration,
                         const Resources& oldUsage);

  ResourceEstimates* estimateFor(const FrameworkID& frameworkId,
                                 const ExecutorID& executorId,
                                 const SlaveID& slaveId);
};

} // namespace norequest {
} // namespace internal {
} // namespace mesos {

#endif
