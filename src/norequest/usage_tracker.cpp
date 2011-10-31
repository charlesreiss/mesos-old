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

#include <ostream>

#include "norequest/usage_tracker.hpp"

namespace mesos {
namespace internal {
namespace norequest {

Resources maxResources(Resources a, Resources b) {
  Resources both = a + b;
  Resources result;
  foreach (const Resource& resource, both) {
    Option<Resource> resA = a.get(resource);
    Option<Resource> resB = b.get(resource);
    if (resA.isSome() && resB.isSome()) {
      result += resB.get() <= resA.get() ? resA.get() : resB.get();
    } else if (resA.isSome()) {
      result += resA.get();
    } else {
      CHECK(resB.isSome());
      result += resB.get();
    }
  }
  return result;
}

std::ostream&
operator<<(std::ostream& out, const ResourceEstimates& estimates) {
  out << "used:   " << estimates.usedResources << "\n"
      << "min:    " << estimates.minResources << "\n"
      << "next:   " << estimates.nextUsedResources << "\n"
      << "charge: " << estimates.chargedResources << "\n"
      << "at " << estimates.estimateTime << " with "
      << estimates.curTasks << " tasks";
  return out;
}

namespace {

void assignResourcesKeepOthers(const Resources& newResources, Resources* oldResources) {
  Resources extraResources;
  foreach (const Resource& oldResource, *oldResources) {
    Option<Resource> newResource = newResources.get(oldResource);
    if (newResource.isNone()) {
      extraResources += oldResource;
    }
  }
  LOG(INFO) << *oldResources << " <-- " << newResources << " (extra: "
            << extraResources << ")";
  *oldResources = newResources;
  *oldResources += extraResources;
}

void updateTime(double now, ResourceEstimates* estimate) {
  estimate->estimateTime = std::max(estimate->estimateTime, now);
}

void updateTime(double now, AllEstimates estimates) {
  updateTime(now, estimates.executor);
  foreach (ResourceEstimates* estimate, estimates.aggregates) {
    updateTime(now, estimate);
  }
}

void mergeObservedUsage(double now, const Resources& usage,
                        AllEstimates estimates) {
  LOG(INFO) << "set observed usage to " << usage << " from\n"
            << *estimates.executor;
  Resources usageDiff = usage - estimates.executor->usedResources;
  estimates.executor->usedResources = usage;
  Resources nextDiff = estimates.executor->updateNext(now);
  Resources chargedDiff = estimates.executor->updateCharged();
  LOG(INFO) << "deltas usage " << usageDiff << "; next: " << nextDiff
            << "; charged: " << chargedDiff;
  foreach (ResourceEstimates* aggregate, estimates.aggregates) {
    aggregate->usedResources += usageDiff;
    aggregate->nextUsedResources += nextDiff;
    aggregate->chargedResources += chargedDiff;
  }
  updateTime(now, estimates);
}

void mergeMinResources(double now, const Resources& minResources,
                       AllEstimates estimates) {
  Resources minDiff = minResources - estimates.executor->minResources;
  estimates.executor->minResources = minResources;
  Resources chargedDiff = estimates.executor->updateCharged();
  foreach (ResourceEstimates* aggregate, estimates.aggregates) {
    aggregate->minResources += minDiff;
    aggregate->chargedResources += chargedDiff;
  }
  updateTime(now, estimates);
}

void mergeInitialGuess(double now, Resources usage, AllEstimates estimates) {
  Resources nextDiff = estimates.executor->updateNextWithGuess(now, usage);
  foreach (ResourceEstimates* aggregate, estimates.aggregates) {
    aggregate->nextUsedResources += nextDiff;
  }
  updateTime(now, estimates);
}

} // anonymous namespace

Resources
ResourceEstimates::updateNextWithGuess(double now, Resources guess) {
  Resources oldNextResources = nextUsedResources;
  assignResourcesKeepOthers(guess, &nextUsedResources);
  estimateTime = now;
  return nextUsedResources - oldNextResources;
}

Resources
ResourceEstimates::updateCharged() {
  Resources oldChargedResources = chargedResources;
  assignResourcesKeepOthers(maxResources(usedResources, minResources),
                  &chargedResources);
  return chargedResources - oldChargedResources;
}

AllEstimates
UsageTrackerImpl::allEstimatesFor(const FrameworkID& frameworkId,
                                  const ExecutorID& executorId,
                                  const SlaveID& slaveId) {
  AllEstimates result;
  result.executor = &estimateByExecutor[boost::make_tuple(frameworkId,
                                                          executorId,
                                                          slaveId)];
  result.aggregates[0] = &slaveEstimates[slaveId];
  result.aggregates[1] = &frameworkEstimates[frameworkId];
  return result;
}

void
UsageTrackerImpl::recordUsage(const UsageMessage& update) {
  LOG(INFO) << "recordUsage(" << update.DebugString() << ")";
  mergeObservedUsage(update.timestamp(),
                     update.resources(),
                     allEstimatesFor(update.framework_id(),
                                     update.executor_id(),
                                     update.slave_id()));

}

void
UsageTrackerImpl::placeUsage(const FrameworkID& frameworkId,
                             const ExecutorID& executorId,
                             const SlaveID& slaveId,
                             const Resources& minResources,
                             const Option<Resources>& estResources,
                             int numTasks) {
  AllEstimates estimates = allEstimatesFor(frameworkId, executorId, slaveId);
  mergeMinResources(lastTickTime, minResources, estimates);
  if (estResources.isSome()) {
    mergeInitialGuess(lastTickTime, estResources.get(), estimates);
  }
  int taskDiff = numTasks - estimates.executor->curTasks;
  LOG(INFO) << "Adjusting task count by " << taskDiff << " for "
            << frameworkId << " " << executorId << " (" << slaveId << ")";
  estimates.executor->curTasks = numTasks;
  foreach (ResourceEstimates* estimate, estimates.aggregates) {
    estimate->curTasks += taskDiff;
  }
}

void
UsageTrackerImpl::forgetExecutor(const FrameworkID& frameworkId,
                                 const ExecutorID& executorId,
                                 const SlaveID& slaveId) {
  placeUsage(frameworkId, executorId, slaveId, Resources(),
             Option<Resources>(Resources()), 0);
  estimateByExecutor.erase(ExecutorKey(
        boost::make_tuple(frameworkId, executorId, slaveId)));
}

void
UsageTrackerImpl::setCapacity(const SlaveID& slaveId,
                              const Resources& resources) {
  slaveCapacities[slaveId] = resources;
}

void
UsageTrackerImpl::timerTick(double curTime) {
  const double kForgetTime = 2.0;
  lastTickTime = curTime;
  // TODO(charles): do we still want this behavior?
  std::vector<ExecutorKey> toRemove;
  foreachpair (const ExecutorKey& key, const ResourceEstimates estimates,
               estimateByExecutor) {
    if (curTime - estimates.estimateTime > kForgetTime &&
        estimates.curTasks == 0) {
      LOG(INFO) << "Found stale entry " << key.v.get<0>() << " "
                << key.v.get<1>() << " " << key.v.get<2>();
      AllEstimates all = allEstimatesFor(key);
      placeUsage(key.v.get<0>(),
                 key.v.get<1>(),
                 key.v.get<2>(),
                 Resources(),
                 Option<Resources>(Resources()),
                 0);
      toRemove.push_back(key);
    }
  }
  foreach (const ExecutorKey& key, toRemove) {
    estimateByExecutor.erase(key);
    if (frameworkEstimates[key.v.get<0>()].curTasks == 0) {
      frameworkEstimates.erase(key.v.get<0>());
    }
  }
}

hashmap<FrameworkID, ResourceEstimates>
UsageTrackerImpl::usageByFramework() const {
  return frameworkEstimates;
}

template <class Key, class Value>
Value lookupOrDefault(const hashmap<Key, Value>& container, const Key& key) {
  typename hashmap<Key, Value>::const_iterator it = container.find(key);
  if (it == container.end()) {
    return Value();
  } else {
    return it->second;
  }
}

Resources
UsageTrackerImpl::freeForSlave(const SlaveID& slave) const {
  LOG(INFO) << "Slave estimates = \n" << lookupOrDefault(slaveEstimates, slave);
  return lookupOrDefault(slaveCapacities, slave) -
         lookupOrDefault(slaveEstimates, slave).nextUsedResources;
}

Resources
UsageTrackerImpl::gaurenteedFreeForSlave(const SlaveID& slave) const {
  return lookupOrDefault(slaveCapacities, slave) -
         lookupOrDefault(slaveEstimates, slave).minResources;
}

Resources
UsageTrackerImpl::nextUsedForExecutor(const SlaveID& slaveId,
                                      const FrameworkID& frameworkId,
                                      const ExecutorID& executorId) const {
  const ExecutorKey key(boost::make_tuple(frameworkId, executorId, slaveId));
  return lookupOrDefault(estimateByExecutor, key).nextUsedResources;
}

Resources
UsageTrackerImpl::gaurenteedForExecutor(const SlaveID& slaveId,
                                       const FrameworkID& frameworkId,
                                       const ExecutorID& executorId) const {
  const ExecutorKey key(boost::make_tuple(frameworkId, executorId, slaveId));
  return lookupOrDefault(estimateByExecutor, key).minResources;
}

} // namespace norequest {
} // namespace internal {
} // namespace mesos {
