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
#include "norequest/usage_tracker_impl.hpp"

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

Resources multiplyResources(const Resources& resources, double by) {
  Resources result;
  foreach (const Resource& resource, resources) {
    switch (resource.type()) {
    case Resource::SCALAR:
      {
        Resource scaled;
        scaled.set_name(resource.name());
        scaled.set_type(Resource::SCALAR);
        scaled.mutable_scalar()->set_value(resource.scalar().value() * by);
        CHECK(Resources::isValid(scaled));
        result += scaled;
        break;
      }
    default:
      result += resource;
    }
  }
  return result;
}

void assignResourcesKeepOthers(const Resources& newResources,
                               Resources* oldResources) {
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

} // anonymous namespace

void
ResourceEstimates::observeUsage(double now, double duration,
                                const Resources& usage) {
  LOG(INFO) << "observeUsage on \n" << *this;
  if (now - duration > setTaskTime && curTasks > 0) {
    LOG(INFO) << "lastUsedPerTask = " << lastUsedPerTask;
    Resources tasksUsage = usage;
    if (lastUsedForZero.isSome()) {
      tasksUsage -= lastUsedForZero.get();
    }
    lastUsedPerTask = multiplyResources(tasksUsage, 1.0 / curTasks);
    lastUsedPerTaskTasks = curTasks;
    lastUsedPerTaskTime = now;
  } else if (now - duration > setTaskTime && curTasks == 0) {
    Resources lastUsedForZeroDiff = usage;
    if (lastUsedForZero.isSome()) {
      lastUsedForZeroDiff -= lastUsedForZero.get();
    }
    LOG(INFO) << "lastUsedPerTask = " << lastUsedPerTask;
    LOG(INFO) << "lastUsedForZeroDiff = " << lastUsedForZeroDiff;
    lastUsedForZero = usage;
    LOG(INFO) << "lastUsedForZero = " << usage;
    if (lastUsedPerTask.isSome()) {
      Resources lastUsedPerTaskDiff =
        multiplyResources(lastUsedForZeroDiff, 1.0 / lastUsedPerTaskTasks);
      Resources newUsedPerTask = lastUsedPerTask.get();
      LOG(INFO) << "newUsedPerTask = " << newUsedPerTask;
      newUsedPerTask += lastUsedPerTaskDiff;
      LOG(INFO) << "newUsedPerTask = " << newUsedPerTask;
      LOG(INFO) << "lastUsedPerTask = " << lastUsedPerTask;
      lastUsedPerTask.get() = newUsedPerTask;
      LOG(INFO) << "lastUsedPerTask = " << lastUsedPerTask;
      LOG(INFO) << " (delta: " << lastUsedPerTaskDiff << ")";
    }
  } else {
    LOG(INFO) << "Not updating per-task estimate; tasks = " << curTasks
              << "; now - duration = " << (now - duration)
              << "; setTaskTime = " << setTaskTime;
  }
  LOG(INFO) << "set observed usage to " << usage << " from\n" << *this;
  Resources usageDiff = usage - usedResources;
  usedResources = usage;
  usageDuration = duration;
  Resources nextDiff = updateNextWithGuess(now, usedResources);
  Resources chargedDiff = updateCharged();
  LOG(INFO) << "deltas usage " << usageDiff << "; next: " << nextDiff
            << "; charged: " << chargedDiff;
  foreach (ResourceEstimates* aggregate, linked) {
    aggregate->usedResources += usageDiff;
    aggregate->nextUsedResources += nextDiff;
    aggregate->chargedResources += chargedDiff;
  }
  setTime(now);
}

void
ResourceEstimates::setGuess(double now, const Resources& guess) {
  Resources nextDiff = updateNextWithGuess(now, guess);
  foreach (ResourceEstimates* aggregate, linked) {
    aggregate->nextUsedResources += nextDiff;
  }
  setTime(now);
}


void
ResourceEstimates::setTasks(double now, int newTasks) {
  if (lastUsedPerTask.isSome()) {
    // TODO(charles): expire these estimates???
    setGuess(now, multiplyResources(lastUsedPerTask.get(), newTasks));
  } else if (curTasks > 0 && newTasks > 0) {
    setGuess(now, multiplyResources(nextUsedResources,
                                    double(newTasks) / curTasks));
  }
  setTaskTime = now;
  // TODO(charles): aggregate task counts??
  curTasks = newTasks;
}

void
ResourceEstimates::setMin(double now, const Resources& newMinResources) {
  // TODO(charles): does this handle removing a resource type from
  // our minimum?
  Resources minDiff = newMinResources - minResources;
  minResources = newMinResources;
  Resources chargedDiff = updateCharged();
  foreach (ResourceEstimates* aggregate, linked) {
    aggregate->minResources += minDiff;
    aggregate->chargedResources += chargedDiff;
  }
  setTime(now);
}

void
ResourceEstimates::setTime(double now) {
  estimateTime = std::max(estimateTime, now);
  foreach (ResourceEstimates* aggregate, linked) {
    aggregate->estimateTime = std::max(aggregate->estimateTime, now);
  }
}

Resources
ResourceEstimates::updateNextWithGuess(double now, Resources guess) {
  Resources oldNextResources = nextUsedResources;
  assignResourcesKeepOthers(guess, &nextUsedResources);
  return nextUsedResources - oldNextResources;
}

Resources
ResourceEstimates::updateCharged() {
  Resources oldChargedResources = chargedResources;
  assignResourcesKeepOthers(maxResources(usedResources, minResources),
                  &chargedResources);
  return chargedResources - oldChargedResources;
}

ResourceEstimates*
UsageTrackerImpl::estimateFor(const FrameworkID& frameworkId,
                              const ExecutorID& executorId,
                              const SlaveID& slaveId) {
  ExecutorKey key(boost::make_tuple(frameworkId, executorId, slaveId));
  hashmap<ExecutorKey, ResourceEstimates>::iterator it =
    estimateByExecutor.find(key);
  if (it == estimateByExecutor.end()) {
    ResourceEstimates *executor = &estimateByExecutor[key];
    executor->link(&slaveEstimates[slaveId], &frameworkEstimates[frameworkId]);
    return executor;
  } else {
    return &it->second;
  }
}

void
UsageTrackerImpl::recordUsage(const UsageMessage& update) {
  LOG(INFO) << "recordUsage(" << update.DebugString() << ")";
  Resources usage = update.resources();
  estimateFor(update.framework_id(), update.executor_id(), update.slave_id())->
    observeUsage(update.timestamp(), update.duration(), usage);
}

void
UsageTrackerImpl::placeUsage(const FrameworkID& frameworkId,
                             const ExecutorID& executorId,
                             const SlaveID& slaveId,
                             const Resources& minResources,
                             const Option<Resources>& estResources,
                             int numTasks) {
  ResourceEstimates* executor = estimateFor(frameworkId, executorId, slaveId);
  executor->setMin(lastTickTime, minResources);
  if (estResources.isSome()) {
    executor->setGuess(lastTickTime, estResources.get());
  }
  executor->setTasks(lastTickTime, numTasks);
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
      toRemove.push_back(key);
    }
  }
  foreach (const ExecutorKey& key, toRemove) {
    forgetExecutor(key.v.get<0>(), key.v.get<1>(), key.v.get<2>());
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
