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
    case Value::SCALAR:
      {
        Resource scaled;
        scaled.set_name(resource.name());
        scaled.set_type(Value::SCALAR);
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
  *oldResources = newResources;
  *oldResources += extraResources;
}

void addZerosFrom(const Resources& source, Resources* dest)
{
  foreach (const Resource& sourceResource, source) {
    if (sourceResource.type() != Value::SCALAR) {
      continue;
    }
    if (dest->get(sourceResource).isNone()) {
      Resource newResource = sourceResource;
      newResource.mutable_scalar()->set_value(0.0);
      *dest += newResource;
    }
  }
}

Resources subtractWithNegatives(const Resources& a, const Resources& b)
{
  Resources aWithZeros = a;
  addZerosFrom(b, &aWithZeros);
  aWithZeros -= b;
  return aWithZeros;
}

} // anonymous namespace

void
ResourceEstimates::updateEstimates(double now, double duration,
    const Resources& usage)
{
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
      newUsedPerTask -= lastUsedPerTaskDiff;
      LOG(INFO) << "newUsedPerTask = " << newUsedPerTask;
      LOG(INFO) << "lastUsedPerTask = " << lastUsedPerTask;
      lastUsedPerTask = newUsedPerTask;
      LOG(INFO) << "lastUsedPerTask = " << lastUsedPerTask;
      LOG(INFO) << " (delta: " << lastUsedPerTaskDiff << ")";
    }
  } else {
    LOG(INFO) << "not updating per-task estimate; tasks = " << curTasks
              << "; now - duration = " << (now - duration)
              << "; setTaskTime = " << setTaskTime;
  }
}

void
ResourceEstimates::setUsage(double now, double duration,
    const Resources& usage, bool clearUnknown, bool keepCharge)
{
  Resources usageDiff = subtractWithNegatives(usage, usedResources);
  usedResources = usage;
  usageDuration = duration;
  Resources nextDiff = updateNextWithGuess(now, usedResources, clearUnknown);
  Resources chargedDiff = updateCharged(clearUnknown);
  LOG(INFO) << "deltas usage " << usageDiff << "; next: " << nextDiff
            << "; charged: " << chargedDiff;
  foreach (ResourceEstimates* aggregate, linked) {
    aggregate->usedResources += usageDiff;
    aggregate->nextUsedResources += nextDiff;
    if (keepCharge) {
      aggregate->deadChargedResources =
        subtractWithNegatives(aggregate->deadChargedResources, chargedDiff);
    } else {
      aggregate->chargedResources += chargedDiff;
    }
  }
  setTime(now);
}

void
ResourceEstimates::expireCharge()
{
  chargedResources -= deadChargedResources;
  deadChargedResources = Resources();
}

void
ResourceEstimates::observeUsage(double now, double duration,
                                const Resources& usage)
{
  LOG(INFO) << "observeUsage on \n" << *this;
  updateEstimates(now, duration, usage);
  LOG(INFO) << "set observed usage to " << usage << " from\n" << *this;
  setUsage(now, duration, usage, false, false);
}

void
ResourceEstimates::clearUsage(double now, bool clearCharge)
{
  setUsage(now, 0.0, Resources(), true, !clearCharge);
}

void
ResourceEstimates::setGuess(double now, const Resources& guess) {
  Resources nextDiff = updateNextWithGuess(now, guess, true);
  foreach (ResourceEstimates* aggregate, linked) {
    aggregate->nextUsedResources += nextDiff;
  }
  setTime(now);
}


void
ResourceEstimates::setTasks(double now, int newTasks) {
  LOG(INFO) << "adjusting tasks with newTasks = " << newTasks;
  if (lastUsedPerTask.isSome()) {
    // TODO(charles): expire these estimates???
    Resources zeroTasks;
    if (lastUsedForZero.isSome()) {
      zeroTasks = lastUsedForZero.get();
    }
    setGuess(now, zeroTasks +
                  multiplyResources(lastUsedPerTask.get(), newTasks));
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
  Resources minDiff = subtractWithNegatives(newMinResources, minResources);
  minResources = newMinResources;
  Resources chargedDiff = updateCharged(false);
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
ResourceEstimates::updateNextWithGuess(double now, Resources guess,
      bool clearUnknown) {
  Resources oldNextResources = nextUsedResources;
  if (clearUnknown) {
    nextUsedResources = guess;
  } else {
    assignResourcesKeepOthers(guess, &nextUsedResources);
  }
  LOG(INFO) << "update next: " << oldNextResources << " -> " << nextUsedResources;
  return subtractWithNegatives(nextUsedResources, oldNextResources);
}

Resources
ResourceEstimates::updateCharged(bool clearUnknown) {
  Resources oldChargedResources = chargedResources;
  Resources newCharged = maxResources(usedResources, minResources);
  if (clearUnknown) {
    chargedResources = newCharged;
  } else {
    assignResourcesKeepOthers(newCharged, &chargedResources);
  }
  return subtractWithNegatives(chargedResources, oldChargedResources);
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

UsageTrackerImpl::UsageTrackerImpl(const Configuration& conf_)
    : lastTickTime(0.0)
{
}

void
UsageTrackerImpl::recordUsage(const UsageMessage& update) {
  LOG(INFO) << "recordUsage(" << update.DebugString() << ")";
  Resources usage = update.resources();
  estimateFor(update.framework_id(), update.executor_id(), update.slave_id())->
    observeUsage(update.timestamp(), update.duration(), usage);
  // TODO(Charles Reiss): Make this conditional on this update not being
  // an old, stray update.
  if (!update.still_running()) {
    forgetExecutor(update.framework_id(), update.executor_id(),
                   update.slave_id(), false);
  }
}

void
UsageTrackerImpl::placeUsage(const FrameworkID& frameworkId,
                             const ExecutorID& executorId,
                             const SlaveID& slaveId,
                             const Resources& minResources,
                             const Option<Resources>& estResources,
                             int numTasks) {
  LOG(INFO) << "placeUsage(" << frameworkId << "," << executorId
            << "," << slaveId << ", min: " << minResources
            << ", est: " << estResources << ", " << numTasks << ")";
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
                                 const SlaveID& slaveId,
                                 bool clearCharge) {
  LOG(INFO) << "forgetExecutor(" << frameworkId << "," << executorId
            << slaveId << ")";
  const ExecutorKey key(boost::make_tuple(frameworkId, executorId, slaveId));
  if (estimateByExecutor.count(key) > 0) {
    estimateByExecutor[key].clearUsage(lastTickTime, clearCharge);
  }
  estimateByExecutor.erase(key);
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
  LOG(INFO) << "timerTick(" << curTime << ")";
  foreachvalue (ResourceEstimates& estimates, frameworkEstimates) {
    estimates.expireCharge();
  }
  foreachvalue (ResourceEstimates& estimates, slaveEstimates) {
    estimates.expireCharge();
  }
  foreachvalue (ResourceEstimates& estimates, estimateByExecutor) {
    estimates.expireCharge();
  }

  // TODO(charles): do we still want this behavior?
  std::vector<ExecutorKey> toRemove;
  foreachpair (const ExecutorKey& key, const ResourceEstimates& estimates,
               estimateByExecutor) {
    if (curTime - estimates.estimateTime > kForgetTime &&
        estimates.curTasks == 0) {
      LOG(INFO) << "Found stale entry " << key.v.get<0>() << " "
                << key.v.get<1>() << " " << key.v.get<2>();
      toRemove.push_back(key);
    }
  }
  foreach (const ExecutorKey& key, toRemove) {
    forgetExecutor(key.v.get<0>(), key.v.get<1>(), key.v.get<2>(), true);
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
