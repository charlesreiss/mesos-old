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

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "norequest/allocator.hpp"
#include <process/process.hpp>

#define SUGGEST_TASKS

using boost::unordered_map;
using boost::unordered_set;

namespace mesos {
namespace internal {
namespace norequest {

using std::vector;

void
NoRequestAllocator::frameworkAdded(Framework* framework) {
  LOG(INFO) << "add framework";
  makeNewOffers(master->getActiveSlaves());
}

void
NoRequestAllocator::slaveAdded(Slave* slave) {
  CHECK_EQ(0, refusers.count(slave));
  LOG(INFO) << "add slave";
  totalResources += slave->info.resources();
  tracker->setCapacity(slave->id, slave->info.resources());
  std::vector<Slave*> slave_alone;
  slave_alone.push_back(slave);
  makeNewOffers(slave_alone);
}

void
NoRequestAllocator::slaveRemoved(Slave* slave) {
  totalResources -= slave->info.resources();
  tracker->setCapacity(slave->id, Resources());
  refusers.erase(slave);
}

void
NoRequestAllocator::taskAdded(Task* task) {
  placeUsage(task->framework_id(), task->executor_id(), task->slave_id(),
             task, 0, Option<ExecutorInfo>::none());
}

void
NoRequestAllocator::taskRemoved(Task* task) {
  placeUsage(task->framework_id(), task->executor_id(), task->slave_id(),
             0, task, Option<ExecutorInfo>::none());
  Slave* slave = master->getSlave(task->slave_id());
  refusers.erase(slave);
  std::vector<Slave*> slave_alone;
  slave_alone.push_back(slave);
  makeNewOffers(slave_alone);
}

void
NoRequestAllocator::executorAdded(const FrameworkID& frameworkId,
                                  const SlaveID& slaveId,
                                  const ExecutorInfo& info) {
  LOG(INFO) << "executor added " << info.DebugString();
  placeUsage(frameworkId, info.executor_id(), slaveId, 0, 0, info);
}

void
NoRequestAllocator::executorRemoved(const FrameworkID& frameworkId,
                                    const SlaveID& slaveId,
                                    const ExecutorInfo& info) {
  LOG(INFO) << "executor removed " << info.DebugString();
  tracker->forgetExecutor(frameworkId, info.executor_id(), slaveId);
}


void
NoRequestAllocator::placeUsage(const FrameworkID& frameworkId,
                               const ExecutorID& executorId,
                               const SlaveID& slaveId,
                               Task* newTask, Task* removedTask,
                               Option<ExecutorInfo> maybeExecutorInfo) {
  Slave* slave = master->getSlave(slaveId);
  // TODO(charles): this should be tracked by the Slave.
  // TODO(charles): are we sure we're always in sync?
  Resources minResources = tracker->gaurenteedForExecutor(
      slaveId, frameworkId, executorId);
  LOG(INFO) << "min = " << minResources;
  int numTasks = 0;
  typedef std::pair<FrameworkID, TaskID> FrameworkAndTaskID;
  foreachpair (FrameworkAndTaskID key, Task* task, slave->tasks) {
    if (key.first == frameworkId && task->executor_id() == executorId) {
      numTasks += 1;
    }
  }
  // Note that we don't need to adjust numTasks below because we will
  // be called _after_ the Master's information is updated.
  // TODO(charles): estimate resources more intelligently
  //                in usage tracker to centralize policy?
  Option<Resources> estimate = Option<Resources>::none();
  if (newTask) {
    estimate = Option<Resources>(
        tracker->nextUsedForExecutor(slaveId, frameworkId, executorId) +
        newTask->resources());
    minResources += newTask->min_resources();
  } else if (maybeExecutorInfo.isSome()) {
    estimate = Option<Resources>(
        tracker->nextUsedForExecutor(slaveId, frameworkId, executorId) +
        maybeExecutorInfo.get().resources());
    LOG(INFO) << "estimate = " << estimate.get();
    minResources += maybeExecutorInfo.get().min_resources();
  } else if (removedTask) {
    minResources -= removedTask->min_resources();
    if (numTasks == 0) {
      // TODO(charles): wrong for memory
      estimate = Option<Resources>::some(Resources());
    }
  }

  tracker->placeUsage(frameworkId, executorId, slaveId, minResources, estimate,
                      numTasks);
}

namespace {

struct ChargedShareComparator {
  ChargedShareComparator(UsageTracker* _tracker, Resources _totalResources)
      : tracker(_tracker), totalResources(_totalResources) {}

  bool operator()(Framework* first, Framework* second) {
    double firstShare = dominantShareOf(first);
    double secondShare =  dominantShareOf(second);
    if (firstShare == secondShare) {
      LOG(INFO) << "shares equal; copmaring "
                << first->id.value() << " and " << second->id.value()
                << " --> " << (first->id.value() < second->id.value());
      return first->id.value() < second->id.value();
    } else {
      return firstShare < secondShare;
    }
  }

  double dominantShareOf(Framework* framework) {
    // TODO(charles): is the right metric?
    Resources charge = tracker->chargeForFramework(framework->id);
    double share = 0.0;
    foreach (const Resource& resource, charge) {
      if (resource.type() == Value::SCALAR) {
        double total =
            totalResources.get(resource.name(), Value::Scalar()).value();
        if (total > 0.0) {
          share = std::max(share, resource.scalar().value() / total);
        }
      }
    }
    LOG(INFO) << "computed share of " << framework << " = " << share;
    return share;
  }

  UsageTracker *tracker;
  Resources totalResources;
};

}
vector<Framework*>
NoRequestAllocator::getOrderedFrameworks() {
  vector<Framework*> frameworks = master->getActiveFrameworks();
  std::sort(frameworks.begin(), frameworks.end(),
            ChargedShareComparator(tracker, totalResources));
  return frameworks;
}

namespace {

bool enoughResources(Resources res) {
  const double kMinCPU = 0.01;
  const double kMinMem = 16;
  return (res.get("cpus", Value::Scalar()).value() > kMinCPU &&
          res.get("mem", Value::Scalar()).value() > kMinMem);
}

Resource kNoCPU = Resources::parse("cpus", "0.0");
Resource kNoMem = Resources::parse("mem", "0.0");

void fixResources(Resources* res) {
  if (res->get(kNoCPU).isNone()) {
    *res += kNoCPU;
  }
  if (res->get(kNoMem).isNone()) {
    *res += kNoMem;
  }
}

}

void
NoRequestAllocator::makeNewOffers(const std::vector<Slave*>& slaves) {
  if (dontMakeOffers) return;
  LOG(INFO) << "makeNewOffers for " << slaves.size() << " slaves";
  vector<Framework*> orderedFrameworks = getOrderedFrameworks();

  // expected, min
  unordered_map<Slave*, ResourceHints> freeResources;
  foreach(Slave* slave, slaves) {
    LOG(INFO) << "slave " << slave << "; active = " << slave->active;
    if (!slave->active) continue;
    // TODO(charles): FIXME offered but unlaunched tracking
    Resources offered = slave->resourcesOffered.expectedResources;
    Resources gaurenteedOffered = slave->resourcesOffered.minResources;
    Resources free = tracker->freeForSlave(slave->id).allocatable() - offered;
    Resources gaurenteed =
      tracker->gaurenteedFreeForSlave(slave->id).allocatable() -
      gaurenteedOffered;
    if (enoughResources(free) || enoughResources(gaurenteed)) {
      ResourceHints offer;
      fixResources(&free);
      fixResources(&gaurenteed);
      offer.expectedResources = free;
      offer.minResources = gaurenteed;
      freeResources[slave] = offer;
    } else {
      LOG(INFO) << "not enough for " << slave->id << ": "
                << free << " and " << gaurenteed;
      LOG(INFO) << "offered = " << slave->resourcesOffered;
      LOG(INFO) << "[in use] = " << slave->resourcesInUse;
      LOG(INFO) << "[observed] = "  << slave->resourcesObservedUsed;
    }
  }

  foreach (Framework* framework, orderedFrameworks) {
    hashmap<Slave*, ResourceHints> offerable;
    // TODO(charles): offer both separately;
    //                ideally frameworks should be allowed to get gaurentees
    //                of some resources (e.g. memory) and not others (e.g. CPU)
    foreachpair (Slave* slave,
                 const ResourceHints& offerRes,
                 freeResources) {
      if (!(refusers.count(slave) && refusers[slave].count(framework->id)) &&
          !framework->filters(slave, offerRes)) {
        offerable[slave] = offerRes;
        LOG(INFO) << "offering " << framework->id << " "
                  << offerRes << " on slave " << slave->id;
      } else {
        LOG(INFO) << framework->id << " not accepting offer on " << slave->id;
        LOG(INFO) << "refuser? " << (refusers.count(slave) ? "yes" : "no");
        LOG(INFO) << "filtered "
                  << framework->filters(slave, offerRes.expectedResources);
      }
    }

    LOG(INFO) << "have " << offerable.size() << " offers for " << framework->id;

    if (offerable.size() > 0) {
      foreachkey(Slave* slave, offerable) {
        freeResources.erase(slave);
      }
      master->makeOffers(framework, offerable);
    }
  }
}

void NoRequestAllocator::resourcesUnused(const FrameworkID& frameworkId,
                                         const SlaveID& slaveId,
                                         const ResourceHints& unusedResources) {
  refusers[master->getSlave(slaveId)].insert(frameworkId);
  resourcesRecovered(frameworkId, slaveId, unusedResources);
}

void NoRequestAllocator::resourcesRecovered(const FrameworkID& frameworkId,
                                            const SlaveID& slaveId,
                                            const ResourceHints& unusedResources) {
  // FIXME: do we need to inform usagetracker about this?
  std::vector<Slave*> returnedSlave;
  returnedSlave.push_back(master->getSlave(slaveId));
  makeNewOffers(returnedSlave);
}

void NoRequestAllocator::offersRevived(Framework* framework) {
  std::vector<Slave*> revivedSlaves;
  foreachpair (Slave* slave, boost::unordered_set<FrameworkID>& refuserSet,
               refusers) {
    if (refuserSet.count(framework->id)) {
      refuserSet.erase(framework->id);
      revivedSlaves.push_back(slave);
    }
  }
  makeNewOffers(revivedSlaves);
}

void NoRequestAllocator::timerTick() {
  tracker->timerTick(process::Clock::now());
  makeNewOffers(master->getActiveSlaves());
}

void NoRequestAllocator::gotUsage(const UsageMessage& update) {
  tracker->recordUsage(update);
  Slave* slave = master->getSlave(update.slave_id());
  if (slave) {
    // TODO(charles): replace or remove this hack
    foreach (Framework* framework, master->getActiveFrameworks()) {
      framework->slaveFilter.erase(slave);
    }
    refusers.erase(slave);
    vector<Slave*> singleSlave;
    singleSlave.push_back(slave);
    makeNewOffers(singleSlave);
  }
}

} // namespace norequest
} // namespace internal
} // namespace mesos
