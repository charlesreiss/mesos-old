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

#include <process/delay.hpp>
#include <process/process.hpp>
#include <process/timer.hpp>
#include <process/timeout.hpp>

#include "common/utils.hpp"
#include "norequest/allocator.hpp"
#define DO_ALLOC_USAGE_LOG
#ifdef DO_ALLOC_USAGE_LOG
#include "usage_log/usage_log.pb.h"
#endif

using boost::unordered_set;
using process::PID;
using std::vector;

namespace mesos {
namespace internal {
namespace norequest {

using process::Timeout;

// Stupidly copied (will change on later patch)
// XXX FIXME XXX
// Used to represent "filters" for resources unused in offers.
class Filter
{
public:
  virtual ~Filter() {}
  virtual bool filter(const SlaveID& slaveId, const ResourceHints& resources) = 0;
};

namespace {

class RefusedFilter : public Filter
{
public:
  RefusedFilter(const SlaveID& _slaveId,
                const ResourceHints& _resources,
                const Timeout& _timeout)
    : slaveId(_slaveId),
      resources(_resources),
      timeout(_timeout) {}

  virtual bool filter(const SlaveID& slaveId, const ResourceHints& resources)
  {
    return slaveId == this->slaveId &&
      resources <= this->resources &&
      timeout.remaining() < 0.0;
  }

  const SlaveID slaveId;
  const ResourceHints resources;
  const Timeout timeout;
};

} // XXX FIXME XXX FIXME

// XXX FIXME
void NoRequestAllocator::expire(const FrameworkID& frameworkId, Filter* filter)
{
  if (frameworks.contains(frameworkId)) {
    if (filters.contains(frameworkId, filter)) {
      filters.remove(frameworkId, filter);
      delete filter;
      // XXX FIXME slaveId
      makeNewOffers();
    } else {
      delete filter;
    }
  }
}

bool NoRequestAllocator::checkFilters(const FrameworkID& frameworkId,
    const SlaveID& slaveId, const ResourceHints& offer)
{
  foreach (Filter* filter, filters.get(frameworkId)) {
    if (filter->filter(slaveId, offer)) {
      VLOG(1) << "Filtered " << offer
              << " on slave " << slaveId
              << " for framework " << frameworkId;
      return false;
    }
  }
  return true;
}

void
NoRequestAllocator::initialize(
    const process::PID<AllocatorMasterInterface>& _master)
{
  master = _master;
  aggressiveReoffer = conf.get<bool>("norequest_aggressive", false);
  // TODO(Charles): Fix things so this is not the default.
  useCharge = conf.get<bool>("norequest_charge", false);
  useLocalPriorities = conf.get<bool>("norequest_local_prio", false);
  if (!tracker) {
    tracker = getUsageTracker(conf);
    dontDeleteTracker = false;
  }
  usageReofferDelay =  conf.get<double>("norequest_usage_reoffer_delay", -1.);
}

void
NoRequestAllocator::frameworkAdded(const FrameworkID& frameworkId,
                                   const FrameworkInfo& info) {
  frameworks[frameworkId] = info;
  allRefusers.clear();
  makeNewOffers();
}

void
NoRequestAllocator::frameworkDeactivated(const FrameworkID& frameworkId) {
  // XXX FIXME: Difference between remvoed/activated stuff.
  // XXX FIXME: Remove from bookkeeping?
  foreachvalue (hashset<FrameworkID>& refuserSet, refusers) {
    refuserSet.erase(frameworkId);
  }
  frameworks.erase(frameworkId);
  filters.erase(frameworkId);
}

void
NoRequestAllocator::slaveAdded(const SlaveID& slaveId,
                               const SlaveInfo& slaveInfo,
                               const hashmap<FrameworkID, Resources>& used) {
  CHECK_EQ(0, refusers.count(slaveId));
  slaveInfos[slaveId] = slaveInfo;
  totalResources += slaveInfo.resources();
  tracker->setCapacity(slaveId, slaveInfo.resources());
  std::vector<SlaveID> slave_alone;
  slave_alone.push_back(slaveId);
  makeNewOffers(slave_alone);
}

void
NoRequestAllocator::slaveRemoved(const SlaveID& slaveId) {
  SlaveInfo slaveInfo = slaveInfos[slaveId];
  slaveInfos.erase(slaveId);
  totalResources -= slaveInfo.resources();
  tracker->setCapacity(slaveId, Resources());
  refusers.erase(slaveId);
  allRefusers.erase(slaveId);
  waitingOffers.erase(slaveId);
}

void
NoRequestAllocator::taskAdded(
    const FrameworkID& frameworkId, const TaskInfo& task) {
  completeOffer(frameworkId, task.slave_id(),
                ResourceHints::forTaskInfo(task));
  placeUsage(frameworkId, task.executor().executor_id(), task.slave_id(),
             Option<TaskInfo>(task), Option<TaskInfo>::none(),
             Option<ExecutorInfo>::none());
  if (waitingOffers.count(task.slave_id())) {
    std::vector<SlaveID> slave_alone;
    slave_alone.push_back(task.slave_id());
    makeNewOffers(slave_alone);
  }
}

void
NoRequestAllocator::taskRemoved(
    const FrameworkID& frameworkId, const TaskInfo& task) {
  DLOG(INFO) << "task removed" << task.DebugString();
  CHECK(task.has_executor());  // TODO(Charles): Handle this case.
  placeUsage(frameworkId, task.executor().executor_id(),
             task.slave_id(),
             Option<TaskInfo>::none(),
             Option<TaskInfo>(task),
             Option<ExecutorInfo>::none());
  refusers.erase(task.slave_id());
  allRefusers.erase(task.slave_id());
  std::vector<SlaveID> slave_alone;
  slave_alone.push_back(task.slave_id());
  makeNewOffers(slave_alone);
}

void
NoRequestAllocator::executorAdded(const FrameworkID& frameworkId,
                                  const SlaveID& slaveId,
                                  const ExecutorInfo& info) {
  completeOffer(frameworkId, slaveId, ResourceHints::forExecutorInfo(info));
  placeUsage(frameworkId, info.executor_id(), slaveId,
      Option<TaskInfo>::none(), Option<TaskInfo>::none(), info);
}

void
NoRequestAllocator::executorRemoved(const FrameworkID& frameworkId,
                                    const SlaveID& slaveId,
                                    const ExecutorInfo& info) {
  DLOG(INFO) << "executor removed " << info.DebugString();
  tracker->forgetExecutor(frameworkId, info.executor_id(), slaveId);
  knownTasks.erase(ExecutorKey(frameworkId, info.executor_id(), slaveId));
  refusers.erase(slaveId);
  allRefusers.erase(slaveId);
  std::vector<SlaveID> slave_alone;
  slave_alone.push_back(slaveId);
  // TODO(Charles): Unit test for this happening
  makeNewOffers(slave_alone);
}


void
NoRequestAllocator::placeUsage(const FrameworkID& frameworkId,
                               const ExecutorID& executorId,
                               const SlaveID& slaveId,
                               Option<TaskInfo> newTask,
                               Option<TaskInfo> removedTask,
                               Option<ExecutorInfo> maybeExecutorInfo) {
  Resources minResources = tracker->gaurenteedForExecutor(
      slaveId, frameworkId, executorId);
  boost::unordered_set<TaskID>* tasks = &knownTasks[
    ExecutorKey(frameworkId, executorId, slaveId)];
  // TODO(charles): estimate resources more intelligently
  //                in usage tracker to centralize policy?
  Option<Resources> estimate = Option<Resources>::none();
  if (newTask.isSome()) {
    // TODO(Charles): Take into account Executor usage if executorAdded()
    //                not yet called.
    tasks->insert(newTask.get().task_id());
    estimate = Option<Resources>(
        tracker->nextUsedForExecutor(slaveId, frameworkId, executorId) +
        newTask.get().resources());
    minResources += newTask.get().min_resources();
  } else if (maybeExecutorInfo.isSome()) {
    estimate = Option<Resources>(
        tracker->nextUsedForExecutor(slaveId, frameworkId, executorId) +
        maybeExecutorInfo.get().resources());
    minResources += maybeExecutorInfo.get().min_resources();
  } else if (removedTask.isSome()) {
    CHECK_EQ(1, tasks->count(removedTask.get().task_id()));
    tasks->erase(removedTask.get().task_id());
    minResources -= removedTask.get().min_resources();
    if (tasks->size() == 0) {
      // TODO(charles): wrong for memory
      estimate = Option<Resources>::some(Resources());
    }
  }

  tracker->placeUsage(frameworkId, executorId, slaveId, minResources, estimate,
                      tasks->size());
}

namespace {

struct ChargedShareComparator {
  ChargedShareComparator(UsageTracker* _tracker,
                         const hashmap<FrameworkID, ResourceHints>& _frameworkOffered,
                         Resources _totalResources,
                         bool _useCharge)
      : tracker(_tracker), totalResources(_totalResources),
        useCharge(_useCharge), frameworkOffered(_frameworkOffered) {}

  bool operator()(const FrameworkID& first, const FrameworkID& second) {
    double firstShare = dominantShareOf(first);
    double secondShare = dominantShareOf(second);
    if (firstShare == secondShare) {
      return first.value() < second.value();
    } else {
      return firstShare < secondShare;
    }
  }

  double dominantShareOf(const FrameworkID& framework) {
    // TODO(charles): is the right metric?
    // TODO(Charles): Test for this!
    if (drfFor.count(framework) > 0) {
      return drfFor[framework];
    } else {
      Resources charge = useCharge ?
        tracker->chargeForFramework(framework) :
        tracker->nextUsedForFramework(framework);
      // XXX min or expected?
      if (frameworkOffered.count(framework) > 0) {
        charge += frameworkOffered.find(framework)->second.expectedResources;
      }
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
      drfFor[framework] = share;
      return share;
    }
  }

  UsageTracker *tracker;
  Resources totalResources;
  bool useCharge;
  hashmap<FrameworkID, double> drfFor;
  const hashmap<FrameworkID, ResourceHints>& frameworkOffered;
};

}


// TODO(Charles): Cache DRF calculations over longer time scales for speed.
vector<FrameworkID>
NoRequestAllocator::getOrderedFrameworks() {
  vector<FrameworkID> result;
  foreachkey (const FrameworkID& frameworkId, frameworks) {
    result.push_back(frameworkId);
  }
  std::sort(result.begin(), result.end(),
            ChargedShareComparator(tracker, frameworkOffered,
                                   totalResources, useCharge));
  return result;
}

namespace {

bool enoughResources(const Resources& res) {
  const double kMinCPU = 0.01;
  const double kMinMem = 0.01;
  return (res.get("cpus", Value::Scalar()).value() > kMinCPU &&
          res.get("mem", Value::Scalar()).value() > kMinMem);
}

Resource kNoCPU = Resources::parse("cpus", "0.0");
Resource kNoMem = Resources::parse("mem", "0.0");

double myround(double x) {
#if 0
  const int kPrecision = 20;
  int exp;
  double fraction = frexp(x, &exp);
  int factor = std::min(kPrecision + exp, kPrecision);
  fraction = ldexp(ceil(ldexp(fraction, factor)), -factor);
  double result = ldexp(fraction, exp);
  CHECK_GE(result, x);
  CHECK_LT(result, x + .0001);
  return result;
#else
  return x;
#endif
}

void fixResources(Resources* res) {
  if (res->get(kNoCPU).isNone()) {
    *res += kNoCPU;
  }
  if (res->get(kNoMem).isNone()) {
    *res += kNoMem;
  }
  foreach (Resource& resource, *res) {
    if (resource.scalar().value() < 0.0) {
      resource.mutable_scalar()->set_value(0.0);
    } else if (resource.name() == "cpus") {
      resource.mutable_scalar()->set_value(myround(resource.scalar().value()));
    }
  }
}

}

ResourceHints
NoRequestAllocator::nextOfferForSlave(const SlaveID& slaveId)
{
  Resources offer = offered[slaveId].expectedResources;
  Resources gaurenteedOffer = offered[slaveId].minResources;
  Resources free = tracker->freeForSlave(slaveId).allocatable() - offer;
  Resources gaurenteed =
    tracker->gaurenteedFreeForSlave(slaveId).allocatable() -
    gaurenteedOffer;
  fixResources(&free);
  fixResources(&gaurenteed);
  return ResourceHints(free, gaurenteed);
}

void
NoRequestAllocator::makeNewOffers(const std::vector<SlaveID>& slaves) {
  if (dontMakeOffers) return;
  if (lastTime != process::Clock::now()) {
    lastTime = process::Clock::now();
    offersSinceTimeChange = 0;
  }
  ++offersSinceTimeChange;
  if (offersSinceTimeChange > 100000000L) {
    DLOG(FATAL) << "Stuck in reoffer loop";
  }
  DLOG(INFO) << "makeNewOffers for " << slaves.size() << " slaves";
  vector<FrameworkID> orderedFrameworks = getOrderedFrameworks();

  // expected, min
  hashmap<SlaveID, ResourceHints> freeResources;
  foreachkey (const SlaveID& slaveId, slaveInfos) {
    ResourceHints toOffer = nextOfferForSlave(slaveId);
    waitingOffers.erase(slaveId);
    if (enoughResources(toOffer.expectedResources) ||
        enoughResources(toOffer.minResources)) {
      freeResources[slaveId] = toOffer;
    }
  }

  // Clear refusers on any slave that has been refused by everyone.
  // TODO(charles): consider case where offer is filtered??
  foreachkey (SlaveID slave, freeResources) {
    if (refusers.count(slave) &&
        refusers[slave].size() == orderedFrameworks.size()) {
      if (allRefusers.count(slave) == 0) {
        DVLOG(1) << "Clearing refusers for slave " << slave
                 << " because EVERYONE has refused resources from it";
        refusers.erase(slave);
        allRefusers.insert(slave);
      } else {
        DVLOG(1) << "EVERYONE has refused offers from " << slave
                 << " but we've already had it completely refused twice.";
      }
    }
  }

  foreach (const FrameworkID& framework, orderedFrameworks) {
    hashmap<SlaveID, ResourceHints> offerable;
    // TODO(charles): offer both separately;
    //                ideally frameworks should be allowed to get gaurentees
    //                of some resources (e.g. memory) and not others (e.g. CPU)
    foreachpair (const SlaveID& slave,
                 const ResourceHints& offerRes,
                 freeResources) {
      if (!(refusers.count(slave) && refusers[slave].count(framework)) &&
          checkFilters(framework, slave, offerRes)) {
        offerable[slave] = offerRes;
      }
    }

    if (offerable.size() > 0) {
      foreachkey (const SlaveID& slave, offerable) {
        freeResources.erase(slave);
      }
      foreachpair (const SlaveID& slave, const ResourceHints& resources,
          offerable) {
        accountOffer(framework, slave, resources);
      }
      dispatch(master, &AllocatorMasterInterface::offer, framework, offerable);
    }
  }
}

void NoRequestAllocator::makeNewOffers()
{
  std::vector<SlaveID> slaves;
  foreachkey (const SlaveID& slave, slaveInfos) {
    slaves.push_back(slave);
  }
  makeNewOffers(slaves);
}

void NoRequestAllocator::resourcesUnused(const FrameworkID& frameworkId,
                                         const SlaveID& slaveId,
                                         const ResourceHints& _unusedResources,
                                         const Option<Filters>& filters)
{
  // FIXME(charles): Need to account for allocatable() [and elsewhere]!
  ResourceHints unusedResources = _unusedResources;
  fixResources(&unusedResources.expectedResources);
  fixResources(&unusedResources.minResources);
  completeOffer(frameworkId, slaveId, unusedResources);

  // XXX FIXME: This should be refactored elsewhere
  double timeout = filters.isSome()
    ? filters.get().refuse_seconds()
    : Filters().refuse_seconds();

  if (timeout != 0.0) {
    LOG(INFO) << "Framework " << frameworkId
	      << " filtered slave " << slaveId
	      << " for " << timeout << " seconds";

    // Create a new filter and delay it's expiration.
    Filter* filter = new RefusedFilter(slaveId, unusedResources, timeout);
    this->filters.put(frameworkId, filter);

    // TODO(benh): Use 'this' and '&This::' as appropriate.
    delay(timeout, self(), &NoRequestAllocator::expire, frameworkId, filter);
  }

  /* Before recording a framework as a refuser, make sure we would offer
   * them at least as many resources now. If not, give them a chance to get the
   * resources we reclaimed asynchronously.
   */
  ResourceHints freeResources = nextOfferForSlave(slaveId);
  waitingOffers.erase(slaveId);
  if (freeResources <= unusedResources) {
    DLOG(INFO) << "adding refuser";
    if (enoughResources(offered[slaveId].expectedResources) ||
        enoughResources(offered[slaveId].minResources)) {
      DLOG(INFO) << "delaying response for " << slaveId.value();
      waitingOffers.insert(slaveId);
    } else {
      DLOG(INFO) << "marking " << frameworkId << " as a refuser";
      refusers[slaveId].insert(frameworkId);
    }
  }

  // XXX Can addedRefuser be true if waitingOffers.count(slave) > 0?
  if (!waitingOffers.count(slaveId)) {
    if (aggressiveReoffer) {
      makeNewOffers();
    } else {
      std::vector<SlaveID> returnedSlave;
      returnedSlave.push_back(slaveId);
      makeNewOffers(returnedSlave);
    }
  }
}

void NoRequestAllocator::resourcesRecovered(const FrameworkID& frameworkId,
                                            const SlaveID& slaveId,
                                            const ResourceHints& unusedResources) {
  // FIXME: do we need to inform usagetracker about this?
  refusers[slaveId].erase(frameworkId);
  allRefusers.erase(slaveId);
  if (aggressiveReoffer) {
    makeNewOffers();
  } else {
    std::vector<SlaveID> returnedSlave;
    returnedSlave.push_back(slaveId);
    makeNewOffers(returnedSlave);
  }
}

void NoRequestAllocator::offersRevived(const FrameworkID& frameworkId) {
  DLOG(INFO) << "offersRevived for " << frameworkId;
  std::vector<SlaveID> revivedSlaves;
  foreachpair (SlaveID slave, boost::unordered_set<FrameworkID>& refuserSet,
               refusers) {
    if (refuserSet.count(frameworkId)) {
      refuserSet.erase(frameworkId);
      revivedSlaves.push_back(slave);
    }
  }
  allRefusers.clear();
  // TODO(Charles): Can we get away with doing this for jsut revivedSlaves
  // plus allRefusers entries we actually cleared?
  makeNewOffers();
}

void NoRequestAllocator::timerTick() {
  tracker->timerTick(process::Clock::now());
  if (aggressiveReoffer) {
    // FIXME: Charles -- this is a workaround for an unknown bug where we miss
    // some time where we're supposed to remove something from refusers.
    foreachvalue (boost::unordered_set<FrameworkID>& refuserSet, refusers) {
      refuserSet.clear();
    }
  }

#ifdef DO_ALLOC_USAGE_LOG
  {
    double now(process::Clock::now());
    ChargedShareComparator comp(tracker, frameworkOffered,
                                totalResources, useCharge);
    AllocatorEstimates estimates;
    foreachkey (const FrameworkID& framework, frameworks) {
      AllocatorEstimate* estimate = estimates.add_estimate();
      estimate->set_drf(comp.dominantShareOf(framework));
      estimate->mutable_framework_id()->MergeFrom(framework);
      estimate->set_time(now);
      estimate->mutable_next_used()->MergeFrom(
          tracker->nextUsedForFramework(framework));
      estimate->mutable_charge()->MergeFrom(
          tracker->chargeForFramework(framework));
    }
    dispatch(master, &AllocatorMasterInterface::forwardAllocatorEstimates, estimates);
  }
#endif

  if (useLocalPriorities) {
    ChargedShareComparator comp(tracker, frameworkOffered,
                                totalResources, useCharge);
    FrameworkPrioritiesMessage message;
    foreachkey (const FrameworkID& framework, frameworks) {
      message.add_framework_id()->MergeFrom(framework);
      message.add_priority(std::max(0.0, 1.0 - comp.dominantShareOf(framework)));
    }
    dispatch(master, &AllocatorMasterInterface::sendFrameworkPriorities, message);
  }

  allRefusers.clear();
  makeNewOffers();
}

void NoRequestAllocator::gotUsage(const UsageMessage& update) {
  // TODO(Charles): Check whether we actually got more free resources on the
  // slave to short-circuit the reoffer; or defer reoffers until we likely have
  // a full set of usage updates.
  tracker->recordUsage(update);
  const SlaveID& slave = update.slave_id();
  if (slaveInfos.count(slave)) {
    refusers.erase(slave);
    allRefusers.erase(slave);
    if (usageReofferDelay >= 0.0) {
      usageReofferSlaves.insert(slave);
      if (!pendingReoffer) {
        pendingReoffer = true;
        usageReofferTimer = process::delay(usageReofferDelay, self(),
            &NoRequestAllocator::makeUsageReoffers);
      }
    } else {
      vector<SlaveID> singleSlave;
      singleSlave.push_back(slave);
      DLOG(INFO) << "Trying to make new offers based on usage update for "
                 << update.slave_id();
      if (aggressiveReoffer) {
        makeNewOffers();
      } else {
        makeNewOffers(singleSlave);
      }
    }
  } else {
    LOG(WARNING) << "Got usage from non-slave " << update.slave_id();
  }
}

void NoRequestAllocator::completeOffer(
    const FrameworkID& frameworkId, const SlaveID& slaveId,
    const ResourceHints& resources)
{
  offered[slaveId] -= resources;
  frameworkOffered[frameworkId] -= resources;
}

void NoRequestAllocator::accountOffer(
    const FrameworkID& frameworkId, const SlaveID& slaveId,
    const ResourceHints& resources)
{
  offered[slaveId] += resources;
  frameworkOffered[frameworkId] += resources;
}

} // namespace norequest
} // namespace internal
} // namespace mesos
