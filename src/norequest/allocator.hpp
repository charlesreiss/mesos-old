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

#ifndef __NOREQUEST_ALLOCATOR__
#define __NOREQUEST_ALLOCATOR__

#include <vector>

#include <boost/scoped_ptr.hpp>

#include <process/timer.hpp>

#include "norequest/usage_tracker.hpp"

#include "common/type_utils.hpp"
#include "master/allocator.hpp"
#include "master/master.hpp"

namespace mesos {
namespace internal {
namespace norequest {

using master::Allocator;
using master::AllocatorMasterInterface;
using master::Master;

struct Filter;

class NoRequestAllocator : public Allocator, public process::Process<NoRequestAllocator> {
public:
  // XXX FIXME pass Configuration for real
  NoRequestAllocator(const Configuration& _conf) :
    dontDeleteTracker(true), dontMakeOffers(false), tracker(0),
    aggressiveReoffer(false), useCharge(false), lastTime(0.0),
    offersSinceTimeChange(0), usageReofferDelay(-1.),
    conf(_conf) {}

  NoRequestAllocator(UsageTracker* _tracker, const Configuration& _conf) :
    dontDeleteTracker(true), dontMakeOffers(false), tracker(_tracker),
    aggressiveReoffer(false), useCharge(false), lastTime(0.0),
    offersSinceTimeChange(0), usageReofferDelay(-1.),
    conf(_conf) {}

  using process::Process<NoRequestAllocator>::self;

  void initialize(const process::PID<Master>& _master) {
    initialize(_master.operator process::PID<AllocatorMasterInterface>());
  }
  void initialize(const process::PID<AllocatorMasterInterface>& _master);
  void resourcesRequested(const FrameworkID& frameworkId,
      const std::vector<Request>& requests) {}

  void updateWhitelist(const Option<hashset<std::string> >&) {
    LOG(FATAL) << "unimplemented";
  }

  virtual void frameworkAdded(const FrameworkID& frameworkId,
                              const FrameworkInfo& frameworkInfo);

  virtual void frameworkDeactivated(const FrameworkID& frameworkId);
  virtual void frameworkRemoved(const FrameworkID& frameworkId) {
    frameworkDeactivated(frameworkId); // XXX does this need to be different?
  }

  virtual void slaveAdded(const SlaveID& slaveId,
                          const SlaveInfo& slaveInfo,
                          const hashmap<FrameworkID, Resources>& used);

  virtual void slaveRemoved(const SlaveID& slaveId);

  void taskAdded(const FrameworkID& frameworkID, const TaskInfo& task);
  void taskRemoved(const FrameworkID& frameworkId, const TaskInfo& task);
  void executorAdded(const FrameworkID& frameworkId,
                     const SlaveID& slaveId,
                     const ExecutorInfo& info);
  void executorRemoved(const FrameworkID& frameworkId,
                       const SlaveID& slaveId,
                       const ExecutorInfo& info);
  void resourcesUnused(const FrameworkID& frameworkId,
                       const SlaveID& slaveId,
                       const ResourceHints& unusedResources,
                       const Option<Filters>& filters);
  void resourcesRecovered(const FrameworkID& frameworkId,
                          const SlaveID& slaveId,
                          const ResourceHints& resources);
  void offersRevived(const FrameworkID& framework);
  void timerTick();
  void gotUsage(const UsageMessage& update);

  // public for testing
  std::vector<FrameworkID> getOrderedFrameworks();
  void stopMakingOffers() { dontMakeOffers = true; }
  void startMakingOffers() { dontMakeOffers = false; }
  void wipeOffers() {
    allRefusers.clear();
    refusers.clear();
    offered.clear();
    frameworkOffered.clear();
    filters.clear();
  }

private:
  void removeFiltersFor(const FrameworkID& framework);

  void makeUsageReoffers() {
    pendingReoffer = false;
    std::vector<SlaveID> offerSlaves(
        usageReofferSlaves.begin(), usageReofferSlaves.end());
    usageReofferSlaves.clear();
    makeNewOffers(offerSlaves);
  }
  void makeNewOffers(const std::vector<SlaveID>& slaves);
  void makeNewOffers();
  void placeUsage(const FrameworkID& frameworkId,
                  const ExecutorID& executorId,
                  const SlaveID& slaveId,
                  Option<TaskInfo> newTask, Option<TaskInfo> removedTask,
                  Option<ExecutorInfo> maybeExecutorInfo);
  void completeOffer(
      const FrameworkID& frameworkId,
      const SlaveID& slaveId,
      const ResourceHints& resources);

  void accountOffer(
      const FrameworkID& frameworkId,
      const SlaveID& slaveId,
      const ResourceHints& resources);

  bool checkFilters(const FrameworkID& frameworkId, const SlaveID& slaveId,
                    const ResourceHints& offer);
  void expire(const FrameworkID& frameworkID, Filter* filter);

  ResourceHints nextOfferForSlave(const SlaveID& slave);

  Resources totalResources;
  process::PID<AllocatorMasterInterface> master;
  UsageTracker* tracker;
  bool dontDeleteTracker;
  bool dontMakeOffers;
  Configuration conf;
  bool aggressiveReoffer;
  bool useCharge;
  double lastTime;
  int offersSinceTimeChange;

  // A framework enters refusers when we learn that it left part of an offer we
  // gave it unused. We will not reoffer resources to this framework until:
  // - a framework releases allocated resources on that slave;
  // - we get a usage update for that slave;
  // - the framework makes a reviveOffers() call; or
  // - all frameworks are refusers (see allRefusers)
  boost::unordered_map<SlaveID, hashset<FrameworkID> > refusers;
  // This is a `second-chance' list for when all frameworks refuse a slave. When
  // all frameworks refuse a slave, we add it to this set and clear the refuser
  // list for the slave. If all frameworks refuse a slave again (without us
  // having removed the slave from allRefusers), then we will not make offers
  // on the slave until the next timer tick.
  //
  // Every time an entry is cleared from refusers, we need to clear the
  // corresponding allRefusers entry.
  boost::unordered_set<SlaveID> allRefusers;

  // We want to make sure we eventually consolidate offers that are being
  // refused. Whenever an offer is returned unused and more resources are not
  // available immediately, we do not offer the resources again immediately if
  // there are resources in offers in flight with which it might be combined.
  //
  // Instead, we will delay responding to the offer until the next natural
  // occassion to make an offer for that slave (which may be another offer
  // refusal). As a special case, if the pending offer is completely accepted
  // after we delay responding to a prior offer, we need to make sure we reoffer
  // the prior offer at that time.
  //
  // Note that we still always offer on timer ticks, which gaurentees that we
  // make progress in spite of a framework hoarding offers.
  boost::unordered_set<SlaveID> waitingOffers;

  // We keep track of the set of known tasks here so we can incrementally
  // update our estimates. Otherwise, we will be confused when, e.g.,
  // an executor and a task are added/removed simulatenously.
  boost::unordered_map<ExecutorKey, hashset<TaskID> > knownTasks;

  // After receiving a usage message, wait this long to actually send new
  // offers. If less than 0, do it immediately. Configured from
  // norequest_usage_reoffer_delay
  double usageReofferDelay;
  hashset<SlaveID> usageReofferSlaves;
  bool pendingReoffer;
  process::Timer usageReofferTimer;
  process::Timer tickTimer;

  bool useLocalPriorities;

  // Master state tracking:
  hashmap<FrameworkID, FrameworkInfo> frameworks;
  hashmap<SlaveID, SlaveInfo> slaveInfos;
  hashmap<SlaveID, ResourceHints> offered;
  hashmap<FrameworkID, ResourceHints> frameworkOffered;
  multihashmap<FrameworkID, Filter*> filters;
};

} // namespace norequest
} // namespace internal
} // namespace mesos

#endif
