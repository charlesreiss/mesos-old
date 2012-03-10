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

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/scoped_ptr.hpp>

#include "norequest/usage_tracker.hpp"

#include "common/type_utils.hpp"
#include "master/allocator.hpp"
#include "master/master.hpp"

namespace mesos {
namespace internal {
namespace norequest {

using master::Allocator;
using master::AllocatorMasterInterface;
using master::Framework;
using master::Slave;

class NoRequestAllocator : public Allocator {
public:
  // XXX FIXME pass Configuration for real
  NoRequestAllocator() :
    dontDeleteTracker(true), dontMakeOffers(false), tracker(0),
    master(0), aggressiveReoffer(false), useCharge(false) {}

  NoRequestAllocator(AllocatorMasterInterface* _master,
                     UsageTracker* _tracker) :
    dontDeleteTracker(true), dontMakeOffers(false), tracker(_tracker),
    master(_master), aggressiveReoffer(false), useCharge(false) { }

  ~NoRequestAllocator() {
    if (!dontDeleteTracker && tracker) {
      delete tracker;
    }
  }

  void initialize(AllocatorMasterInterface* _master, const Configuration& _conf) {
    master = _master;
    conf = _conf;
    aggressiveReoffer = conf.get<bool>("norequest_aggressive", false);
    // TODO(Charles): Fix things so this is not the default.
    useCharge = conf.get<bool>("norequest_charge", false);
    if (!tracker) {
      tracker = getUsageTracker(conf);
      dontDeleteTracker = false;
    }
  }

  void initialize(master::Master* _master, const Configuration& _conf) {
    master = _master;
    conf = _conf;
    aggressiveReoffer = conf.get<bool>("norequest_aggressive", false);
    useCharge = conf.get<bool>("norequest_charge", false);
    LOG(INFO) <<  "aggressive = " << aggressiveReoffer;
    if (!tracker) {
      tracker = getUsageTracker(conf);
      dontDeleteTracker = false;
    }
  }

  void frameworkAdded(Framework* framework);
  void frameworkRemoved(Framework* framework) {}
  void slaveAdded(Slave* slave);
  void slaveRemoved(Slave* slave);
  void taskAdded(Task* task);
  void taskRemoved(Task* task);
  void executorAdded(const FrameworkID& frameworkId,
                     const SlaveID& slaveId,
                     const ExecutorInfo& info);
  void executorRemoved(const FrameworkID& frameworkId,
                       const SlaveID& slaveId,
                       const ExecutorInfo& info);
  void resourcesUnused(const FrameworkID& frameworkId,
                       const SlaveID& slaveId,
                       const ResourceHints& unusedResources);
  void resourcesRecovered(const FrameworkID& frameworkId,
                          const SlaveID& slaveId,
                          const ResourceHints& resources);
  void offersRevived(Framework* framework);
  void timerTick();
  void gotUsage(const UsageMessage& update);

  // public for testing
  std::vector<Framework*> getOrderedFrameworks();
  void stopMakingOffers() { dontMakeOffers = true; }
  void startMakingOffers() { dontMakeOffers = false; }
private:
  void makeNewOffers(const std::vector<Slave*>& slaves);
  void placeUsage(const FrameworkID& frameworkId,
                  const ExecutorID& executorId,
                  const SlaveID& slaveId,
                  Task* newTask, Task* removedTask,
                  Option<ExecutorInfo> maybeExecutorInfo);

  Resources totalResources;
  AllocatorMasterInterface* master;
  UsageTracker* tracker;
  bool dontDeleteTracker;
  bool dontMakeOffers;
  Configuration conf;
  bool aggressiveReoffer;
  bool useCharge;

  boost::unordered_map<Slave*, boost::unordered_set<FrameworkID> > refusers;

  // We keep track of the set of known tasks here so we can incrementally
  // update our estimates. Otherwise, we will be confused when, e.g.,
  // an executor and a task are added/removed simulatenously.
  boost::unordered_map<ExecutorKey, boost::unordered_set<Task*> > knownTasks;
};

} // namespace norequest
} // namespace internal
} // namespace mesos

#endif
