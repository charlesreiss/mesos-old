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

#include "norequest/usage_tracker.hpp"

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
  NoRequestAllocator() :
    dontMakeOffers(false), tracker(new UsageTrackerImpl), master(0) { }
  NoRequestAllocator(AllocatorMasterInterface* _master,
                     UsageTracker* _tracker) :
    dontMakeOffers(false), tracker(_tracker), master(_master) { }
  ~NoRequestAllocator() {}

  void initialize(AllocatorMasterInterface* _master) {
    master = _master;
  }
  void initialize(master::Master* _master) {
    master = _master;
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
  bool dontMakeOffers;

  boost::unordered_map<Slave*, boost::unordered_set<FrameworkID> > refusers;
};

} // namespace norequest
} // namespace internal
} // namespace mesos

#endif
