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

#ifndef __NOREQUEST_USAGE_TRACKER_HPP__
#define __NOREQUEST_USAGE_TRACKER_HPP__

#include "common/resources.hpp"
#include "master/allocator.hpp"

namespace mesos {
namespace internal {
namespace norequest {

// abstract base class to aid testing.
class UsageTracker {
public:
  virtual void recordUsage(const UsageMessage& update) = 0;
  virtual void placeUsage(const FrameworkID& frameworkId,
                          const ExecutorID& executorId,
                          const SlaveID& slaveId,
                          const Resources& minResources,
                          const Option<Resources>& estResources,
                          int numTasks) = 0;
  virtual void forgetExecutor(const FrameworkID& frameworkId,
                              const ExecutorID& executorId,
                              const SlaveID& slaveId) = 0;
  virtual void setCapacity(const SlaveID& slaveId,
                           const Resources& resources) = 0;
  virtual void timerTick(double cur_time) = 0;

  virtual Resources nextUsedForExecutor(const SlaveID& slaveId,
                                        const FrameworkID& frameworkId,
                                        const ExecutorID& executorId)
    const = 0;
  virtual Resources gaurenteedForExecutor(const SlaveID& slaveId,
                                          const FrameworkID& frameworkId,
                                          const ExecutorID& executorId)
    const = 0;
  virtual Resources chargeForFramework(const FrameworkID& frameworkId)
    const = 0;
  virtual Resources nextUsedForFramework(const FrameworkID& frameworkId)
    const = 0;
  virtual Resources usedForFramework(const FrameworkID& frameworkId)
    const = 0;
  virtual Resources gaurenteedForFramework(const FrameworkID& frameworkId)
    const = 0;
  virtual Resources freeForSlave(const SlaveID& slaveId) const = 0;
  virtual Resources gaurenteedFreeForSlave(const SlaveID& slaveId) const = 0;

  virtual ~UsageTracker() {}
};

UsageTracker* getUsageTracker();

} // namespace norequest {
} // namespace internal {
} // namespace mesos {

#endif
