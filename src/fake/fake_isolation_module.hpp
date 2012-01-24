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

#ifndef __FAKE_ISOLATION_MODULE_HPP__
#define __FAKE_ISOLATION_MODULE_HPP__

#include "boost/scoped_ptr.hpp"

#include "common/type_utils.hpp"
#include "slave/isolation_module.hpp"
#include "mesos/mesos.hpp"
#include "common/resources.hpp"

#include "mesos/executor.hpp"

#include "fake/fake_task.hpp"

namespace mesos {
namespace internal {
namespace fake {

using mesos::internal::slave::IsolationModule;
using mesos::internal::slave::Slave;

class FakeIsolationModule;

class FakeExecutor : public Executor {
public:
  FakeExecutor(FakeIsolationModule* module_) : module(module_) {}

  void init(ExecutorDriver* driver, const ExecutorArgs& args) {
  }

  void launchTask(ExecutorDriver* driver, const TaskDescription& task);

  void killTask(ExecutorDriver* driver, const TaskID& taskId);

  void frameworkMessage(ExecutorDriver* driver, const std::string& data) {
    LOG(FATAL) << "should not get framework message";
  }

  void shutdown(ExecutorDriver* driver) {
  }

  void error(ExecutorDriver* driver, int code, const std::string& message) {
    LOG(ERROR) << "FakeExecutor; code = " << code << "; message = " << message;
  }

  virtual ~FakeExecutor() {}
private:
  FakeIsolationModule* module;
};


class FakeIsolationModule : public IsolationModule {
public:
  void initialize(const Configuration& conf, bool local,
                  const process::PID<Slave>& slave);

  void launchExecutor(const FrameworkID& frameworkId,
                      const FrameworkInfo& frameworkInfo,
                      const ExecutorInfo& executorInfo,
                      const std::string& directory,
                      const ResourceHints& resources);

  void killExecutor(const FrameworkID& frameworkId,
                    const ExecutorID& executorId);

  void resourcesChanged(const FrameworkID& frameworkId,
                        const ExecutorID& executorId,
                        const ResourceHints& resources) {}

  /* for calling elsewhere */
  void registerTask(const FrameworkID& frameworkId,
                    const ExecutorID& executorId,
                    const TaskID& taskId,
                    mesos::internal::fake::FakeTask* fakeTask) {}
  void unregisterTask(const FrameworkID& frameworkId,
                      const ExecutorID& executorId,
                      const TaskID& taskId) {}

private:
  friend class FakeExecutor;

  // We only have one instance of an executor, with multiple executor
  // drivers calling it.
  boost::scoped_ptr<FakeExecutor> executor;
  typedef hashmap<std::pair<FrameworkID, ExecutorID>, MesosExecutorDriver*> DriverMap;
  DriverMap drivers;
  // For now, only one task per executor
  typedef hashmap<std::pair<FrameworkID, ExecutorID>, FakeTask*> TaskMap;
  TaskMap tasks;
  process::PID<Slave> slave;
};

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
