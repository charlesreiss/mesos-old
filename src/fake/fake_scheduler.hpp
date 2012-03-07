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

#ifndef __FAKE_SCHEDULER_HPP__
#define __FAKE_SCHEDULER_HPP__

#include <glog/logging.h>

#include "fake/fake_task.hpp"

#include "common/type_utils.hpp"
#include "common/attributes.hpp"

#include <mesos/scheduler.hpp>

#include <map>


namespace mesos {
namespace internal {
namespace fake {

using std::map;

class FakeScheduler : public Scheduler {
public:
  FakeScheduler(const Attributes& attributes_,
                FakeTaskTracker* taskTracker_)
    : attributes(attributes_), taskTracker(taskTracker_),
      haveMinRequest(false) {}
  void registered(SchedulerDriver* driver, const FrameworkID& frameworkId);
  void resourceOffers(SchedulerDriver* driver,
                      const std::vector<Offer>& offers);
  void offerRescinded(SchedulerDriver* driver,
                      const OfferID& offerId);
  void statusUpdate(SchedulerDriver* driver,
                    const TaskStatus& status);
  void frameworkMessage(SchedulerDriver* driver,
                        const SlaveID& slaveId,
                        const ExecutorID& executorId,
                        const std::string& data) {
    LOG(FATAL) << "unexpected framework message " << data;
  }
  void slaveLost(SchedulerDriver* driver,
                 const SlaveID& slaveId) {}
  void error(SchedulerDriver* driver, int code, const std::string& message) {
    LOG(ERROR) << "fake scheduler error: " << code << ": " << message;
  }

  void setTasks(const map<TaskID, FakeTask*>& tasks_) {
    tasksPending = tasks_;
    updateMinRequest();
  }

  void addTask(const TaskID& taskId, FakeTask* task) {
    tasksPending[taskId] = task;
    updateMinRequest(task->getResourceRequest());
  }

  int countPending() const {
    return tasksPending.size();
  }

  int countRunning() const {
    return tasksRunning.size();
  }

  const Attributes& getAttributes() const {
    return attributes;
  }

  int count(TaskState terminalState) const
  {
    map<TaskState, int>::const_iterator it = numTerminal.find(terminalState);
    if (it != numTerminal.end()) {
      return it->second;
    } else {
      return 0;
    }
  }

private:
  void updateMinRequest();
  void updateMinRequest(const ResourceHints& request);

  FakeTaskTracker* taskTracker;
  map<TaskID, FakeTask*> tasksPending;
  map<TaskID, FakeTask*> tasksRunning;
  FrameworkID frameworkId;
  Attributes attributes;
  map<TaskState, int> numTerminal;

  bool haveMinRequest;
  ResourceHints minRequest;
};

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
