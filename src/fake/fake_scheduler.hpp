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

#include <process/timer.hpp>

#include "fake/fake_task.hpp"

#include "boost/scoped_ptr.hpp"

#include "common/type_utils.hpp"
#include "common/attributes.hpp"

#include <mesos/scheduler.hpp>

#include <algorithm>
#include <map>


namespace mesos {
namespace internal {
namespace fake {

using std::map;

class FakeScheduler;

// We use this process to ensure that we execute reviveOffers() from a
// process context, so timing information is passed through.
struct SchedulerStartTimerProcess
    : public process::Process<SchedulerStartTimerProcess> {
  SchedulerStartTimerProcess(FakeScheduler* scheduler_) : scheduler(scheduler_)
  {}

  void realSetTime(double time) {
    CHECK(process::Clock::paused());
    process::timers::cancel(startTimer);
    const double delta = time - process::Clock::now();
    LOG(INFO) << "delta = " << delta;
    CHECK_GT(delta, 0.0);
    startTimer = delay(delta, self(),
        &SchedulerStartTimerProcess::gotStartTime);
  }

  void setTime(double time) {
    process::dispatch(self(), &SchedulerStartTimerProcess::realSetTime,
        time);
  }

  void gotStartTime();

  virtual ~SchedulerStartTimerProcess() {
    process::timers::cancel(startTimer);
  }

  FakeScheduler* scheduler;
  process::Timer startTimer;
};

class FakeScheduler : public Scheduler {
public:
  FakeScheduler(const Attributes& attributes_,
                FakeTaskTracker* taskTracker_)
    : attributes(attributes_), taskTracker(taskTracker_),
      haveMinRequest(false), startTime(0), driver(0),
      finishedScore(0), taskCount(0), passedStartTime(true) {}

  virtual ~FakeScheduler() {
    if (startTimer.get()) {
      process::terminate(startTimer.get());
      process::wait(startTimer.get());
    }
  }
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

  void setStartTime(double time);

  double getStartTime(double minTime) const
  {
    return std::max(0.0, startTime - minTime);
  }

  void setTasks(const map<std::string, FakeTask*>& tasks_) {
    tasksPending = tasks_;
    updateMinRequest();
  }

  void setTasks(const map<TaskID, FakeTask*>& tasks_) {
    tasksPending.clear();
    foreachpair(const TaskID& baseTaskIdAsId, FakeTask* task, tasks_) {
      tasksPending[baseTaskIdAsId.value()] = task;
    }
    updateMinRequest();
  }

  void addTask(const std::string& taskId, FakeTask* task) {
    CHECK(task);
    tasksPending[taskId] = task;
    updateMinRequest(task->getResourceRequest());
  }

  void addTask(const TaskID& _taskId, FakeTask* task) {
    addTask(_taskId.value(), task);
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

  // True if we would accept these resources anywhere. Only for debugging.
  bool mightAccept(const ResourceHints& resources) const;

  double getScore() const;

  void atStartTime();

private:
  void updateMinRequest();
  void updateMinRequest(const ResourceHints& request);

  FakeTaskTracker* taskTracker;
  map<std::string, FakeTask*> tasksPending;
  map<std::string, FakeTask*> tasksRunning;
  map<TaskID, std::string> runningTaskIds;
  FrameworkID frameworkId;
  Attributes attributes;
  map<TaskState, int> numTerminal;

  bool haveMinRequest;
  ResourceHints minRequest;

  boost::scoped_ptr<SchedulerStartTimerProcess> startTimer;
  double startTime;
  SchedulerDriver* driver;
  double finishedScore;
  int taskCount;
  bool passedStartTime;
};

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
