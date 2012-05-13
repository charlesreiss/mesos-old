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

#include "fake/fake_scheduler.hpp"

#include <iomanip>
#include <vector>

namespace mesos {
namespace internal {
namespace fake {

using std::vector;

void FakeScheduler::registered(SchedulerDriver* driver_,
                               const FrameworkID& frameworkId_)
{
  CHECK(!driver) << "excess registration for " << frameworkId_
                 << "; new driver = " << (void*) driver_
                 << "; old driver = " << (void*) driver
                 << "; old id = " << frameworkId;
  driver = driver_;
  frameworkId.MergeFrom(frameworkId_);
}

bool FakeScheduler::mightAccept(const ResourceHints& resources) const
{
  bool beforeStartTime = process::Clock::now() <= startTime;
  if (beforeStartTime) {
    return false;
  } else {
    foreachvalue (FakeTask* task, tasksPending) {
      ResourceHints curRequest = task->getResourceRequest();
      if (curRequest <= resources) {
        return true;
      }
    }
    return false;
  }
}

void FakeScheduler::resourceOffers(SchedulerDriver* driver,
                                   const std::vector<Offer>& offers)
{
  bool beforeStartTime = process::Clock::now() < startTime;
  if (beforeStartTime) {
    CHECK(!passedStartTime) << (process::Clock::now() - startTime);
  }

  if (tasksPending.size() == 0) {
    foreach (const Offer& offer, offers) {
      vector<TaskDescription> toLaunch;
      driver->launchTasks(offer.id(), toLaunch);
    }
    return;
  }

  foreach (const Offer& offer, offers) {
    DLOG(INFO) << "got offer " << offer.DebugString();
    DLOG(INFO) << attributes << ": tasksPending has "
              << tasksPending.size() << " entries.";
    DCHECK_EQ(offer.framework_id(), frameworkId);
    vector<TaskDescription> toLaunch;
    ResourceHints bucket = ResourceHints::forOffer(offer);
    DLOG(INFO) << "minRequest = " << minRequest << "; bucket = " << bucket
               << "; beforeStartTime = " << beforeStartTime
               << "; now - startTime = " << (process::Clock::now() - startTime);
    if (!beforeStartTime && (!haveMinRequest || minRequest <= bucket)) {
      foreachpair (const std::string& baseTaskId, FakeTask* task, tasksPending) {
        ResourceHints curRequest = task->getResourceRequest();
        if (curRequest <= bucket) {
          TaskID taskId;
          ++taskCount;
          taskId.set_value(baseTaskId + ":" + boost::lexical_cast<std::string>(taskCount));
          TaskDescription newTask;
          newTask.set_name(baseTaskId);
          newTask.mutable_task_id()->MergeFrom(taskId);
          newTask.mutable_slave_id()->MergeFrom(offer.slave_id());
          newTask.mutable_resources()->MergeFrom(curRequest.expectedResources);
          newTask.mutable_min_resources()->MergeFrom(curRequest.minResources);
          newTask.mutable_executor()->mutable_executor_id()->set_value(
              taskId.value());
          newTask.mutable_executor()->set_uri("no-executor");
          toLaunch.push_back(newTask);
          bucket -= curRequest;
          taskTracker->registerTask(frameworkId,
              newTask.executor().executor_id(), taskId, task);
          tasksRunning[baseTaskId] = task;
          runningTaskIds[newTask.task_id()] = baseTaskId;
          if (!(minRequest <= bucket)) {
            break;
          }
          DLOG(INFO) << "placed " << *task << " in " << newTask.DebugString();
        } else {
          DLOG(INFO) << "rejected " << *task << "; only " << bucket << " versus "
                     << curRequest;
        }
      }
      foreach (const TaskDescription& task, toLaunch) {
        tasksPending.erase(task.name());
      }
    }
    driver->launchTasks(offer.id(), toLaunch);
  }
}

void FakeScheduler::offerRescinded(SchedulerDriver* driver,
                                   const OfferID& offerId)
{
}

void FakeScheduler::statusUpdate(SchedulerDriver* driver,
                                 const TaskStatus& status)
{
  if (status.state() != TASK_STARTING && status.state() != TASK_RUNNING) {
    numTerminal[status.state()]++;
  }

  if (!runningTaskIds.count(status.task_id())) {
    LOG(WARNING) << "Late status update " << status.DebugString();
    return;
  }

  std::string decodedTaskLabel = runningTaskIds[status.task_id()];

  // TODO(Charles): Handle "delayed" updates for old ids for old ids

  switch (status.state()) {
  case TASK_STARTING: case TASK_RUNNING:
    break;
  case TASK_FINISHED:
    {
      map<std::string, FakeTask*>::iterator it =
        tasksRunning.find(decodedTaskLabel);
      CHECK(it != tasksRunning.end());
      CHECK(it->second);
      finishedScore += it->second->getScore();
      tasksRunning.erase(it);
      runningTaskIds.erase(status.task_id());
      ExecutorID executorId;
      executorId.set_value(status.task_id().value());
      taskTracker->unregisterTask(frameworkId, executorId, status.task_id());
    }
    break;
  case TASK_FAILED: case TASK_KILLED: case TASK_LOST:
    {
      map<std::string, FakeTask*>::iterator it =
        tasksRunning.find(decodedTaskLabel);
      if (it != tasksRunning.end()) {
        tasksPending[it->first] = it->second;
        tasksRunning.erase(it);
        runningTaskIds.erase(status.task_id());
        driver->reviveOffers();
      } else {
        LOG(WARNING) << "excess termination for task ID " << status.task_id();
      }
    }
    break;
  }
}

void FakeScheduler::setStartTime(double time)
{
  CHECK(process::Clock::paused());
  startTime = time;
  passedStartTime = false;
  if (!startTimer.get()) {
    startTimer.reset(new SchedulerStartTimerProcess(this));
    process::UPID pid = process::spawn(startTimer.get());
    LOG(INFO) << "scheduler is " << pid;
  }
  startTimer->setTime(time);
}

void SchedulerStartTimerProcess::gotStartTime()
{
  LOG(INFO) << "gotStartTime(): " << std::setprecision(9) << std::fixed <<
    process::Clock::now();
  CHECK(process::Clock::paused());
  scheduler->atStartTime();
}

void FakeScheduler::atStartTime()
{
  LOG(INFO) << "atStartTime: " << attributes;
  CHECK_LT(process::Clock::now() - startTime, 1.0);
  CHECK_GE(process::Clock::now() - startTime, 0.0);
  passedStartTime = true;
  if (driver) {
    driver->reviveOffers();
  }
}

void FakeScheduler::updateMinRequest(const ResourceHints& resources)
{
  if (haveMinRequest) {
    minRequest = minResources(minRequest, resources);
  } else {
    minRequest = resources;
    haveMinRequest = true;
  }
}

void FakeScheduler::updateMinRequest()
{
  haveMinRequest = false;
  foreachvalue (FakeTask* task, tasksPending) {
    updateMinRequest(task->getResourceRequest());
  }
  foreachvalue (FakeTask* task, tasksRunning) {
    updateMinRequest(task->getResourceRequest());
  }
  if (driver) {
    driver->reviveOffers();
  }
}

double FakeScheduler::getScore() const
{
  double score = finishedScore;
  foreachvalue (FakeTask* task, tasksPending) {
    CHECK(task);
    score += task->getScore();
  }
  foreachvalue (FakeTask* task, tasksRunning) {
    CHECK(task);
    score += task->getScore();
  }
  return score;
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
