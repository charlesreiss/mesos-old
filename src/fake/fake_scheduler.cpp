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

#include <vector>

namespace mesos {
namespace internal {
namespace fake {

using std::vector;

void FakeScheduler::registered(SchedulerDriver* driver,
                               const FrameworkID& frameworkId_)
{
  frameworkId.MergeFrom(frameworkId_);
}

void FakeScheduler::resourceOffers(SchedulerDriver* driver,
                                   const std::vector<Offer>& offers)
{
  foreach (const Offer& offer, offers) {
    vector<TaskDescription> toLaunch;
    ResourceHints bucket = ResourceHints::forOffer(offer);
    foreachpair (const TaskID& taskId, FakeTask* task, tasksPending) {
      ResourceHints curRequest = task->getResourceRequest();
      if (curRequest <= bucket) {
        TaskDescription newTask;
        newTask.set_name("dummy-name");
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
        tasksRunning[taskId] = task;
        LOG(INFO) << "placed " << task << " in " << newTask.DebugString();
      } else {
        LOG(INFO) << "rejected " << task << "; only " << bucket << " versus "
                  << curRequest;
      }
    }
    foreach (const TaskDescription& task, toLaunch) {
      tasksPending.erase(task.task_id());
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

  switch (status.state()) {
  case TASK_STARTING: case TASK_RUNNING:
    break;
  case TASK_FINISHED:
    {
      tasksRunning.erase(status.task_id());
      ExecutorID executorId;
      executorId.set_value(status.task_id().value());
      taskTracker->unregisterTask(frameworkId, executorId, status.task_id());
    }
    break;
  case TASK_FAILED: case TASK_KILLED: case TASK_LOST:
    tasksPending[status.task_id()] = tasksRunning[status.task_id()];
    tasksRunning.erase(status.task_id());
    driver->reviveOffers();
    break;
  }
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
