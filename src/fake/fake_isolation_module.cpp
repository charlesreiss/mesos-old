#include <boost/bind.hpp>

#include <glog/logging.h>

#include <process/dispatch.hpp>
#include <process/process.hpp>
#include <process/timer.hpp>

#include "fake/fake_isolation_module.hpp"
#include "mesos/executor.hpp"
#include "slave/slave.hpp"

namespace mesos {
namespace internal {
namespace fake {

using std::make_pair;

void FakeExecutor::launchTask(ExecutorDriver* driver,
                              const TaskDescription& task)
{
  // TODO(Charles Reiss): Locking??
  FakeTaskMap::const_iterator it =
    fakeTasks.find(make_pair(frameworkId, task.task_id()));
  CHECK(it != fakeTasks.end());
  module->registerTask(frameworkId, executorId, task.task_id(), it->second);
}

void FakeExecutor::killTask(ExecutorDriver* driver,
                            const TaskID& taskId)
{
  TaskStatus status;
  status.mutable_task_id()->MergeFrom(taskId);
  status.set_state(TASK_KILLED);
  driver->sendStatusUpdate(status);

  module->unregisterTask(frameworkId, executorId, taskId);
}

void FakeIsolationModule::initialize(const Configuration& conf, bool local,
    const process::PID<Slave>& slave_)
{
  slave = slave_;
  interval = conf.get<double>("fake_interval", 1.0);
  lastTime = process::Clock::now();
  process::timers::create(interval,
      boost::bind(&FakeIsolationModule::tick, *this));
  CHECK(local);
}

void FakeIsolationModule::launchExecutor(
    const FrameworkID& frameworkId, const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo, const std::string& directory,
    const ResourceHints& resources) {
  FakeExecutor* executor = new FakeExecutor(this, fakeTasks);
  MesosExecutorDriver* driver = new MesosExecutorDriver(executor);
  drivers[make_pair(frameworkId, executorInfo.executor_id())] =
    make_pair(driver, executor);
  tasks[make_pair(frameworkId, executorInfo.executor_id())].fakeTask = 0;
  LOG(INFO) << "starting driver";
  driver->start(true, std::string(slave), frameworkId.value(),
                executorInfo.executor_id().value(),
                "ignored-directory-name");
  LOG(INFO) << "started driver";
}

void FakeIsolationModule::killExecutor(
    const FrameworkID& frameworkId, const ExecutorID& executorId) {
  LOG(INFO) << "asked to kill executor";
  DriverMap::iterator it = drivers.find(make_pair(frameworkId, executorId));
  if (it != drivers.end()) {
    LOG(INFO) << "about to stop driver";
    MesosExecutorDriver* driver = it->second.first;
    FakeExecutor* executor = it->second.second;
    drivers.quick_erase(it);
    driver->stop();
    delete driver;
    delete executor;
  }
  tasks.erase(make_pair(frameworkId, executorId));
}

void FakeIsolationModule::resourcesChanged(const FrameworkID& frameworkId,
    const ExecutorID& executorId, const ResourceHints& resources)
{
  tasks[make_pair(frameworkId, executorId)].assignedResources = resources;
}

void FakeIsolationModule::registerTask(
    const FrameworkID& frameworkId, const ExecutorID& executorId,
    const TaskID& taskId, FakeTask* task)
{
  RunningTaskInfo* taskInfo = &tasks[make_pair(frameworkId, executorId)];
  CHECK(!taskInfo->fakeTask);
  taskInfo->taskId.MergeFrom(taskId);
  taskInfo->fakeTask = task;
}

void FakeIsolationModule::unregisterTask(
    const FrameworkID& frameworkId, const ExecutorID& executorId,
    const TaskID& taskId)
{
  RunningTaskInfo* taskInfo = &tasks[make_pair(frameworkId, executorId)];
  CHECK_EQ(taskInfo->taskId, taskId);
  CHECK_NOTNULL(taskInfo->fakeTask);
  taskInfo->fakeTask = 0;
}

namespace {

Resources minResources(Resources a, Resources b) {
  Resources both = a + b;
  Resources result;
  foreach (const Resource& resource, both) {
    Option<Resource> resA = a.get(resource);
    Option<Resource> resB = b.get(resource);
    if (resA.isSome() && resB.isSome()) {
      result += resB.get() <= resA.get() ? resB.get() : resA.get();
    }
  }
  return result;
}

}  // unnamed namespace

void FakeIsolationModule::tick() {
  process::timers::create(interval,
      boost::bind(&FakeIsolationModule::tick, *this));

  seconds oldTime(lastTime);
  seconds newTime(process::Clock::now());

  // Version 0: Ignore min_* requests. For each task, assume that the task
  // gets min(its usage, its allocation).

  typedef std::pair<FrameworkID, ExecutorID> FrameworkExecutorID;
  foreachpair(const FrameworkExecutorID& frameworkAndExec,
              RunningTaskInfo& task, tasks) {
    FakeTask *fakeTask = task.fakeTask;
    if (fakeTask) {
      Resources requested = fakeTask->getUsage(oldTime, newTime);
      Resources usage = minResources(requested,
          task.assignedResources.expectedResources);
      TaskState state = fakeTask->takeUsage(oldTime, newTime, usage);
      if (state != TASK_RUNNING) {
        StatusUpdate update;
        update.mutable_framework_id()->MergeFrom(frameworkAndExec.first);
        update.mutable_status()->mutable_task_id()->MergeFrom(task.taskId);
        update.mutable_status()->set_state(state);
        update.set_timestamp(process::Clock::now());
        update.set_uuid(task.taskId.value());
        process::dispatch(slave, &Slave::statusUpdate, update);
        unregisterTask(frameworkAndExec.first, frameworkAndExec.second,
                       task.taskId);
      }
    } else {
      LOG(INFO) << "Executor with no task";
    }
  }

  lastTime = newTime.value;
}


}  // namespace fake
}  // namespace internal
}  // namespace mesos
