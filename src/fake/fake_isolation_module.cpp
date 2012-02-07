#include <vector>

#include <boost/bind.hpp>
#include <boost/tuple/tuple.hpp>

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
using std::vector;

void FakeExecutor::launchTask(ExecutorDriver* driver,
                              const TaskDescription& task)
{
  LOG(INFO) << "Asked to launch task " << task.DebugString();
  module->registerTask(frameworkId, executorId, task.task_id());

  TaskStatus status;
  status.mutable_task_id()->MergeFrom(task.task_id());
  status.set_state(TASK_RUNNING);
  driver->sendStatusUpdate(status);
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

void FakeIsolationModule::registerOptions(Configurator* configurator)
{
  configurator->addOption<double>("fake_interval",
      "tick interval for fake isolation module", 1.0);
}

void FakeIsolationModule::initialize(const Configuration& conf, bool local,
    const process::PID<Slave>& slave_)
{
  LOG(INFO) << "initialize; this = " << (void*)this;
  slave = slave_;
  interval = conf.get<double>("fake_interval", 1.0);
  lastTime = process::Clock::now();
  ticker.reset(new FakeIsolationModuleTicker(this, interval));
  process::spawn(ticker.get());
  CHECK(local);
}

void FakeIsolationModule::launchExecutor(
    const FrameworkID& frameworkId, const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo, const std::string& directory,
    const ResourceHints& resources) {
  LOG(INFO) << "launchExecutor; this = " << (void*)this;
  FakeExecutor* executor = new FakeExecutor(this);
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
    const TaskID& taskId)
{
  FakeTask* task = fakeTasks.getTaskFor(frameworkId, executorId, taskId);
  CHECK_EQ(0, pthread_mutex_lock(&tasksLock));
  LOG(INFO) << "FakeIsolationModule::registerTask(" << frameworkId << ","
            << executorId << ", ...); this = " << (void*) this;
  RunningTaskInfo* taskInfo = &tasks[make_pair(frameworkId, executorId)];
  CHECK(!taskInfo->fakeTask);
  taskInfo->taskId.MergeFrom(taskId);
  taskInfo->fakeTask = task;
  CHECK_GT(tasks.size(), 0);
  LOG(INFO) << "Now got " << tasks.size();
  CHECK_EQ(0, pthread_mutex_unlock(&tasksLock));
}

void FakeIsolationModule::unregisterTask(
    const FrameworkID& frameworkId, const ExecutorID& executorId,
    const TaskID& taskId)
{
  CHECK_EQ(0, pthread_mutex_lock(&tasksLock));
  LOG(INFO) << "FakeIsolationModule::unregisterTask(" << frameworkId << ","
            << executorId << ", ...)";
  RunningTaskInfo* taskInfo = &tasks[make_pair(frameworkId, executorId)];
  CHECK_EQ(taskInfo->taskId, taskId);
  CHECK_NOTNULL(taskInfo->fakeTask);
  taskInfo->fakeTask = 0;
  CHECK_EQ(0, pthread_mutex_unlock(&tasksLock));
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

void FakeIsolationModuleTicker::tick() {
  if (module->tick()) {
    VLOG(2) << "scheduling new tick";
    timer = process::delay(interval, self(), &FakeIsolationModuleTicker::tick);
  }
}

bool FakeIsolationModule::tick() {
  VLOG(2) << "FakeIsolationModule::tick; this = " << (void*)this;
  CHECK_EQ(0, pthread_mutex_lock(&tasksLock));
  VLOG(2) << "tasks.size = " << tasks.size();

  seconds oldTime(lastTime);
  seconds newTime(process::Clock::now());

  // Version 0: Ignore min_* requests. For each task, assume that the task
  // gets min(its usage, its allocation).

  typedef std::pair<FrameworkID, ExecutorID> FrameworkExecutorID;
  typedef boost::tuple<FrameworkID, ExecutorID, TaskID> TaskTuple;
  vector<TaskTuple> toUnregister;
  foreachpair(const FrameworkExecutorID& frameworkAndExec,
              RunningTaskInfo& task, tasks) {
    FakeTask *fakeTask = task.fakeTask;
    if (fakeTask) {
      VLOG(2) << "Checking on task " << frameworkAndExec.first << " "
                << frameworkAndExec.second;
      Resources requested = fakeTask->getUsage(oldTime, newTime);
      Resources usage = minResources(requested,
          task.assignedResources.expectedResources);
      TaskState state = fakeTask->takeUsage(oldTime, newTime, usage);
      if (state != TASK_RUNNING) {
        MesosExecutorDriver* driver = drivers[frameworkAndExec].first;
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.taskId);
        status.set_state(state);
        driver->sendStatusUpdate(status);
        toUnregister.push_back(boost::make_tuple(
              frameworkAndExec.first, frameworkAndExec.second, task.taskId));
      }
    } else {
      VLOG(1) << "Executor with no task " << frameworkAndExec.first << " "
                << frameworkAndExec.second;
    }
  }
  CHECK_EQ(0, pthread_mutex_unlock(&tasksLock));

  foreach (const TaskTuple& tuple, toUnregister) {
    unregisterTask(tuple.get<0>(), tuple.get<1>(), tuple.get<2>());
  }

  lastTime = newTime.value;
  return !shuttingDown;
}

FakeIsolationModule::~FakeIsolationModule()
{
  LOG(INFO) << "~FakeIsolationModule";
  CHECK_EQ(0, pthread_mutex_lock(&tasksLock));
  shuttingDown = true;
  CHECK_EQ(0, pthread_mutex_unlock(&tasksLock));
  LOG(INFO) << "teriminate";
  process::terminate(ticker.get());
  LOG(INFO) << "cancel";
  process::timers::cancel(ticker->timer);
  LOG(INFO) << "wait";
  process::wait(ticker.get());
  LOG(INFO) << "shut down ticker";
  pthread_mutex_destroy(&tasksLock);
  foreachvalue(const DriverMap::mapped_type& pair, drivers) {
    MesosExecutorDriver* driver = pair.first;
    FakeExecutor* executor = pair.second;
    driver->stop();
    delete driver;
    delete executor;
  }
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
