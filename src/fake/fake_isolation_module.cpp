#include "fake/fake_isolation_module.hpp"

#include <iomanip>
#include <vector>

#include <boost/bind.hpp>
#include <boost/tuple/tuple.hpp>

#include <glog/logging.h>

#include <process/dispatch.hpp>
#include <process/process.hpp>
#include <process/timer.hpp>

#include "mesos/executor.hpp"
#include "slave/slave.hpp"

namespace mesos {
namespace internal {
namespace fake {

using std::make_pair;
using std::vector;

FakeExecutor::FakeExecutor(FakeIsolationModule* module_)
    : initialized(false), module(module_) {}

void FakeExecutor::init(ExecutorDriver* driver,
                        const ExecutorArgs& args)
{
  CHECK(!initialized);
  initialized = true;
  frameworkId.MergeFrom(args.framework_id());
  executorId.MergeFrom(args.executor_id());
}

void FakeExecutor::shutdown(ExecutorDriver* driver)
{
  CHECK(initialized);
  initialized = false;
}

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
      "tick interval for fake isolation module", 1./8.);
  configurator->addOption<double>("fake_usage_interval",
      "usage messsage generation tick interval for fake isolation module",
      1.0);
  configurator->addOption<bool>("fake_extra_cpu",
      "simulated isolation allows processes to use CPU beyond their "
      "allocation", false);
  configurator->addOption<bool>("fake_extra_mem",
      "simulated isolation allows processes to use memory beyond their "
      "allocation", false);
  configurator->addOption<bool>("fake_assign_min",
      "use minimum for gaurenteed allocation instead of expected", false);
  configurator->addOption<double>("fake_mem_slack",
      "slack memory", false);
}

FakeIsolationModule::FakeIsolationModule(const FakeTaskTracker& fakeTasks_)
      : fakeTasks(fakeTasks_), shuttingDown(false), extraCpu(false),
        extraMem(false), assignMin(false) {
  pthread_mutexattr_t mattr;
  pthread_mutexattr_init(&mattr);
  pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_ERRORCHECK);
  pthread_mutex_init(&tasksLock, &mattr);
}

void FakeIsolationModule::initialize(const Configuration& conf, bool local,
    const process::PID<Slave>& slave_)
{
  LOG(INFO) << "initialize; this = " << (void*)this;
  slave = slave_;
  interval = conf.get<double>("fake_interval", 1.0);
  usageInterval = conf.get<double>("fake_usage_interval", 1.0);
  extraCpu = conf.get<bool>("fake_extra_cpu", false);
  extraMem = conf.get<bool>("fake_extra_mem", false);
  assignMin = conf.get<bool>("fake_assign_min", false);
  slackMem = conf.get<double>("fake_slack_mem", 0.05);
  totalResources = Resources::parse(conf.get<std::string>("resources", ""));
  lastUsageTime = lastTime = process::Clock::now();
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
  dispatch(slave, &Slave::executorStarted, frameworkId,
           executorInfo.executor_id(), (pid_t)42);
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

  dispatch(slave, &Slave::executorExited, frameworkId, executorId, 0);
}

void FakeIsolationModule::resourcesChanged(const FrameworkID& frameworkId,
    const ExecutorID& executorId, const ResourceHints& _resources)
{
  VLOG(1) << "resourcesChanged: " << frameworkId << ", " << executorId
          << " to " << _resources;
  ResourceHints resources = _resources;
  mesos::Resource slackMemResource;
  slackMemResource.set_name("mem");
  slackMemResource.set_type(Value::SCALAR);
  slackMemResource.mutable_scalar()->set_value(slackMem);
  resources.expectedResources += slackMemResource;
  resources.minResources += slackMemResource;
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
  dispatch(self(), &IsolationModule::killExecutor, frameworkId, executorId);
}

void FakeIsolationModuleTicker::tick() {
  if (module->tick()) {
    VLOG(2) << "scheduling new tick";
    timer = process::delay(interval, self(), &FakeIsolationModuleTicker::tick);
  }
}

namespace {

struct DesiredUsage {
  typedef std::pair<FrameworkID, ExecutorID> FrameworkExecutorID;
  FrameworkExecutorID id;
  FakeIsolationModule::RunningTaskInfo* task;
  Resources desiredUsage;
  Resources assignedUsage;

  double excessCpu;
  double cpuWeight;
  double excessMem;
  double memWeight; // currently deliberately kept at 1.

  void setDesired(const Resources& desired) {
    desiredUsage = desired;
    excessCpu = desired.get("cpus", Value::Scalar()).value();
    excessMem = desired.get("mem", Value::Scalar()).value();
  }

  void assign(const Resources& usage) {
    desiredUsage -= usage;
    assignedUsage += usage;
    excessCpu -= usage.get("cpus", Value::Scalar()).value();
    excessMem -= usage.get("mem", Value::Scalar()).value();
  }

  void assign(const Resource& resource) {
    desiredUsage -= resource;
    assignedUsage += resource;
    if (resource.name() == "cpus") {
      excessCpu -= resource.scalar().value();
    } else if (resource.name() == "mem") {
      excessMem -= resource.scalar().value();
    }
  }

  DesiredUsage() : excessCpu(0.0), cpuWeight(0.0), excessMem(0.0),
                   memWeight(1.0) {}
};

inline std::ostream& operator<<(std::ostream& out, const DesiredUsage& usage)
{
  return out << usage.id.first << ", " << usage.id.second
             << ": assigned " << usage.assignedUsage
             << "; desired " << usage.desiredUsage
             << "; extra CPU = " << usage.excessCpu
             << "; CPU weight = " << usage.cpuWeight;
}

void distributeFree(const std::string& name,
                    double DesiredUsage::* weightMember,
                    double DesiredUsage::* excessMember,
                    const Resources& totalUsed,
                    const Resources& totalResources,
                    std::vector<DesiredUsage>* usages)
{
  const double kSmall = 1e-6;
  const double kLarge = 1e6;
  double used = totalUsed.get(name, Value::Scalar()).value();
  double extra = totalResources.get(name, Value::Scalar()).value() -
      used;
  while (extra >= kSmall) {
    double totalWeight = 0.0;
    double totalExcess = 0.0;
    int numExcess = 0;
    foreach (const DesiredUsage& usage, *usages) {
      totalExcess += std::max(0.0, usage.*excessMember);
      if (usage.*excessMember > 0.0) {
        totalWeight += usage.*weightMember;
        numExcess += 1;
      }
    }

    VLOG(1) << "excess (" << name << ") = " << totalExcess << "; "
            << "weight = " << totalWeight;

    if (totalExcess <= kSmall) {
      break;
    }

    double totalAssignAmount = std::min(totalExcess, extra);
    double perUnit = totalAssignAmount / totalWeight;
    VLOG(1) << "assigning excess in units of " << perUnit;
    foreach (DesiredUsage& usage, *usages) {
      double assignAmount = (perUnit <= kSmall || perUnit >= kLarge) ?
          extra / numExcess : usage.*weightMember * perUnit;
      assignAmount = std::min(usage.*excessMember, assignAmount);
      mesos::Resource usageAssign;
      usageAssign.set_name(name);
      usageAssign.set_type(Value::SCALAR);
      usageAssign.mutable_scalar()->set_value(assignAmount);
      usage.assign(usageAssign);
      extra -= assignAmount;
      used += assignAmount;
    }
  }
}

} // unnamed namespace

bool FakeIsolationModule::tick() {
  VLOG(2) << "FakeIsolationModule::tick; this = " << (void*)this;
  CHECK_EQ(0, pthread_mutex_lock(&tasksLock));
  VLOG(2) << "tasks.size = " << tasks.size();

  seconds oldTime(lastTime);
  seconds newTime(process::Clock::now());

  // Version 1: Ignore min_* requests. For each task, assume that the task
  // gets min(its usage, its allocation).
  //
  // If the extraCpu flag is set, distribute left-over CPU in proportion to the
  // size of each task's CPU assignment.

  typedef std::pair<FrameworkID, ExecutorID> FrameworkExecutorID;
  typedef boost::tuple<FrameworkID, ExecutorID, TaskID> TaskTuple;
  // 1) Gather all resource requests; assign usage within resource setting.
  std::vector<DesiredUsage> usages;
  Resources totalUsed;
  foreachpair (const FrameworkExecutorID& frameworkAndExec,
               RunningTaskInfo& task, tasks) {
    if (task.fakeTask) {
      usages.push_back(DesiredUsage());
      DesiredUsage* usage = &usages.back();
      usage->id = frameworkAndExec;
      usage->task = &task;
      usage->setDesired(task.fakeTask->getUsage(oldTime, newTime));
      Resources toAssign = minResources(usage->desiredUsage,
          assignMin ? task.assignedResources.minResources :
                      task.assignedResources.expectedResources);
      usage->assign(toAssign);
      usage->cpuWeight =
          task.assignedResources.expectedResources.get(
              "cpus", Value::Scalar()).value();
      totalUsed += toAssign;

      VLOG(1) << "Created usage " << *usage;
    }
  }

  // 2a) If using free CPU is enabled, distribute it.
  if (extraCpu) {
    distributeFree("cpus", &DesiredUsage::cpuWeight, &DesiredUsage::excessCpu,
        totalUsed, totalResources, &usages);
  }

  if (extraMem) {
    distributeFree("mem", &DesiredUsage::memWeight, &DesiredUsage::excessMem,
        totalUsed, totalResources, &usages);
  }

  // 3) Use assigned usages to actually consume simulated resources.
  vector<TaskTuple> toUnregister;
  foreach (const DesiredUsage& usage, usages) {
    VLOG(1) << "decided on usage for " << usage;
    FakeTask* fakeTask = usage.task->fakeTask;
    TaskState state =
        fakeTask->takeUsage(oldTime, newTime, usage.assignedUsage);
      recentUsage[usage.id].accumulate(seconds(interval), usage.assignedUsage,
          state != TASK_RUNNING);
    VLOG(1) << "state == "
            << TaskState_descriptor()->FindValueByNumber(state)->name();
    if (state != TASK_RUNNING) {
      MesosExecutorDriver* driver = drivers[usage.id].first;
      TaskStatus status;
      status.mutable_task_id()->MergeFrom(usage.task->taskId);
      status.set_state(state);
      driver->sendStatusUpdate(status);
      toUnregister.push_back(boost::make_tuple(
            usage.id.first, usage.id.second, usage.task->taskId));
    }
  }
  CHECK_EQ(0, pthread_mutex_unlock(&tasksLock));

  // Unregister tasks which returned a terminal status.
  foreach (const TaskTuple& tuple, toUnregister) {
    unregisterTask(tuple.get<0>(), tuple.get<1>(), tuple.get<2>());
  }

  lastTime = newTime.value;

  if (lastUsageTime + usageInterval <= lastTime) {
    LOG(INFO) << "sending usage at " << std::setprecision(10) << std::fixed
              << lastTime
              << "; last was " << lastUsageTime
              << "; interval is " << usageInterval;
    sendUsage();
  }

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

FakeIsolationModule::ResourceRecord::ResourceRecord()
    : cpuTime(0.0), memoryTime(0.0), maxMemory(0.0), dead(false)
{
}

void FakeIsolationModule::ResourceRecord::accumulate(
    seconds secs, const Resources& measurement, bool dead_)
{
  cpuTime += measurement.get("cpus", Value::Scalar()).value() * secs.value;
  memoryTime +=
      measurement.get("mem", Value::Scalar()).value() * secs.value;
  maxMemory = std::max(
      measurement.get("mem", Value::Scalar()).value(),
      maxMemory);
  dead = dead_;
}

Resources FakeIsolationModule::ResourceRecord::getResult(seconds secs) const
{
  Resources result;
  {
    mesos::Resource cpu;
    cpu.set_type(Value::SCALAR);
    cpu.set_name("cpus");
    cpu.mutable_scalar()->set_value(cpuTime / secs.value);
    result += cpu;
  }
  {
    mesos::Resource mem;
    mem.set_type(Value::SCALAR);
    mem.set_name("mem");
    mem.mutable_scalar()->set_value(maxMemory);
    result += mem;
  }
  return result;
}

void FakeIsolationModule::ResourceRecord::clear()
{
  cpuTime = memoryTime = maxMemory = 0.0;
}

void FakeIsolationModule::sendUsage()
{
  seconds interval(process::Clock::now() - lastUsageTime);
  lastUsageTime = process::Clock::now();
  foreachpair(const ResourceRecordMap::key_type& key, ResourceRecord& record,
      recentUsage) {
    UsageMessage message;
    message.mutable_framework_id()->MergeFrom(key.first);
    message.mutable_executor_id()->MergeFrom(key.second);
    message.set_timestamp(lastUsageTime);
    message.set_duration(interval.value);
    message.mutable_resources()->MergeFrom(record.getResult(interval));
    message.set_still_running(!record.dead);
    dispatch(slave, &Slave::sendUsageUpdate, message);
  }
  recentUsage.clear();
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
