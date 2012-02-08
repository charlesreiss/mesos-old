#include "fake/scenario.hpp"

#include "fake/fake_isolation_module.hpp"
#include "master/simple_allocator.hpp"

namespace mesos {
namespace internal {
namespace fake {

using process::PID;

void Scenario::registerOptions(Configurator* configurator)
{
  FakeIsolationModule::registerOptions(configurator);
}

Scenario::Scenario(const Configuration& conf_)
    : conf(conf_)
{
}

void Scenario::spawnMaster()
{
  spawnMaster(new mesos::internal::master::SimpleAllocator);
}

void Scenario::spawnMaster(mesos::internal::master::Allocator* allocator)
{
  CHECK(process::Clock::paused());
  master = new Master(allocator);
  masterPid = process::spawn(master);
  masterMasterDetector.reset(new BasicMasterDetector(masterPid));
}

void Scenario::spawnSlave(const Resources& resources)
{
  CHECK(masterPid);
  FakeIsolationModule* module = new FakeIsolationModule(tracker);
  Slave* slave = new Slave("", resources, conf, true, module);
  slaves.push_back(slave);
  slavePids.push_back(process::spawn(slave));
  slaveMasterDetectors.push_back(
      new BasicMasterDetector(masterPid, slavePids.back()));
  isolationModules.push_back(module);
}

FakeScheduler* Scenario::spawnScheduler(
    const std::string& name, const std::map<TaskID, FakeTask*>& tasks)
{
  CHECK(schedulers.find(name) == schedulers.end());
  FakeScheduler* scheduler = new FakeScheduler(&tracker);
  scheduler->setTasks(tasks);
  ExecutorInfo info;
  info.mutable_executor_id()->set_value("SHOULD-NOT-BE-RUN");
  info.set_uri("does-not-exist");
  MesosSchedulerDriver* driver = new MesosSchedulerDriver(
      scheduler,
      name,
      info,
      "mesos://" + std::string(masterPid));
  driver->start();
  schedulers[name] = scheduler;
  schedulerDrivers[name] = driver;

  foreachvalue (FakeTask* task, tasks) {
    allTasks.push_back(task);
  }
  return scheduler;
}

void Scenario::finishSetup()
{
  // Everything must be registered with the Master.
  // Make sure any timer expiration actually happens.
  process::Clock::advance(0.0);
  process::Clock::settle();
  CHECK_EQ(master->getActiveFrameworks().size(), schedulers.size());
  CHECK_EQ(master->getActiveSlaves().size(), slaves.size());
}

void Scenario::runFor(double seconds)
{
  CHECK(process::Clock::paused());
  const double kInterval = 1./2.;
  while (seconds > 0.0) {
    process::Clock::advance(std::min(kInterval, seconds));
    process::Clock::settle();
    seconds -= kInterval;
  }
}

void Scenario::stop()
{
  // terminate everything that's running asynchronously.
  foreachvalue (MesosSchedulerDriver* driver, schedulerDrivers) {
    driver->stop();
    driver->join();
  }
  foreach (PID<Slave> slavePid, slavePids) {
    process::terminate(slavePid);
    process::wait(slavePid);
  }
  process::terminate(masterPid);
  process::wait(masterPid);

  // now delete and clear everything we allocated or took ownership of
  if (master) {
    delete master;
    master = 0;
  }
  foreach (Slave* slave, slaves) {
    delete slave;
  }
  slaves.clear();
  foreach (FakeIsolationModule* module, isolationModules) {
    delete module;
  }
  isolationModules.clear();
  foreachvalue (FakeScheduler* scheduler, schedulers) {
    delete scheduler;
  }
  schedulers.clear();
  masterMasterDetector.reset(0);
  foreach (BasicMasterDetector* detector, slaveMasterDetectors) {
    delete detector;
  }
  slaveMasterDetectors.clear();
  foreachvalue (MesosSchedulerDriver* driver, schedulerDrivers) {
    delete driver;
  }
  schedulerDrivers.clear();
  foreachvalue (FakeScheduler* scheduler, schedulers) {
    delete scheduler;
  }
  schedulers.clear();
  foreach (FakeTask* task, allTasks) {
    delete task;
  }
  allTasks.clear();
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
