#include "fake/scenario.hpp"

#include "fake/fake_isolation_module.hpp"
#include "master/simple_allocator.hpp"
#include "fake/fake_task_simple.hpp"
#include "boost/property_tree/json_parser.hpp"

namespace mesos {
namespace internal {
namespace fake {

using process::PID;

void Scenario::registerOptions(Configurator* configurator)
{
  FakeIsolationModule::registerOptions(configurator);
}

Scenario::Scenario() : master(0), conf()
{
}

Scenario::Scenario(const Configuration& conf_) : master(0), conf(conf_)
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
  VLOG(1) << "start slave with resources=" << resources;
  CHECK(masterPid);
  FakeIsolationModule* module = new FakeIsolationModule(tracker);
  Configuration confForSlave = conf;
  {
    std::ostringstream ost;
    foreach (const Resource& resource, resources) {
      ost << resource.name() << ":" << resource.scalar().value() << ";";
    }
    confForSlave.set("resources", ost.str());
  }
  Slave* slave = new Slave("", resources, confForSlave, true, module);
  slaves.push_back(slave);
  slavePids.push_back(process::spawn(slave));
  slaveMasterDetectors.push_back(
      new BasicMasterDetector(masterPid, slavePids.back()));
  isolationModules.push_back(module);
}

FakeScheduler* Scenario::spawnScheduler(
    const std::string& name, const Attributes& attributes,
    const std::map<TaskID, FakeTask*>& tasks)
{
  CHECK(schedulers.find(name) == schedulers.end());
  FakeScheduler* scheduler = new FakeScheduler(attributes, &tracker);
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

using boost::property_tree::ptree;

void populateScenarioFrom(const ptree& spec,
                          Scenario* scenario)
{
  scenario->spawnMaster();
  foreachpair (const std::string& unusedSlaveName,
               const ptree& slaveSpec, spec.get_child("slaves")) {
    CHECK_EQ(unusedSlaveName, "");
    const Resources slaveResources(
        Resources::parse(slaveSpec.get<std::string>("resources", "")));
    scenario->spawnSlave(slaveResources);
  }
  const ptree& batchJobs = spec.get_child("batch");
  foreachpair (const std::string& schedName, const ptree& batch, batchJobs) {
    const ResourceHints batchRequest(
        ResourceHints::parse(
          batch.get<std::string>("request", ""),
          batch.get<std::string>("request_min", "")));
    const Resources constResources(
        Resources::parse(batch.get<std::string>("const_resources", "")));
    const double maxCpus(batch.get<double>("max_cpus", -1.0));
    CHECK_GT(maxCpus, 0.0);
    std::map<TaskID, FakeTask*> tasks;
    double totalTime = 0.0;
    foreachpair (const std::string& key,
                 const ptree& task, batch.get_child("tasks")) {
      TaskID taskId;
      taskId.set_value(key);
      const double taskTime = task.get<double>("cpu_time", -1.0);
      CHECK_GE(taskTime, 0.0);
      totalTime += taskTime;
      tasks[taskId] = new BatchTask(constResources, batchRequest,
                                    taskTime, maxCpus);
      VLOG(2) << "parsed " << *tasks[taskId];
    }
    Attributes schedAttributes(
        Attributes::parse(batch.get<std::string>("attributes", "")));
    // FIXME hack
    schedAttributes.add(
        Attributes::parse("total_time",
                          boost::lexical_cast<std::string>(totalTime)));
    schedAttributes.add(Attributes::parse("type", "batch"));
    scenario->spawnScheduler(schedName, schedAttributes, tasks);
  }
}

void populateScenarioFrom(std::istream* in,
                          Scenario* scenario)
{
  ptree spec;
  boost::property_tree::json_parser::read_json(*in, spec);
  populateScenarioFrom(spec, scenario);
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
