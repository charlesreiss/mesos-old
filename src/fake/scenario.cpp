#include "fake/scenario.hpp"

#include <boost/shared_ptr.hpp>

#include "fake/fake_isolation_module.hpp"
#include "master/allocator_factory.hpp"
#include "fake/fake_task_simple.hpp"
#include "fake/fake_task_pattern.hpp"
#include "boost/property_tree/json_parser.hpp"

namespace mesos {
namespace internal {
namespace fake {

using process::PID;

void Scenario::registerOptions(Configurator* configurator)
{
  FakeIsolationModule::registerOptions(configurator);
}

Scenario::Scenario() : master(0), conf(), interval(1./8.)
{
}

Scenario::Scenario(const Configuration& conf_)
    : master(0), conf(conf_), interval(conf_.get<double>("fake_interval", 1./8.))
{
}

void Scenario::spawnMaster()
{
  spawnMaster(mesos::internal::master::AllocatorFactory::instantiate(
        conf.get<std::string>("allocator", "simple"), 0));
}

void Scenario::spawnMaster(mesos::internal::master::Allocator* allocator_)
{
  CHECK(process::Clock::paused());
  allocator = allocator_;
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

void Scenario::spawnScheduler(
    const std::string& name, const Attributes& attributes,
    const std::map<TaskID, FakeTask*>& tasks, double startTime)
{
  CHECK(schedulers.find(name) == schedulers.end());
  FakeScheduler* scheduler = new FakeScheduler(attributes, &tracker);
  scheduler->setTasks(tasks);
  ExecutorInfo info;
  info.mutable_executor_id()->set_value("SHOULD-NOT-BE-RUN");
  info.set_uri("does-not-exist");
  FrameworkID id;
  id.set_value(name);
  MesosSchedulerDriver* driver = new MesosSchedulerDriver(
      scheduler,
      name,
      info,
      "mesos://" + std::string(masterPid),
      id);
  CHECK_EQ(OK, driver->start());
  schedulers[name] = scheduler;
  schedulerDrivers[name] = driver;

  foreachvalue (FakeTask* task, tasks) {
    allTasks.push_back(task);
  }
  if (startTime > 0.0) {
    scheduler->setStartTime(startTime);
  }
}

void Scenario::stopScheduler(const std::string& name)
{
  schedulerDrivers[name]->stop();
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
  while (seconds > 0.0) {
    process::Clock::advance(std::min(interval, seconds));
    process::Clock::settle();
    sanityCheck();
    seconds -= interval;
  }
}

void Scenario::sanityCheck()
{
  allocator->sanityCheck();
  foreach (master::Framework* framework, master->getActiveFrameworks())
  {
    CHECK_EQ(0, framework->offers.size());
  }

  if (conf.get<std::string>("allocator", "simple") != "simple") {
    const Resources smallResources(Resources::parse("cpus:0.0001;mem:0.0001"));
    foreach (master::Slave* slave, master->getActiveSlaves())
    {
      Resources conservativeFreeMin = slave->info.resources() -
        slave->resourcesOffered.minResources - slave->resourcesGaurenteed;
      Resources conservativeFreeExpect = slave->info.resources() -
        slave->resourcesOffered.expectedResources - slave->resourcesInUse -
        slave->resourcesObservedUsed;
      conservativeFreeMin -= smallResources;
      conservativeFreeExpect -= smallResources;
      ResourceHints conservativeFree(conservativeFreeExpect, conservativeFreeMin);
      LOG(INFO) << "We think that " << conservativeFree << " is free on slave "
                << slave->id;
      foreachpair (const std::string& name, FakeScheduler* scheduler, schedulers) {
        CHECK(!scheduler->mightAccept(conservativeFree)) << name;
      }
    }
  }
}

void Scenario::stop()
{
  // terminate everything that's running asynchronously.
  foreachvalue (MesosSchedulerDriver* driver, schedulerDrivers) {
    driver->stop();
    driver->join();
  }
  LOG(ERROR) << "stopped/joined schedulers";
  foreach (PID<Slave> slavePid, slavePids) {
    process::terminate(slavePid);
    process::wait(slavePid);
  }
  process::terminate(masterPid);
  process::wait(masterPid);

  LOG(ERROR) << "terminated master, slaves";

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

namespace {

void setupScheduler(Scenario* scenario,
                    const std::string& schedName, const ptree& spec,
                    const std::map<TaskID, FakeTask*>& tasks,
                    const Attributes& extraAttributes,
                    const std::string& type)
{
  const double now(process::Clock::now());
  Attributes schedAttributes(
      Attributes::parse(spec.get<std::string>("attributes", "")));
  foreach (const Attribute& attr, extraAttributes) {
    schedAttributes.add(attr);
  }
  schedAttributes.add(Attributes::parse("type", type));
  schedAttributes.add(Attributes::parse("name", schedName));
  const double startTime(spec.get<double>("start_time", 0.0) + now);
  scenario->spawnScheduler(schedName, schedAttributes, tasks, startTime);
}

}  // namespace

void populateScenarioFrom(const ptree& spec, Scenario* scenario)
{
  CHECK(process::Clock::paused());
  const double now(process::Clock::now());
  scenario->spawnMaster();
  scenario->setLabelColumns(spec.get<std::string>("label_cols"));
  scenario->setLabel(spec.get<std::string>("label", ""));
  boost::optional<const ptree&> batchJobs = spec.get_child_optional("batch");
  if (batchJobs) {
    foreachpair (const std::string& schedName, const ptree& batch,
                 batchJobs.get()) {
      const ResourceHints batchRequest(
          ResourceHints::parse(
            batch.get<std::string>("request", ""),
            batch.get<std::string>("request_min", "")));
      const Resources constResources(
          Resources::parse(batch.get<std::string>("const_resources", "")));
      const double baseMaxCpus(batch.get<double>("max_cpus", -1.0));
      std::map<TaskID, FakeTask*> tasks;
      double totalTime = 0.0;
      foreachpair (const std::string& key,
                   const ptree& task, batch.get_child("tasks")) {
        TaskID taskId;
        taskId.set_value(key);
        const double taskTime = task.get<double>("cpu_time", -1.0);
        const Resources extraConstResources(
            Resources::parse(task.get<std::string>("const_resources", "")));
        const double maxCpus(task.get<double>("max_cpus", baseMaxCpus));
        CHECK_GT(maxCpus, 0.0);
        CHECK_GE(taskTime, 0.0);
        totalTime += taskTime;
        tasks[taskId] = new BatchTask(constResources + extraConstResources,
                                      batchRequest, taskTime, maxCpus);
        VLOG(2) << "parsed " << *tasks[taskId];
      }
      Attributes attrs;
      attrs.add(Attributes::parse("total_time",
          boost::lexical_cast<std::string>(totalTime)));
      setupScheduler(scenario, schedName, batch, tasks, attrs, "batch");
    }
  }

  boost::optional<const ptree&> serveJobs = spec.get_child_optional("serve");
  if (serveJobs) {
    foreachpair (const std::string& schedName, const ptree& serve,
                 serveJobs.get()) {
      const ResourceHints request(ResourceHints::parse(
            serve.get<std::string>("request", ""),
            serve.get<std::string>("request_min", "")));
      const Resources constResources(
          Resources::parse(serve.get<std::string>("const_resources", "")));
      const double cpuPerUnit(serve.get<double>("cpu_per_unit", 1.0));
      std::vector<double> counts;
      foreachvalue (const ptree& value, serve.get_child("counts")) {
        counts.push_back(value.get_value<double>());
      }
      const double duration(serve.get<double>("time_per_count"));
      CHECK_GT(duration, 0.0);
      boost::shared_ptr<Pattern> pattern(new Pattern(counts, seconds(duration)));
      std::map<TaskID, FakeTask*> tasks;
      foreachpair (const std::string& key,
                   const ptree& task, serve.get_child("tasks")) {
        TaskID taskId;
        taskId.set_value(key);
        tasks[taskId] = new PatternTask(constResources, request,
                                        pattern, cpuPerUnit, seconds(duration));
      }
      setupScheduler(scenario, schedName, serve, tasks, Attributes(), "serve");
    }
  }

  // This is done _after_ so startTime stuff can take effect.
  foreachpair (const std::string& unusedSlaveName,
               const ptree& slaveSpec, spec.get_child("slaves")) {
    CHECK_EQ(unusedSlaveName, "");
    const Resources slaveResources(
        Resources::parse(slaveSpec.get<std::string>("resources", "")));
    scenario->spawnSlave(slaveResources);
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
