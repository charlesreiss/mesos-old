#include <glog/logging.h>

#include "fake/fake_isolation_module.hpp"
#include "mesos/executor.hpp"

namespace mesos {
namespace internal {
namespace fake {

using std::make_pair;
void FakeIsolationModule::initialize(const Configuration& conf, bool local,
    const process::PID<Slave>& slave_) {
  slave = slave_;
  executor.reset(new FakeExecutor);
  CHECK(local);
}

void FakeIsolationModule::launchExecutor(
    const FrameworkID& frameworkId, const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo, const std::string& directory,
    const ResourceHints& resources) {
  CHECK(executor.get());
  MesosExecutorDriver* driver = new MesosExecutorDriver(executor.get());
  drivers[make_pair(frameworkId, executorInfo.executor_id())] = driver;
  LOG(INFO) << "starting driver";
  driver->start(true, std::string(slave), frameworkId.value(),
                executorInfo.executor_id().value(),
                "ignored-directory-name");
  LOG(INFO) << "started driver";
}

void FakeIsolationModule::killExecutor(
    const FrameworkID& frameworkId, const ExecutorID& executorId) {
  DriverMap::iterator it = drivers.find(make_pair(frameworkId, executorId));
  if (it != drivers.end()) {
    LOG(INFO) << "about to stop driver";
    MesosExecutorDriver* driver = it->second;
    drivers.quick_erase(it);
    driver->stop();
    delete driver;
  }
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
