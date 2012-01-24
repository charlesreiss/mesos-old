#include "fake/fake_isolation_module.hpp"


namespace mesos {
namespace internal {
namespace fake {

void FakeIsolationModule::launchExecutor(
    const FrameworkID& frameworkId, const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo, const std::string& directory,
    const ResourceHints& resources) {
}

void FakeIsolationModule::killExecutor(
    const FrameworkID& frameworkId, const ExecutorID& executorId) {
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
