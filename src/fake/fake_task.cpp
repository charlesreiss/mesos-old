#include "fake/fake_task.hpp"

namespace mesos {
namespace internal {
namespace fake {

hashmap<std::pair<FrameworkID, TaskID>, FakeTask*> fakeTasks;

}  // namespace fake
}  // namespace internal
}  // namespace mesos
