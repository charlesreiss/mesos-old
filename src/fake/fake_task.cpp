#include <glog/logging.h>
#include "fake/fake_task.hpp"

#include "common/lock.hpp"

namespace mesos {
namespace internal {
namespace fake {

FakeTaskTracker::FakeTaskTracker() {
  pthread_rwlock_init(&lock, 0);
}

bool FakeTaskTracker::haveTaskFor(const FrameworkID& frameworkId,
                                  const ExecutorID& executorId,
                                  const TaskID& taskId) const
{
  ReadLock l(&lock);
  TaskMap::const_iterator it =
    tasks.find(boost::make_tuple(frameworkId, executorId, taskId));
  return it != tasks.end();
}


FakeTask* FakeTaskTracker::getTaskFor(const FrameworkID& frameworkId,
                                      const ExecutorID& executorId,
                                      const TaskID& taskId) const
{
  LOG(INFO) << "getTaskFor(" << frameworkId << ", " << executorId
            << ", " << taskId << ")";
  ReadLock l(&lock);
  TaskMap::const_iterator it =
    tasks.find(boost::make_tuple(frameworkId, executorId, taskId));
  CHECK(it != tasks.end());
  return it->second;
}

void FakeTaskTracker::registerTask(const FrameworkID& frameworkId,
                                   const ExecutorID& executorId,
                                   const TaskID& taskId,
                                   FakeTask* task)
{
  WriteLock l(&lock);
  tasks.insert(std::make_pair(
        boost::make_tuple(frameworkId, executorId, taskId), task));
}

void FakeTaskTracker::unregisterTask(const FrameworkID& frameworkId,
                                     const ExecutorID& executorId,
                                     const TaskID& taskId)
{
  WriteLock l(&lock);
  tasks.erase(boost::make_tuple(frameworkId, executorId, taskId));
}

FakeTaskTracker::~FakeTaskTracker() {
  pthread_rwlock_destroy(&lock);
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
