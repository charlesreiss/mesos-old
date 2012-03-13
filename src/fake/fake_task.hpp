/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __FAKE_TASK_HPP__
#define __FAKE_TASK_HPP__

#include <pthread.h>

#include "boost/tuple/tuple.hpp"
#include "boost/tuple/tuple_comparison.hpp"

#include "mesos/executor.hpp"
#include "mesos/scheduler.hpp"
#include "mesos/mesos.hpp"

#include "common/hashmap.hpp"
#include "common/lock.hpp"
#include "common/resources.hpp"
#include "common/seconds.hpp"
#include "common/type_utils.hpp"

namespace mesos {
namespace internal {
namespace fake {

struct FakeTask {
  virtual Resources getUsage(seconds from, seconds to) const = 0;
  virtual mesos::TaskState takeUsage(seconds from, seconds to,
                                     const Resources& resources) = 0;
  virtual ResourceHints getResourceRequest() const = 0;
  virtual void printToStream(std::ostream&) const = 0;
  virtual double getScore() const { return 0.0; }
};

inline std::ostream& operator<<(std::ostream& out, const FakeTask& fakeTask) {
  fakeTask.printToStream(out);
  return out;
}

class FakeTaskTracker {
public:
  FakeTaskTracker();

  bool haveTaskFor(const FrameworkID& frameworkId,
                   const ExecutorID& executorId,
                   const TaskID& taskId) const;

  FakeTask* getTaskFor(const FrameworkID& frameworkId,
                       const ExecutorID& executorId,
                       const TaskID& taskId) const;

  void registerTask(const FrameworkID& frameworkId,
                    const ExecutorID& executorId,
                    const TaskID& taskId,
                    FakeTask* task);
  void unregisterTask(const FrameworkID& frameworkId,
                      const ExecutorID& executorId,
                      const TaskID& taskId);

  ~FakeTaskTracker();

  typedef boost::tuple<FrameworkID, ExecutorID, TaskID> TaskTuple;

private:
  mutable pthread_rwlock_t lock;
  typedef std::map<TaskTuple, FakeTask*> TaskMap;
  TaskMap tasks;
};

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
