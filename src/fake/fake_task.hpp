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

#include "mesos/executor.hpp"
#include "mesos/scheduler.hpp"
#include "mesos/mesos.hpp"

#include "common/resources.hpp"
#include "common/seconds.hpp"
#include "common/hashmap.hpp"

namespace mesos {
namespace internal {
namespace fake {

struct FakeTask {
  virtual Resources getUsage(seconds from, seconds to) const = 0;
  virtual mesos::TaskState takeUsage(seconds from, seconds to, Resources resources) = 0;
  virtual ResourceHints getResourceRequest() const = 0;
  virtual void PrintToStream(std::ostream&) const = 0;
};

inline std::ostream& operator<<(std::ostream& out, const FakeTask& fakeTask) {
  fakeTask.PrintToStream(out);
  return out;
}


private:
  double cpuUnits;
  Resources constUsage;
  ResourceHints request;
};

typedef hashmap<std::pair<FrameworkID, TaskID>, FakeTask*> FakeTaskMap;

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
