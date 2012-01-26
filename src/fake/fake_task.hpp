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

inline seconds operator-(const seconds& s1, const seconds& s2) {
  return seconds(s1.value - s2.value);
}

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

struct ContinuousTask : FakeTask {
  ContinuousTask(const Resources& usage_, const ResourceHints& request_)
    : usage(usage_), request(request_), score(0.0) {}

  Resources getUsage(seconds from, seconds to) const {
    return usage;
  }
  TaskState takeUsage(seconds from, seconds to, Resources resources) {
    if (usage <= resources) {
      score += (to - from).value;
    }
    return TASK_RUNNING;
  }

  ResourceHints getResourceRequest() const {
    return request;
  }

  void PrintToStream(std::ostream& out) const {
    out << "ContinuousTask";
  }
private:
  Resources usage;
  ResourceHints request;
  double score;
};

struct LimitedTask : FakeTask {
  LimitedTask(const Resources& constUsage_, const ResourceHints& request_,
      double cpuUnits_)
    : constUsage(constUsage_), request(request_), cpuUnits(cpuUnits_) {}

  bool done() const {
    return cpuUnits <  0.0;
  }

  Resources getUsage(seconds from, seconds to) const {
    Resources result(constUsage);
    Resource cpu;
    cpu.set_name("cpu");
    cpu.set_type(Value::SCALAR);
    cpu.mutable_scalar()->set_value(std::max(0.0, cpuUnits / (to - from).value));
    result += cpu;
    return result;
  }

  TaskState takeUsage(seconds from, seconds to, Resources resources) {
    cpuUnits -= resources.get("cpu", Value::Scalar()).value();
    return done() ? TASK_FINISHED : TASK_RUNNING;
  }

  ResourceHints getResourceRequest() const {
    return request;
  }

  void PrintToStream(std::ostream& out) const {
    out << "LimitedTask";
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
