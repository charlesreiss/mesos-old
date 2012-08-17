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

#ifndef __FAKE_TASK_SIMPLE_HPP__
#define __FAKE_TASK_SIMPLE_HPP__

#include "fake/fake_task.hpp"

namespace mesos {
namespace internal {
namespace fake {

class ConstantTask : public FakeTask {
public:
  ConstantTask(const Resources& usage_, const ResourceHints& request_);
  Resources getUsage(seconds from, seconds to) const;
  UsageInfo takeUsage(seconds from, seconds to, const Resources& resources);
  ResourceHints getResourceRequest() const;
  void printToStream(std::ostream& out) const;

private:
  Resources usage;
  Resources usageNoCpu;
  ResourceHints request;
};

class BatchTask : public FakeTask {
public:
  BatchTask(const Resources& constUsage_, const ResourceHints& request_,
            double cpuUnits_, double maxCpuRate_,
            double hiddenPerCpu_ = 0.0);
  Resources getUsage(seconds from, seconds to) const;
  UsageInfo takeUsage(seconds from, seconds to, const Resources& resources);
  ResourceHints getResourceRequest() const;
  void printToStream(std::ostream& out) const;

private:
  Resources constUsage;
  ResourceHints request;
  double initialCpuUnits, cpuUnits, maxCpuRate, hiddenPerCpu;
};

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
