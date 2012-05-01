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

#include <glog/logging.h>

#include "fake/fake_task_simple.hpp"

#include <algorithm>

namespace mesos {
namespace internal {
namespace fake {

ConstantTask::ConstantTask(const Resources& usage_,
                           const ResourceHints& request_)
    : usage(usage_), request(request_) {}

Resources
ConstantTask::getUsage(seconds from, seconds to) const
{
  return usage;
}

TaskState
ConstantTask::takeUsage(seconds from, seconds to, const Resources& given)
{
  bool less = true;
  foreach (const Resource& resource, usage) {
    if (resource.name() == "cpus") continue;
    Option<Resource> givenResource = given.get(resource);
    if (givenResource.isNone() || resource <= givenResource.get()) {
      less = false;
    }
  }
  if (less) {
    return TASK_LOST;
  } else {
    return TASK_RUNNING;
  }
}

ResourceHints
ConstantTask::getResourceRequest() const
{
  return request;
}

void
ConstantTask::printToStream(std::ostream& out) const
{
  out << "ConstantTask[uses " << usage << "; requests " << request << "]";
}

BatchTask::BatchTask(const Resources& constUsage_,
                     const ResourceHints& request_,
                     double cpuUnits_, double maxCpuRate_)
    : constUsage(constUsage_), request(request_), initialCpuUnits(cpuUnits_),
      cpuUnits(cpuUnits_), maxCpuRate(maxCpuRate_)
{
  CHECK(constUsage <= request.expectedResources);
  CHECK_GT(cpuUnits, 1e-8);
  CHECK_GT(maxCpuRate, 1e-8);
}

Resources
BatchTask::getUsage(seconds from, seconds to) const
{
  double delta = to.value - from.value;
  CHECK_GT(delta, 0);
  double cpu = std::min(maxCpuRate, cpuUnits / delta);
  Resources result = constUsage;
  Resource cpuResource;
  cpuResource.set_name("cpus");
  cpuResource.set_type(Value::SCALAR);
  cpuResource.mutable_scalar()->set_value(cpu);
  result += cpuResource;
  return result;
}

TaskState
BatchTask::takeUsage(seconds from, seconds to, const Resources& resources)
{
  // Make sure rounding errors between getUsage() --> takeUsage() don't cause
  // us to never complete.
  const double kSmall = 1e-10;
  double delta = to.value - from.value;
  CHECK_GT(delta, 0);
  if (resources < constUsage) {
    cpuUnits = initialCpuUnits;
    LOG(WARNING) << "TASK_LOST: needed " << constUsage << "; got "
                 << resources << "; requested " << request;
    return TASK_LOST;
  } else {
    double cpuTaken = resources.get("cpus", Value::Scalar()).value();
    CHECK_GT(cpuTaken, 1e-10) << *this << " got " << resources;
    cpuUnits -= resources.get("cpus", Value::Scalar()).value() * delta;
    if (cpuUnits <= kSmall) {
      return TASK_FINISHED;
    } else {
      return TASK_RUNNING;
    }
  }
}

ResourceHints
BatchTask::getResourceRequest() const
{
  return request;
}

void
BatchTask::printToStream(std::ostream& out) const
{
  out << "BatchTask[uses at least " << constUsage
      << "; requests " << request
      << "; needs " << cpuUnits << ", max rate " << maxCpuRate
      << "]";
}

}  // namespace fake
}  // namespace internal
}  // namespace mesos
