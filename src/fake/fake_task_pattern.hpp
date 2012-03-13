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

#include <boost/scoped_ptr.hpp>

#include "fake/fake_task.hpp"

namespace mesos {
namespace internal {
namespace fake {

// For testing.
struct GenericPattern {
  virtual double countDuring(seconds start, seconds end) const = 0;

  virtual ~GenericPattern() {}
};

struct Pattern : GenericPattern {
  std::vector<double> counts;
  // per sample duration
  seconds duration;

  double countDuring(seconds start, seconds end) const;

  Pattern(const std::vector<double>& _counts, const seconds& _duration)
      : counts(_counts), duration(_duration) {}

private:
  double at(int index) const;
};

class PatternTask : public FakeTask {
public:
  // TODO -- Figure out queuing model re: variance
  PatternTask(const Resources& _constUsage, const ResourceHints& _request,
              const GenericPattern* _pattern, double _cpuPerUnit,
              seconds _baseTime);

  Resources getUsage(seconds from, seconds to) const;
  TaskState takeUsage(seconds from, seconds to, const Resources& resources);
  ResourceHints getResourceRequest() const { return request; }
  void printToStream(std::ostream& out) const;
  double getViolations() const { return violations; }
  double getScore() const { return score; }

private:
  Resources constUsage;
  ResourceHints request;
  boost::scoped_ptr<const GenericPattern> pattern;
  double cpuPerUnit;
  double violations;
  double score;
  seconds baseTime;
};

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif // __FAKE_TASK_SIMPLE_HPP__
