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

#ifndef __TESTS_FAKE_UTIL_HPP__
#define __TESTS_FAKE_UTIL_HPP__

#include <gmock/gmock.h>
#include "fake/fake_task.hpp"
#include "tests/utils.hpp"
#include "common/seconds.hpp"
#include "common/resources.hpp"

using namespace mesos;
using namespace mesos::internal;

#define DEFAULT_FRAMEWORK_ID \
  ({ \
    mesos::FrameworkID id; \
    id.set_value("default-framework"); \
    id; \
  })

#define DEFAULT_FRAMEWORK_INFO \
  ({ \
    mesos::FrameworkInfo info; \
    info.set_user("ignored-username"); \
    info.set_name("ignored-name"); \
    info.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO); \
    info; \
  })

struct MockFakeTask : mesos::internal::fake::FakeTask {
  MOCK_CONST_METHOD2(getUsage, Resources(seconds, seconds));
  MOCK_METHOD3(takeUsage, TaskState(seconds, seconds, const Resources&));
  MOCK_CONST_METHOD0(getResourceRequest, ResourceHints());

  void printToStream(std::ostream& out) const {
    out << "MockFakeTask";
  }
};

#endif
