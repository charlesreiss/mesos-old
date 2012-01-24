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

#include <gmock/gmock.h>

#include "boost/scoped_ptr.hpp"

#include "tests/utils.hpp"

#include "fake/fake_isolation_module.hpp"
#include "fake/fake_task.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;
using namespace mesos::internal::test;

struct MockFakeTask : FakeTask {
  MOCK_CONST_METHOD2(getUsage, Resources(seconds, seconds));
  MOCK_METHOD3(takeUsage, TaskState(seconds, seconds, Resources));
  MOCK_CONST_METHOD0(done, bool(void));
};

#define DEFAULT_FRAMEWORK_ID \
  ({ \
    FrameworkID id; \
    id.set_value("default-framework"); \
    id; \
  })

#define DEFAULT_FRAMEWORK_INFO \
  ({ \
    FrameworkInfo info; \
    info.set_user("ignored-username"); \
    info.set_name("ignored-name"); \
    info.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO); \
    info; \
  })

class FakeIsolationModuleTest : public ::testing::Test {
public:
  void startSlave() {
    runningExecs = 0;
    process::Clock::pause();
    process::filter(&mockFilter);
    using testing::_;
    EXPECT_MSG(mockFilter, _, _, _).WillRepeatedly(testing::Return(false));
    mockMaster.reset(new FakeProtobufProcess);
    mockMaster->setFilter(&mockFilter);
    mockMasterPid = process::spawn(mockMaster.get());
    module.reset(new FakeIsolationModule);
    slave.reset(new Slave(Resources::parse("cpu:4.0;mem:4096"), true,
                          module.get()));
    slavePid = process::spawn(slave.get());

    trigger askedToRegister;
    mockMaster->expectAndWait<RegisterSlaveMessage>(slavePid, &askedToRegister);
    process::dispatch(slavePid, &Slave::newMasterDetected, mockMasterPid);
    WAIT_UNTIL(askedToRegister);

    SlaveID dummySlaveId;
    dummySlaveId.set_value("default");
    process::dispatch(slavePid, &Slave::registered, dummySlaveId);
  }

  void startExecutorFor(std::string id) {
    ExecutorInfo info;
    info.mutable_executor_id()->set_value(id);
    trigger gotRegister;
    EXPECT_MSG(mockFilter, "RegisterExecutorMessage", testing::_, slavePid).
      WillOnce(testing::DoAll(Trigger(&gotRegister),
                              testing::Return(false)));
    process::dispatch(module.get(),
        &IsolationModule::launchExecutor,
        DEFAULT_FRAMEWORK_ID,
        DEFAULT_FRAMEWORK_INFO,
        info, "must-be-ignored",
        ResourceHints());
    WAIT_UNTIL(gotRegister);
    ++runningExecs;
  }

  void startTask(std::string id, MockFakeTask* task);
  void endTask(std::string id, MockFakeTask* task);
  void killExecutorFor(std::string id) {
    ExecutorID executorId;
    executorId.set_value(id);
    trigger gotExit;
    mockMaster->expectAndWait<ExitedExecutorMessage>(slavePid, &gotExit);
    process::dispatch(module.get(),
        &IsolationModule::killExecutor,
        DEFAULT_FRAMEWORK_ID,
        executorId);
    WAIT_UNTIL(gotExit);
    --runningExecs;
  }

  void queryUsage() {
  }

  void stopSlave() {
    process::terminate(slavePid);
    process::terminate(mockMasterPid);
    process::wait(slavePid);
    process::wait(mockMasterPid);
    process::filter(0);
    process::Clock::resume();
    EXPECT_EQ(runningExecs, 0);
  }

protected:
  hashmap<std::string, UsageMessage> lastUsage;
  int runningExecs;

  boost::scoped_ptr<Slave> slave;
  process::PID<Slave> slavePid;
  boost::scoped_ptr<FakeIsolationModule> module;
  process::UPID mockMasterPid;
  boost::scoped_ptr<FakeProtobufProcess> mockMaster;
  MockFilter mockFilter;
};

TEST_F(FakeIsolationModuleTest, InitStop) {
  startSlave();
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, StartKillExecutor) {
  startSlave();
  startExecutorFor("task0");
  killExecutorFor("task0");
  stopSlave();
}
