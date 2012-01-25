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

using std::make_pair;

using testing::AtLeast;
using testing::Return;

struct MockFakeTask : FakeTask {
  MOCK_CONST_METHOD2(getUsage, Resources(seconds, seconds));
  MOCK_METHOD3(takeUsage, TaskState(seconds, seconds, Resources));
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

static const double kTick = 1.0;

class FakeIsolationModuleTest : public ::testing::Test {
public:
  SlaveID getSlaveId() {
    SlaveID dummySlaveId;
    dummySlaveId.set_value("default");
    return dummySlaveId;
  }

  void startSlave() {
    process::Clock::pause();
    process::filter(&mockFilter);
    using testing::_;
    EXPECT_MSG(mockFilter, _, _, _).WillRepeatedly(testing::Return(false));
    mockMaster.reset(new FakeProtobufProcess);
    mockMaster->setFilter(&mockFilter);
    mockMasterPid = process::spawn(mockMaster.get());
    module.reset(new FakeIsolationModule(fakeTasks));
    slave.reset(new Slave(Resources::parse("cpu:4.0;mem:4096"), true,
                          module.get()));
    slavePid = process::spawn(slave.get());

    trigger askedToRegister;
    mockMaster->expectAndWait<RegisterSlaveMessage>(slavePid, &askedToRegister);
    process::dispatch(slave.get(), &Slave::newMasterDetected, mockMasterPid);
    WAIT_UNTIL(askedToRegister);

    process::dispatch(slave.get(), &Slave::registered, getSlaveId());
  }

  TaskDescription makeTaskDescription(const std::string& id,
                                      const ResourceHints& resources) {
    TaskDescription task;
    task.set_name("task-" + id);
    task.mutable_task_id()->set_value(id);
    task.mutable_slave_id()->MergeFrom(getSlaveId());
    task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);
    task.mutable_executor()->mutable_executor_id()->set_value(id);
    return task;
  }

  template <class T> static std::string name() {
    T m;
    return m.GetTypeName();
  }

  void startTask(std::string id, MockFakeTask* task,
                 const ResourceHints& resources) {
    TaskID taskId;
    taskId.set_value(id);
    fakeTasks[make_pair(DEFAULT_FRAMEWORK_ID, taskId)] = task;
    trigger gotRegister;
    EXPECT_MSG(mockFilter, name<RegisterExecutorMessage>(),
                           testing::_, slavePid).
      WillOnce(testing::DoAll(Trigger(&gotRegister),
                              testing::Return(false)));
    process::dispatch(slave.get(), &Slave::runTask,
        DEFAULT_FRAMEWORK_INFO,
        DEFAULT_FRAMEWORK_ID,
        mockMasterPid, // mock master acting as scheduler
        makeTaskDescription(id, resources));
    WAIT_UNTIL(gotRegister);
  }

  void killTask(std::string id) {
    trigger gotStatusUpdate;
    mockMaster->expectAndWait<StatusUpdateMessage>(slavePid, &gotStatusUpdate);
    TaskID taskId;
    taskId.set_value(id);
    process::dispatch(slave.get(), &Slave::killTask,
                      DEFAULT_FRAMEWORK_ID, taskId);
    WAIT_UNTIL(gotStatusUpdate);
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
  }

protected:
  hashmap<std::string, UsageMessage> lastUsage;

  boost::scoped_ptr<Slave> slave;
  process::PID<Slave> slavePid;
  boost::scoped_ptr<FakeIsolationModule> module;
  process::UPID mockMasterPid;
  boost::scoped_ptr<FakeProtobufProcess> mockMaster;
  MockFilter mockFilter;
  FakeTaskMap fakeTasks;
};

TEST_F(FakeIsolationModuleTest, InitStop) {
  startSlave();
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, StartKillTask) {
  startSlave();
  MockFakeTask mockTask;
  startTask("task0", &mockTask, ResourceHints());
  killTask("task0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, TaskRunOneSecond) {
  using testing::_;
  startSlave();
  MockFakeTask mockTask;
  startTask("task0", &mockTask, ResourceHints());
  EXPECT_CALL(mockTask, getUsage(_, _)).
    WillRepeatedly(Return(Resources::parse("cpu:0.0")));
  EXPECT_CALL(mockTask, takeUsage(_, _, _)).
    WillOnce(Return(TASK_FINISHED));

  trigger gotStatusUpdate;
  StatusUpdateMessage updateMessage;
  mockMaster->expectAndStore<StatusUpdateMessage>(slavePid, &updateMessage,
      &gotStatusUpdate);
  process::Clock::advance(kTick);
  WAIT_UNTIL(gotStatusUpdate);
  StatusUpdateAcknowledgementMessage ack;
  ack.mutable_slave_id()->MergeFrom(getSlaveId());
  ack.mutable_framework_id()->MergeFrom(DEFAULT_FRAMEWORK_ID);
  ack.mutable_task_id()->set_value("task0");
  ack.set_uuid(updateMessage.update().uuid());
  mockMaster->send(slavePid, ack);
  stopSlave();
}
