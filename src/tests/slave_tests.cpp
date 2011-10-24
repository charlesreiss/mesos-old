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
#include <gtest/gtest.h>

#include <mesos/executor.hpp>
#include <mesos/scheduler.hpp>

#include <boost/smart_ptr/scoped_ptr.hpp>

#include <process/pid.hpp>

#include "detector/detector.hpp"

#include "local/local.hpp"

#include "slave/slave.hpp"

#include "tests/utils.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::test;

using boost::scoped_ptr;

using process::PID;
using process::UPID;
using process::Future;
using process::Promise;

using std::map;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Eq;
using testing::Return;
using testing::SaveArgPointee;

using mesos::internal::slave::Slave;

class MockTestingIsolationModule : public TestingIsolationModule
{
public:
  MockTestingIsolationModule(const std::map<ExecutorID, Executor*>& execs)
    : TestingIsolationModule(execs) {}
  MOCK_METHOD3(resourcesChanged, void(const FrameworkID&,
        const ExecutorID&, const Resources&));
  Resources use_resources;
};

class SlaveTest : public ::testing::Test {
protected:
  SlaveTest() {
    ranExecutor = false;
    EXPECT_MSG(filter, _, _, _)
      .WillRepeatedly(Return(false));
    process::filter(&filter);
    master.setFilter(&filter);
  }

  ~SlaveTest() {
    process::filter(0);
  }

  void spawnSlave() {
    map<ExecutorID, Executor*> execs;
    execs[DEFAULT_EXECUTOR_ID] = &executor;

   isolationModule.reset(new MockTestingIsolationModule(execs));
    slave.reset(new Slave(
          Resources::parse("cpus:2;mem:1024"),
          true,
          isolationModule.get()));
    slavePid = process::spawn(slave.get());
  }

  static FrameworkInfo getDefaultFramework(const std::string& frameworkName) {
    FrameworkInfo info;
    info.set_user("testUsername");
    info.set_name(frameworkName);
    info.mutable_executor()->CopyFrom(DEFAULT_EXECUTOR_INFO);
    return info;
  }

  static SlaveID getDefaultSlaveID() {
    SlaveID id;
    id.set_value("testSlaveId");
    return id;
  }

  static TaskDescription getDefaultTask(const std::string& frameworkName) {
    TaskDescription task;
    task.set_name("testTask");
    task.mutable_task_id()->set_value("testId-" + frameworkName);
    task.mutable_slave_id()->CopyFrom(getDefaultSlaveID());
    return task;
  }

  void detectMaster() {
    master.expect<RegisterSlaveMessage>(slavePid);
    masterPid = process::spawn(&master);
    masterDetector.reset(new BasicMasterDetector(masterPid, slavePid, false));
  }

  void registerSlave() {
    SlaveRegisteredMessage registered;
    registered.mutable_slave_id()->CopyFrom(getDefaultSlaveID());
    master.send(slavePid, registered);
  }

  void runTask(const std::string& frameworkName) {
    EXPECT_CALL(executor, init(_, _)).Times(1);

    EXPECT_CALL(executor, launchTask(_, _))
      .WillOnce(SendStatusUpdate(TASK_RUNNING));

    EXPECT_CALL(*isolationModule, resourcesChanged(_, _, _)).Times(1);

    RunTaskMessage message;
    message.mutable_framework_id()->set_value(frameworkName);
    message.mutable_framework()->CopyFrom(getDefaultFramework(frameworkName));
    message.set_pid("testFrameworkPid");
    message.mutable_task()->CopyFrom(getDefaultTask(frameworkName));

    StatusUpdateMessage update;
    master.expectAndStoreStart(slavePid, &update);
    master.send(slavePid, message);

    master.expectAndStoreFinish();

    StatusUpdateAcknowledgementMessage ack;
    ack.mutable_slave_id()->CopyFrom(getDefaultSlaveID());
    ack.mutable_framework_id()->set_value("testFrameworkId");
    ack.mutable_task_id()->set_value("testId-" + frameworkName);
    ack.set_uuid(update.update().uuid());
    master.send(slavePid, ack);
  }

  void shutdown() {
    if (ranExecutor) {
      EXPECT_CALL(executor, shutdown(_));
    }
    process::post(slavePid, process::TERMINATE);
    process::wait(slavePid);
    process::post(masterPid, process::TERMINATE);
    process::wait(masterPid);
  }

  bool ranExecutor;
  scoped_ptr<BasicMasterDetector> masterDetector;
  scoped_ptr<Slave> slave;
  PID<Slave> slavePid;
  FakeProtobufProcess master;
  UPID masterPid;
  MockExecutor executor;
  MockScheduler scheduler;
  MockFilter filter;
  scoped_ptr<MockTestingIsolationModule> isolationModule;
};

TEST_F(SlaveTest, RegisterWithMaster) {
  spawnSlave();
  detectMaster();
  registerSlave();
  shutdown();
}

TEST_F(SlaveTest, StartTask) {
  spawnSlave();
  detectMaster();
  registerSlave();
  runTask("default");
  shutdown();
}
