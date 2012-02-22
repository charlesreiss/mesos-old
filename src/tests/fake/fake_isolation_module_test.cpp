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
#include "tests/fake/util.hpp"

#include "configurator/configuration.hpp"
#include "fake/fake_isolation_module.hpp"
#include "fake/fake_task.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;
using namespace mesos::internal::test;

using std::make_pair;

using testing::AtLeast;
using testing::DoAll;
using testing::Return;
static const double kTick = 1.0;

class FakeIsolationModuleTest : public ::testing::Test {
public:
  SlaveID getSlaveId()
  {
    SlaveID dummySlaveId;
    dummySlaveId.set_value("default");
    return dummySlaveId;
  }

  void startSlave()
  {
    process::Clock::pause();
    process::filter(&mockFilter);
    using testing::_;
    EXPECT_MESSAGE(mockFilter, _, _, _).WillRepeatedly(testing::Return(false));
    mockMaster.reset(new FakeProtobufProcess);
    mockMaster->setFilter(&mockFilter);
    mockMasterPid = mockMaster->start();
    module.reset(new FakeIsolationModule(taskTracker));
    conf.set("resources", "cpus:4.0;mem:4096");
    slave.reset(new Slave(conf, true, module.get()));
    slavePid = process::spawn(slave.get());

    trigger askedToRegister;
    mockMaster->expectAndWait<RegisterSlaveMessage>(slavePid, &askedToRegister);
    process::dispatch(slave.get(), &Slave::newMasterDetected, mockMasterPid);
    WAIT_UNTIL(askedToRegister);

    process::dispatch(slave.get(), &Slave::registered, getSlaveId());
  }

  TaskDescription makeTaskDescription(const std::string& id,
                                      const ResourceHints& resources)
  {
    TaskDescription task;
    task.set_name(id);
    task.mutable_task_id()->set_value(id);
    task.mutable_slave_id()->MergeFrom(getSlaveId());
    task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);
    task.mutable_executor()->mutable_executor_id()->set_value(id);
    task.mutable_resources()->MergeFrom(resources.expectedResources);
    task.mutable_min_resources()->MergeFrom(resources.minResources);
    return task;
  }

  template <class T> static std::string name()
  {
    T m;
    return m.GetTypeName();
  }

  void startTask(std::string id, MockFakeTask* task,
                 const ResourceHints& resources)
  {
    TaskID taskId;
    taskId.set_value(id);
    ExecutorID executorId;
    executorId.set_value(id);
    taskTracker.registerTask(DEFAULT_FRAMEWORK_ID, executorId,
                             taskId, task);
    trigger gotRegister;
    EXPECT_MESSAGE(mockFilter, name<RegisterExecutorMessage>(),
                           testing::_, slavePid).
      WillOnce(testing::DoAll(Trigger(&gotRegister),
                              testing::Return(false)));
    trigger gotStatusUpdate;
    mockMaster->expectAndWait<StatusUpdateMessage>(slavePid, &gotStatusUpdate);
    process::dispatch(slave.get(), &Slave::runTask,
        DEFAULT_FRAMEWORK_INFO,
        DEFAULT_FRAMEWORK_ID,
        mockMasterPid, // mock master acting as scheduler
        makeTaskDescription(id, resources));
    WAIT_UNTIL(gotRegister);
    WAIT_UNTIL(gotStatusUpdate);
  }

  void killTask(std::string id)
  {
    trigger gotStatusUpdate;
    mockMaster->expect<ExitedExecutorMessage>();
    mockMaster->expectAndWait<StatusUpdateMessage>(slavePid, &gotStatusUpdate);
    TaskID taskId;
    taskId.set_value(id);
    process::dispatch(slave.get(), &Slave::killTask,
                      DEFAULT_FRAMEWORK_ID, taskId);
    WAIT_UNTIL(gotStatusUpdate);
  }

  void acknowledgeUpdate(const std::string& taskId,
                         const StatusUpdateMessage& updateMessage)
  {
    EXPECT_EQ(getSlaveId(), updateMessage.update().slave_id());
    FrameworkID expectId = DEFAULT_FRAMEWORK_ID;
    EXPECT_EQ(expectId, updateMessage.update().framework_id());
    EXPECT_EQ(taskId, updateMessage.update().executor_id().value());
    EXPECT_EQ(taskId, updateMessage.update().status().task_id().value());
    StatusUpdateAcknowledgementMessage ack;
    ack.mutable_slave_id()->MergeFrom(getSlaveId());
    ack.mutable_framework_id()->MergeFrom(DEFAULT_FRAMEWORK_ID);
    ack.mutable_task_id()->set_value(taskId);
    ack.set_uuid(updateMessage.update().uuid());
    mockMaster->send(slavePid, ack);
  }

  void tickAndUpdate(const std::string& taskId)
  {
    trigger gotStatusUpdate;
    StatusUpdateMessage updateMessage;
    mockMaster->expectAndStore<StatusUpdateMessage>(slavePid,
        &updateMessage, &gotStatusUpdate);
    process::Clock::advance(kTick);
    WAIT_UNTIL(gotStatusUpdate);
    acknowledgeUpdate(taskId, updateMessage);
  }

  void expectAskUsed(MockFakeTask* mockTask,
                     const std::string& getUsageResult,
                     const std::string& takeUsageExpect)
  {
    using testing::_;
    ON_CALL(*mockTask, takeUsage(_, _, _)).
      WillByDefault(Return(TASK_FAILED));
    EXPECT_CALL(*mockTask, getUsage(_, _)).
      WillRepeatedly(Return(Resources::parse(getUsageResult)));
    EXPECT_CALL(*mockTask, takeUsage(_, _, Resources::parse(takeUsageExpect))).
      WillOnce(Return(TASK_FINISHED));
    mockMaster->expect<ExitedExecutorMessage>();
    tickAndUpdate("task0");
  }

  void makeBackgroundTask(MockFakeTask* task,
                          const std::string& requestExpect,
                          const std::string& requestMin,
                          const std::string& getUsageResult,
                          const std::string& takeUsageExpect)
  {
    using testing::_;
    startTask("backgroundTask", task,
              ResourceHints::parse(requestExpect, requestMin));
    EXPECT_CALL(*task, getUsage(_, _)).
      WillRepeatedly(Return(Resources::parse(getUsageResult)));
    // Default return value so test doesn't abort the test suite.
    ON_CALL(*task, takeUsage(_, _, _)).
      WillByDefault(Return(TASK_FAILED));
    EXPECT_CALL(*task, takeUsage(_, _, Resources::parse(takeUsageExpect))).
      WillRepeatedly(Return(TASK_RUNNING));
  }

  void expectIsolationPolicy(const std::string& requestExpect,
                             const std::string& requestMin,
                             const std::string& getUsageResult,
                             const std::string& takeUsageExpect)
  {
    MockFakeTask mockTask;
    startTask("task0", &mockTask,
              ResourceHints::parse(requestExpect, requestMin));
    expectAskUsed(&mockTask, getUsageResult, takeUsageExpect);
  }

  void stopSlave() {
    if (slavePid) {
      process::terminate(slavePid);
      process::terminate(mockMasterPid);
      process::wait(slavePid);
      process::wait(mockMasterPid);
      process::wait(module.get());
    }
    process::Clock::resume();
  }

  ~FakeIsolationModuleTest() {
    stopSlave();
    module.reset(0);
    process::filter(0);
  }

protected:
  boost::scoped_ptr<Slave> slave;
  process::PID<Slave> slavePid;
  boost::scoped_ptr<FakeIsolationModule> module;
  process::UPID mockMasterPid;
  boost::scoped_ptr<FakeProtobufProcess> mockMaster;
  MockFilter mockFilter;
  FakeTaskTracker taskTracker;
  Configuration conf;
};

TEST_F(FakeIsolationModuleTest, InitStop)
{
  startSlave();
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, StartKillTask)
{
  startSlave();
  MockFakeTask mockTask;
  startTask("task0", &mockTask, ResourceHints());
  killTask("task0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, TaskRunOneSecond)
{
  using testing::_;
  startSlave();
  MockFakeTask mockTask;
  double now = process::Clock::now();
  startTask("task0", &mockTask, ResourceHints::parse("cpus:4.0", ""));
  EXPECT_CALL(mockTask, getUsage(seconds(now), seconds(now + kTick))).
    WillRepeatedly(Return(Resources::parse("cpus:0.0")));
  EXPECT_CALL(mockTask, takeUsage(_, _, _)).
    WillOnce(Return(TASK_FINISHED));

  tickAndUpdate("task0");
  killTask("task0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, TaskRunTwoTicks)
{
  using testing::_;
  startSlave();

  MockFakeTask mockTask;
  startTask("task0", &mockTask, ResourceHints::parse("cpus:4.0", ""));
  EXPECT_CALL(mockTask, getUsage(_, _)).
    WillRepeatedly(Return(Resources::parse("cpus:8.0")));
  trigger gotTaskUsageCall;
  EXPECT_CALL(mockTask, takeUsage(_, _, Resources::parse("cpus:4.0"))).
    WillOnce(DoAll(Trigger(&gotTaskUsageCall), Return(TASK_RUNNING)));
  process::Clock::advance(kTick);
  WAIT_UNTIL(gotTaskUsageCall);
  EXPECT_CALL(mockTask, takeUsage(_, _, Resources::parse("cpus:4.0"))).
    WillOnce(Return(TASK_FINISHED));
  mockMaster->expect<ExitedExecutorMessage>();
  tickAndUpdate("task0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraCPUPolicy)
{
  conf.set("fake_extra_cpu", "1");

  startSlave();
  expectIsolationPolicy("cpus:1.0", "cpus:1.0", "cpus:3.0", "cpus:3.0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraCPUPolicyDoesNotExceedCapacity)
{
  conf.set("fake_extra_cpu", "1");

  startSlave();
  expectIsolationPolicy("cpus:1.0", "cpus:1.0", "cpus:9.0", "cpus:4.0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraCPUPolicyDoesNotSteal)
{
  conf.set("fake_extra_cpu", "1");

  startSlave();
  MockFakeTask backgroundTask;
  makeBackgroundTask(&backgroundTask, "cpus:2.0", "", "cpus:1.0", "cpus:1.0");
  expectIsolationPolicy("cpus:1.0", "", "cpus:9.0", "cpus:3.0");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraCPUPolicyProportional)
{
  conf.set("fake_extra_cpu", "1");

  startSlave();
  MockFakeTask backgroundTask;
  makeBackgroundTask(&backgroundTask, "cpus:1.0", "", "cpus:3.0", "cpus:1.6");
  expectIsolationPolicy("cpus:1.5", "", "cpus:3.0", "cpus:2.4");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraCPUPolicyLessThanMax)
{
  conf.set("fake_extra_cpu", "1");

  startSlave();
  MockFakeTask backgroundTask;
  makeBackgroundTask(&backgroundTask, "cpus:1.0", "", "cpus:1.5", "cpus:1.5");
  expectIsolationPolicy("cpus:1.5", "", "cpus:3.0", "cpus:2.5");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraMemoryPolicy)
{
  conf.set("fake_extra_mem", "1");
  conf.set("fake_assign_min", "1");

  startSlave();
  expectIsolationPolicy("mem:1024", "", "mem:8192", "mem:4096");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraMemoryPolicyMin)
{
  conf.set("fake_extra_mem", "1");
  conf.set("fake_assign_min", "1");

  startSlave();
  MockFakeTask backgroundTask;
  makeBackgroundTask(&backgroundTask,
      "mem:1024", "mem:512", "mem:512", "mem:512");
  expectIsolationPolicy("mem:1024", "", "mem:8192", "mem:3584");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ExtraMemoryPolicyMinUnused)
{
  conf.set("fake_extra_mem", "1");
  conf.set("fake_assign_min", "1");

  startSlave();
  MockFakeTask backgroundTask;
  makeBackgroundTask(&backgroundTask,
      "mem:1024", "mem:512", "mem:128", "mem:128");
  expectIsolationPolicy("mem:1024", "", "mem:8192", "mem:3968");
  stopSlave();
}

// TODO(Charles Reiss): Decide on a policy for the uneven base allocation
// case.
TEST_F(FakeIsolationModuleTest, ExtraMemoryPolicyProportionalEqual)
{
  conf.set("fake_extra_mem", "1");
  conf.set("fake_assign_min", "1");

  startSlave();
  MockFakeTask backgroundTask;
  makeBackgroundTask(&backgroundTask,
      "mem:1024", "mem:512", "mem:4096", "mem:2048");
  expectIsolationPolicy("mem:1024", "mem:512", "mem:8192", "mem:2048");
  stopSlave();
}

TEST_F(FakeIsolationModuleTest, ReportUsageSimple)
{
  using testing::_;
  conf.set("fake_usage_interval",
           boost::lexical_cast<std::string>(kTick * 2.));
  startSlave();
  MockFakeTask mockTask;
  double start = process::Clock::now();
  startTask("task0", &mockTask, ResourceHints::parse("cpus:1.0;mem:0.5", ""));

  trigger tookFirst, tookSecond, tookThird;
  EXPECT_CALL(mockTask, getUsage(seconds(start), seconds(start + kTick))).
    WillRepeatedly(Return(Resources::parse("cpus:0.75;mem:0.25")));
  EXPECT_CALL(mockTask, getUsage(seconds(start + kTick),
                                 seconds(start + kTick * 2))).
    WillRepeatedly(Return(Resources::parse("cpus:0.625;mem:0.5")));
  EXPECT_CALL(mockTask, getUsage(seconds(start + kTick * 2),
                                 seconds(start + kTick * 3))).
    WillRepeatedly(Return(Resources::parse("cpus:0.25;mem:0.375")));
  EXPECT_CALL(mockTask, takeUsage(seconds(start), seconds(start + kTick), _)).
    WillOnce(DoAll(Trigger(&tookFirst), Return(TASK_RUNNING)));
  EXPECT_CALL(mockTask, takeUsage(
        seconds(start + kTick), seconds(start + kTick * 2), _)).
    WillOnce(DoAll(Trigger(&tookSecond), Return(TASK_RUNNING)));
  EXPECT_CALL(mockTask, takeUsage(
        seconds(start + kTick * 2), seconds(start + kTick * 3), _)).
    WillOnce(DoAll(Trigger(&tookThird), Return(TASK_FINISHED)));

  process::Clock::advance(kTick);
  WAIT_UNTIL(tookFirst);

  trigger gotFirstUsage;
  UsageMessage firstUsage;
  mockMaster->expectAndStore<UsageMessage>(process::UPID(), &firstUsage,
      &gotFirstUsage);
  process::Clock::advance(kTick);
  WAIT_UNTIL(tookSecond);
  WAIT_UNTIL(gotFirstUsage);
  process::Clock::advance(kTick);
  WAIT_UNTIL(tookThird);
  trigger gotSecondUsage;
  UsageMessage secondUsage;
  mockMaster->expectAndStore<UsageMessage>(process::UPID(), &secondUsage,
      &gotSecondUsage);
  process::Clock::advance(kTick);
  WAIT_UNTIL(gotSecondUsage);

  EXPECT_DOUBLE_EQ(start + kTick * 2.0, firstUsage.timestamp());
  EXPECT_DOUBLE_EQ(kTick * 2.0, firstUsage.duration());
  EXPECT_EQ(Resources::parse("cpus:0.6875;mem:0.5"), firstUsage.resources());
  EXPECT_EQ("task0", firstUsage.executor_id().value());
  FrameworkID expectFrameworkId = DEFAULT_FRAMEWORK_ID;
  EXPECT_EQ(expectFrameworkId, firstUsage.framework_id());
  EXPECT_DOUBLE_EQ(start + kTick * 4.0, secondUsage.timestamp());
  EXPECT_DOUBLE_EQ(kTick * 2.0, secondUsage.duration());
  EXPECT_EQ(Resources::parse("cpus:0.125;mem:0.375"), secondUsage.resources());
}

