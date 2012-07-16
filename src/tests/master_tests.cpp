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

#include <unistd.h>
#include <gmock/gmock.h>

#include <boost/smart_ptr/scoped_ptr.hpp>

#include <mesos/executor.hpp>
#include <mesos/scheduler.hpp>

#include "detector/detector.hpp"

#include "local/local.hpp"

#include "master/dominant_share_allocator.hpp"
#include "master/frameworks_manager.hpp"
#include "master/master.hpp"

#include <process/dispatch.hpp>
#include <process/future.hpp>

#include "slave/slave.hpp"

#include "tests/utils.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::test;

using mesos::internal::master::FrameworksManager;
using mesos::internal::master::FrameworksStorage;

using mesos::internal::master::Master;
using mesos::internal::master::DominantShareAllocator;
using mesos::internal::master::Allocator;

using mesos::internal::slave::Slave;

using boost::scoped_ptr;

using process::PID;
using process::Clock;
using process::Future;
using process::PID;

using std::string;
using std::map;
using std::vector;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Eq;
using testing::Return;
using testing::SaveArg;

// Master and Slave together test cases.

MATCHER_P(UpdateForTaskId, id, "") {
  return testing::Matcher<std::string>(id).Matches(arg.task_id().value());
}


class MasterSlaveTest : public testing::Test {
protected:
  MasterSlaveTest() {
    useMockAllocator = false;
  }

  void setupExecutors() {
    EXPECT_CALL(exec, registered(_, _, _, _))
      .Times(AtMost(1))
      .WillOnce(SaveArg<0>(&execDriver));

    EXPECT_CALL(exec, shutdown(_))
      .Times(AtMost(1))
      .WillOnce(Trigger(&shutdownCall));

    map<ExecutorID, Executor*> execs;
    execs[DEFAULT_EXECUTOR_ID] = &exec;

    isolationModule.reset(new TestingIsolationModule(execs));
  }

  void startMasterAndSlave() {
    trigger gotSlave;
    ASSERT_TRUE(GTEST_IS_THREADSAFE);
    Allocator* allocPtr = &allocator;
    if (useMockAllocator) {
      allocPtr = &mockAllocator;
      EXPECT_CALL(mockAllocator, initialize(_)).Times(1);
      EXPECT_CALL(mockAllocator, slaveAdded(_, _, _)).Times(1);
      EXPECT_CALL(mockAllocator, frameworkAdded(_, _)).Times(1);
    }
    m.reset(new Master(allocPtr));
    master = process::spawn(m.get());
    Resources resources = Resources::parse("cpus:2;mem:1024");

    if (!isolationModule.get()) {
      setupExecutors();
    } else {
      shutdownCall.value = 1;
    }

    s.reset(new Slave(resources, true, isolationModule.get()));
    slave = process::spawn(s.get());

    detector.reset(new BasicMasterDetector(master, slave, true));

    FrameworkInfo info;
    info.set_name("");

    schedDriver.reset(new MesosSchedulerDriver(&sched, info,
                                               master));
  }

  void getOffers(vector<Offer>* offers) {
    trigger resourceOfferCall;

    EXPECT_CALL(sched, registered(schedDriver.get(),_,_))
      .WillOnce(SaveArg<1>(&frameworkId));

    EXPECT_CALL(sched, resourceOffers(schedDriver.get(), _))
      .WillOnce(DoAll(SaveArg<1>(offers),
                      Trigger(&resourceOfferCall)))
      .WillRepeatedly(Return());

    schedDriver->start();

    WAIT_UNTIL(resourceOfferCall);

    ASSERT_NE(0, offers->size());
  }

  void getMoreOffers(vector<Offer>* offers, trigger* gotOffers)
  {
    EXPECT_CALL(sched, resourceOffers(schedDriver.get(), _))
      .WillOnce(DoAll(SaveArg<1>(offers),
                      Trigger(gotOffers)));
  }

  void launchTaskForOffer(const Offer& offer, const std::string& taskId,
                          TaskState expectState = TASK_RUNNING)
  {
    vector<std::string> taskIds;
    taskIds.push_back(taskId);
    LOG(INFO) << "Launching for " << offer.DebugString();
    launchTasksForOffer(offer, taskIds, offer.resources(), expectState);
  }

  void launchTasksForOffer(const Offer& offer,
                           const vector<std::string>& taskIds,
                           const Resources& perTaskResources,
                           TaskState expectState = TASK_RUNNING)
  {
    vector<trigger> statusUpdateCalls;
    statusUpdateCalls.resize(taskIds.size());
    vector<TaskStatus> statuses;
    statuses.resize(taskIds.size());

    vector<TaskInfo> tasks;

    if (expectState != TASK_LOST) {
      EXPECT_CALL(exec, launchTask(_, _))
        .Times(taskIds.size())
        .WillRepeatedly(SendStatusUpdate(expectState));
    } else {
      shutdownCall.value = 1;
    }
    for (int i = 0; i < taskIds.size(); ++i) {
      const std::string& taskId = taskIds[i];

      TaskInfo task;
      task.set_name("");
      task.mutable_task_id()->set_value(taskId);
      task.mutable_slave_id()->MergeFrom(offer.slave_id());
      task.mutable_resources()->MergeFrom(perTaskResources);
      task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);
      tasks.push_back(task);

      EXPECT_CALL(sched, statusUpdate(schedDriver.get(),
                                      UpdateForTaskId(taskId)))
        .WillOnce(DoAll(SaveArg<1>(&statuses[i]),
                        Trigger(&statusUpdateCalls[i])));
    }

    schedDriver->launchTasks(offer.id(), tasks);

    for (int i = 0; i < statusUpdateCalls.size(); ++i) {
      WAIT_UNTIL(statusUpdateCalls[i]);
      EXPECT_EQ(expectState, statuses[i].state()) << statuses[i].DebugString();
    }

    if (expectState == TASK_RUNNING) {
      ASSERT_TRUE(execDriver);
    }
  }

  void stopScheduler() {
    if (useMockAllocator) {
      EXPECT_CALL(mockAllocator, frameworkDeactivated(_)).Times(1);
      EXPECT_CALL(mockAllocator, frameworkRemoved(_)).Times(1);
    }
    schedDriver->stop();
    schedDriver->join();
    WAIT_UNTIL(shutdownCall); // To ensure can deallocate MockExecutor.
  }

  void stopMasterAndSlave(bool shutdownAllocator = true)
  {
    if (useMockAllocator) {
      EXPECT_CALL(mockAllocator, slaveRemoved(_)).Times(1);
    }
    LOG(INFO) << "Asking slave to terminate";

    process::terminate(slave);
    process::wait(slave);

    s.reset(0);

    LOG(INFO) << "Asking master to terminate";

    process::terminate(master);
    process::wait(master);

    m.reset(0);
 
    // XXX FIXME Do we need to wait for the allocator?
  }

  trigger shutdownCall;
  DominantShareAllocator allocator;
  MockAllocator<DominantShareAllocator> mockAllocator;
  bool useMockAllocator;
  scoped_ptr<Master> m;
  PID<Master> master;
  MockExecutor exec;
  ExecutorDriver* execDriver;
  scoped_ptr<TestingIsolationModule> isolationModule;
  scoped_ptr<Slave> s;
  PID<Slave> slave;
  scoped_ptr<BasicMasterDetector> detector;
  MockScheduler sched;
  FrameworkID frameworkId;
  scoped_ptr<MesosSchedulerDriver> schedDriver;
};

TEST_F(MasterSlaveTest, TaskRunning)
{
  startMasterAndSlave();
  vector<Offer> offers;
  getOffers(&offers);
  launchTaskForOffer(offers[0], "testTaskId");
  stopScheduler();
  stopMasterAndSlave();
}

#if 0 // requires allocator to make min offers...
TEST_F(MasterSlaveTest, AllocateMinimumIfMarked)
{
  useMockAllocator = true;
  EXPECT_CALL(sched, allocatesMin()).
    WillRepeatedly(testing::Return(true));
  startMasterAndSlave();
  vector<Offer> offers;
  getOffers(&offers);
  launchTaskForOffer(offers[0], "testTaskId");
  stopScheduler();
  stopMasterAndSlave();
  EXPECT_EQ(ResourceHints(Resources::parse(""),
                          Resources::parse("cpus:2;mem:1024")),
      isolationModule->lastResources[DEFAULT_EXECUTOR_ID]);
}
#endif

TEST_F(MasterSlaveTest, RejectMinimumMoreThanOffered)
{
  useMockAllocator = true;
  EXPECT_CALL(sched, allocatesMin()).
    WillRepeatedly(testing::Return(true));
  startMasterAndSlave();
  vector<Offer> offers;
  getOffers(&offers);
  offers[0].clear_resources();
  offers[0].mutable_resources()->MergeFrom(
      Resources::parse("cpus:4;mem:4096"));
  EXPECT_CALL(mockAllocator, resourcesUnused(_, _, _, _))
    .WillOnce(Return());
  launchTaskForOffer(offers[0], "testTaskId", TASK_LOST);
  stopScheduler();
  stopMasterAndSlave(false);
}

TEST_F(MasterSlaveTest, KillTask)
{
  trigger killTaskCall;

  startMasterAndSlave();
  vector<Offer> offers;
  getOffers(&offers);
  launchTaskForOffer(offers[0], "testTaskId");

  EXPECT_CALL(exec, killTask(_, _))
    .WillOnce(Trigger(&killTaskCall));

  TaskID taskId;
  taskId.set_value("testTaskId");

  schedDriver->killTask(taskId);

  WAIT_UNTIL(killTaskCall);

  stopScheduler();
  stopMasterAndSlave();
}

TEST_F(MasterSlaveTest, ClearFilterOnEndTask)
{
  process::Clock::pause();
  vector<std::string> taskIds;
  taskIds.push_back("task0");
  taskIds.push_back("task1");
  taskIds.push_back("task2");

  startMasterAndSlave();

  // Launch 3 tasks.
  vector<Offer> offers;
  getOffers(&offers);
  launchTasksForOffer(offers[0], taskIds,
                      Resources::parse("cpus:0.5;mem:256"));

  // Kill one of them.
  {
    trigger gotMoreOffers;
    trigger killTaskCall;
    EXPECT_CALL(exec, killTask(_, _))
      .WillOnce(DoAll(SendStatusUpdateForId(TASK_KILLED),
                      Trigger(&killTaskCall)));
    TaskID taskId;
    taskId.set_value("task0");
    EXPECT_CALL(sched, statusUpdate(schedDriver.get(),
                                    UpdateForTaskId("task0")));
    getMoreOffers(&offers, &gotMoreOffers);
    schedDriver->killTask(taskId);

    WAIT_UNTIL(killTaskCall);
    WAIT_UNTIL(gotMoreOffers);
  }

  // We should get a new offer; after we get it, don't launch any tasks in it;
  // this should create a filter.
  vector<TaskInfo> empty;
  schedDriver->launchTasks(offers[0].id(), empty);

  // Kill a second task. Dispite the filter, we should get more offers because
  // we have freed up resources.
  {
    trigger gotMoreOffers;
    trigger killTaskCall;
    trigger gotUpdate;
    EXPECT_CALL(exec, killTask(_, _))
      .WillOnce(DoAll(SendStatusUpdateForId(TASK_KILLED),
                      Trigger(&killTaskCall)));
    TaskID taskId;
    taskId.set_value("task1");
    EXPECT_CALL(sched, statusUpdate(schedDriver.get(),
                                    UpdateForTaskId("task1")))
      .WillOnce(Trigger(&gotUpdate));
    getMoreOffers(&offers, &gotMoreOffers);
    schedDriver->killTask(taskId);
    WAIT_UNTIL(gotMoreOffers);
    WAIT_UNTIL(gotUpdate);
  }

  stopScheduler();
  stopMasterAndSlave();
  process::Clock::resume();
}

TEST_F(MasterSlaveTest, FrameworkMessage)
{
  startMasterAndSlave();
  vector<Offer> offers;
  getOffers(&offers);
  launchTaskForOffer(offers[0], "testTaskId");

  string execData;

  trigger execFrameworkMessageCall;

  EXPECT_CALL(exec, frameworkMessage(_, _))
    .WillOnce(DoAll(SaveArg<1>(&execData),
                    Trigger(&execFrameworkMessageCall)));

  trigger schedFrameworkMessageCall;

  string hello = "hello";

  schedDriver->sendFrameworkMessage(DEFAULT_EXECUTOR_ID,
                                    offers[0].slave_id(),
				    hello);

  WAIT_UNTIL(execFrameworkMessageCall);

  EXPECT_EQ(hello, execData);

  string reply = "reply";

  string schedData;

  EXPECT_CALL(sched, frameworkMessage(schedDriver.get(), _, _, _))
    .WillOnce(DoAll(SaveArg<3>(&schedData),
                    Trigger(&schedFrameworkMessageCall)));

  execDriver->sendFrameworkMessage(reply);

  WAIT_UNTIL(schedFrameworkMessageCall);

  EXPECT_EQ(reply, schedData);

  stopScheduler();
  stopMasterAndSlave();
}

TEST_F(MasterSlaveTest, MultipleExecutors)
{
  MockExecutor exec1;
  TaskInfo exec1Task;
  trigger exec1LaunchTaskCall, exec1ShutdownCall;

  EXPECT_CALL(exec1, registered(_, _, _, _))
    .Times(1);

  EXPECT_CALL(exec1, launchTask(_, _))
    .WillOnce(DoAll(SaveArg<1>(&exec1Task),
                    Trigger(&exec1LaunchTaskCall),
                    SendStatusUpdate(TASK_RUNNING)));

  EXPECT_CALL(exec1, shutdown(_))
    .WillOnce(Trigger(&exec1ShutdownCall));

  MockExecutor exec2;
  TaskInfo exec2Task;
  trigger exec2LaunchTaskCall, exec2ShutdownCall;

  EXPECT_CALL(exec2, registered(_, _, _, _))
    .Times(1);

  EXPECT_CALL(exec2, launchTask(_, _))
    .WillOnce(DoAll(SaveArg<1>(&exec2Task),
                    Trigger(&exec2LaunchTaskCall),
                    SendStatusUpdate(TASK_RUNNING)));

  EXPECT_CALL(exec2, shutdown(_))
    .WillOnce(Trigger(&exec2ShutdownCall));

  ExecutorID executorId1;
  executorId1.set_value("executor-1");

  ExecutorID executorId2;
  executorId2.set_value("executor-2");

  map<ExecutorID, Executor*> execs;
  execs[executorId1] = &exec1;
  execs[executorId2] = &exec2;

  isolationModule.reset(new TestingIsolationModule(execs));

  startMasterAndSlave();
  vector<Offer> offers;
  getOffers(&offers);

  TaskStatus status1, status2;

  trigger resourceOffersCall, statusUpdateCall1, statusUpdateCall2;

  EXPECT_CALL(sched, statusUpdate(schedDriver.get(), _))
    .WillOnce(DoAll(SaveArg<1>(&status1), Trigger(&statusUpdateCall1)))
    .WillOnce(DoAll(SaveArg<1>(&status2), Trigger(&statusUpdateCall2)));


  ExecutorInfo executor1; // Bug in gcc 4.1.*, must assign on next line.
  executor1 = CREATE_EXECUTOR_INFO(executorId1, "exit 1");

  TaskInfo task1;
  task1.set_name("");
  task1.mutable_task_id()->set_value("1");
  task1.mutable_slave_id()->MergeFrom(offers[0].slave_id());
  task1.mutable_resources()->MergeFrom(Resources::parse("cpus:1;mem:512"));
  task1.mutable_executor()->MergeFrom(executor1);

  ExecutorInfo executor2; // Bug in gcc 4.1.*, must assign on next line.
  executor2 = CREATE_EXECUTOR_INFO(executorId2, "exit 1");

  TaskInfo task2;
  task2.set_name("");
  task2.mutable_task_id()->set_value("2");
  task2.mutable_slave_id()->MergeFrom(offers[0].slave_id());
  task2.mutable_resources()->MergeFrom(Resources::parse("cpus:1;mem:512"));
  task2.mutable_executor()->MergeFrom(executor2);

  vector<TaskInfo> tasks;
  tasks.push_back(task1);
  tasks.push_back(task2);

  schedDriver->launchTasks(offers[0].id(), tasks);

  WAIT_UNTIL(statusUpdateCall1);

  EXPECT_EQ(TASK_RUNNING, status1.state());

  WAIT_UNTIL(statusUpdateCall2);

  EXPECT_EQ(TASK_RUNNING, status2.state());

  WAIT_UNTIL(exec1LaunchTaskCall);

  EXPECT_EQ(task1.task_id(), exec1Task.task_id());

  WAIT_UNTIL(exec2LaunchTaskCall);

  EXPECT_EQ(task2.task_id(), exec2Task.task_id());

  stopScheduler();

  WAIT_UNTIL(exec1ShutdownCall);
  WAIT_UNTIL(exec2ShutdownCall);

  stopMasterAndSlave();
}

TEST_F(MasterSlaveTest, AccumulateUsage) {
  useMockAllocator = true;
  startMasterAndSlave();
  OfferID offerId;
  vector<Offer> offers;
  getOffers(&offers);
  launchTaskForOffer(offers[0], "testTaskId");
  UsageMessage usage;
  // FIXME XXX Don't use these methods anymore; instead get the JSON blob?
  usage.mutable_slave_id()->MergeFrom(s->getId());
  usage.mutable_framework_id()->MergeFrom(frameworkId);
  usage.mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
  usage.mutable_resources()->MergeFrom(Resources::parse("cpus:1.5;mem:800"));
  usage.set_timestamp(1000.0);
  usage.set_duration(250.0);
  trigger gotUsage;
  EXPECT_CALL(mockAllocator, gotUsage(testing::_)).
    WillOnce(Trigger(&gotUsage));
  process::post(master, usage);
  WAIT_UNTIL(gotUsage);
#if 0 // XXX FIXME use json stuff to test.
  EXPECT_EQ(Resources::parse("cpus:1.5;mem:800"),
            m->getActiveSlaves()[0]->resourcesObservedUsed);
#endif
  EXPECT_CALL(mockAllocator, executorRemoved(_, _, _));
  EXPECT_CALL(mockAllocator, resourcesRecovered(_, _, _));
  stopScheduler();
#if 0
  EXPECT_EQ(Resources::parse("cpus:0;mem:0"),
            m->getActiveSlaves()[0]->resourcesObservedUsed);
#endif
  stopMasterAndSlave();
}

TEST(MasterTest, MasterInfo)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  MockFilter filter;
  process::filter(&filter);

  EXPECT_MESSAGE(filter, _, _, _)
    .WillRepeatedly(Return(false));

  DominantShareAllocator a;
  Master m(&a);
  PID<Master> master = process::spawn(&m);

  MockExecutor exec;

  map<ExecutorID, Executor*> execs;
  execs[DEFAULT_EXECUTOR_ID] = &exec;

  TestingIsolationModule isolationModule(execs);

  Resources resources = Resources::parse("cpus:2;mem:1024");

  Slave s(resources, true, &isolationModule);
  PID<Slave> slave = process::spawn(&s);

  BasicMasterDetector detector(master, slave, true);

  MockScheduler sched;
  MesosSchedulerDriver driver(&sched, DEFAULT_FRAMEWORK_INFO, master);

  MasterInfo masterInfo;

  trigger registeredCall;

  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(DoAll(SaveArg<2>(&masterInfo),
                    Trigger(&registeredCall)));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillRepeatedly(Return());

  driver.start();

  WAIT_UNTIL(registeredCall);

  EXPECT_EQ(master.port, masterInfo.port());
  EXPECT_EQ(master.ip, masterInfo.ip());

  driver.stop();
  driver.join();

  process::terminate(slave);
  process::wait(slave);

  process::terminate(master);
  process::wait(master);

  process::filter(NULL);
}


TEST(MasterTest, MasterInfoOnReElection)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  MockFilter filter;
  process::filter(&filter);

  EXPECT_MESSAGE(filter, _, _, _)
    .WillRepeatedly(Return(false));

  DominantShareAllocator a;
  Master m(&a);
  PID<Master> master = process::spawn(&m);

  MockExecutor exec;

  map<ExecutorID, Executor*> execs;
  execs[DEFAULT_EXECUTOR_ID] = &exec;

  TestingIsolationModule isolationModule(execs);

  Resources resources = Resources::parse("cpus:2;mem:1024");

  Slave s(resources, true, &isolationModule);
  PID<Slave> slave = process::spawn(&s);

  BasicMasterDetector detector(master, slave, true);

  MockScheduler sched;
  MesosSchedulerDriver driver(&sched, DEFAULT_FRAMEWORK_INFO, master);

  MasterInfo masterInfo;

  trigger registeredCall, reregisteredCall;

  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(Trigger(&registeredCall));

  EXPECT_CALL(sched, reregistered(&driver, _))
    .WillOnce(DoAll(SaveArg<1>(&masterInfo),Trigger(&reregisteredCall)));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillRepeatedly(Return());

  process::Message message;

  EXPECT_MESSAGE(filter, Eq(FrameworkRegisteredMessage().GetTypeName()), _, _)
    .WillOnce(DoAll(SaveArgField<0>(&process::MessageEvent::message, &message),
                    Return(false)));

  driver.start();

  WAIT_UNTIL(registeredCall);

  // Simulate a spurious newMasterDetected event (e.g., due to ZooKeeper
  // expiration) at the scheduler.
  NewMasterDetectedMessage newMasterDetectedMsg;
  newMasterDetectedMsg.set_pid(master);

  process::post(message.to, newMasterDetectedMsg);

  WAIT_UNTIL(reregisteredCall);

  EXPECT_EQ(master.port, masterInfo.port());
  EXPECT_EQ(master.ip, masterInfo.ip());

  driver.stop();
  driver.join();

  process::terminate(slave);
  process::wait(slave);

  process::terminate(master);
  process::wait(master);

  process::filter(NULL);
}


class WhitelistFixture : public ::testing::Test
{
protected:
  WhitelistFixture()
    : path("whitelist.txt")
  {}

  virtual ~WhitelistFixture()
  {
    utils::os::rm(path);
  }

  const string path;
};


TEST_F(WhitelistFixture, WhitelistSlave)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  MockFilter filter;
  process::filter(&filter);

  EXPECT_MESSAGE(filter, _, _, _)
    .WillRepeatedly(Return(false));

  // Add some hosts to the white list.
  Try<string> hostname = utils::os::hostname();
  ASSERT_TRUE(hostname.isSome());
  string hosts = hostname.get() + "\n" + "dummy-slave";
  CHECK (utils::os::write(path, hosts).isSome()) << "Error writing whitelist";

  DominantShareAllocator a;
  Configuration conf;
  conf.set("whitelist", "file://" + path);
  Master m(&a, conf);
  PID<Master> master = process::spawn(&m);

  trigger slaveRegisteredMsg;

  EXPECT_MESSAGE(filter, Eq(SlaveRegisteredMessage().GetTypeName()), _, _)
    .WillOnce(DoAll(Trigger(&slaveRegisteredMsg), Return(false)));

  MockExecutor exec;

  map<ExecutorID, Executor*> execs;
  execs[DEFAULT_EXECUTOR_ID] = &exec;

  TestingIsolationModule isolationModule(execs);

  Resources resources = Resources::parse("cpus:2;mem:1024");

  Slave s(resources, true, &isolationModule);
  PID<Slave> slave = process::spawn(&s);

  BasicMasterDetector detector(master, slave, true);

  MockScheduler sched;
  MesosSchedulerDriver driver(&sched, DEFAULT_FRAMEWORK_INFO, master);

  MasterInfo masterInfo;

  trigger registeredCall, reregisteredCall;

  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(Trigger(&registeredCall));

  trigger resourceOffersCall;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(Trigger(&resourceOffersCall));

  driver.start();

  WAIT_UNTIL(slaveRegisteredMsg);

  WAIT_UNTIL(resourceOffersCall);

  driver.stop();
  driver.join();

  process::terminate(slave);
  process::wait(slave);

  process::terminate(master);
  process::wait(master);

  process::filter(NULL);
}


// FrameworksManager test cases.

class MockFrameworksStorage : public FrameworksStorage
{
public:
  // We need this typedef because MOCK_METHOD is a macro.
  typedef map<FrameworkID, FrameworkInfo> Map_FrameworkId_FrameworkInfo;

  MOCK_METHOD0(list, Future<Result<Map_FrameworkId_FrameworkInfo> >());
  MOCK_METHOD2(add, Future<Result<bool> >(const FrameworkID&,
                                          const FrameworkInfo&));
  MOCK_METHOD1(remove, Future<Result<bool> >(const FrameworkID&));
};


TEST(MasterTest, MasterLost)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  MockFilter filter;
  process::filter(&filter);

  EXPECT_MESSAGE(filter, _, _, _)
    .WillRepeatedly(Return(false));

  DominantShareAllocator a;
  Master m(&a);
  PID<Master> master = process::spawn(&m);

  MockExecutor exec;

  map<ExecutorID, Executor*> execs;
  execs[DEFAULT_EXECUTOR_ID] = &exec;

  TestingIsolationModule isolationModule(execs);

  Resources resources = Resources::parse("cpus:2;mem:1024");

  Slave s(resources, true, &isolationModule);
  PID<Slave> slave = process::spawn(&s);

  BasicMasterDetector detector(master, slave, true);

  MockScheduler sched;
  MesosSchedulerDriver driver(&sched, DEFAULT_FRAMEWORK_INFO, master);

  MasterInfo masterInfo;

  trigger registeredCall, disconnectedCall;

  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(Trigger(&registeredCall));

  EXPECT_CALL(sched, disconnected(&driver))
    .WillOnce(Trigger(&disconnectedCall));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillRepeatedly(Return());

  process::Message message;

  EXPECT_MESSAGE(filter, Eq(FrameworkRegisteredMessage().GetTypeName()), _, _)
    .WillOnce(DoAll(SaveArgField<0>(&process::MessageEvent::message, &message),
                    Return(false)));

  driver.start();

  WAIT_UNTIL(registeredCall);

  // Simulate a spurious noMasterDetected event at the scheduler.
  NoMasterDetectedMessage noMasterDetectedMsg;
  process::post(message.to, noMasterDetectedMsg);

  WAIT_UNTIL(disconnectedCall);

  driver.stop();
  driver.join();

  process::terminate(slave);
  process::wait(slave);

  process::terminate(master);
  process::wait(master);

  process::filter(NULL);
}

// This fixture sets up expectations on the storage class
// and spawns both storage and frameworks manager.
class FrameworksManagerTestFixture : public ::testing::Test
{
protected:
  virtual void SetUp()
  {
    ASSERT_TRUE(GTEST_IS_THREADSAFE);

    storage = new MockFrameworksStorage();
    process::spawn(storage);

    EXPECT_CALL(*storage, list())
      .WillOnce(Return(Result<map<FrameworkID, FrameworkInfo> >(infos)));

    EXPECT_CALL(*storage, add(_, _))
      .WillRepeatedly(Return(Result<bool>::some(true)));

    EXPECT_CALL(*storage, remove(_))
      .WillRepeatedly(Return(Result<bool>::some(true)));

    manager = new FrameworksManager(storage);
    process::spawn(manager);
  }

  virtual void TearDown()
  {
    process::terminate(manager);
    process::wait(manager);
    delete manager;

    process::terminate(storage);
    process::wait(storage);
    delete storage;
  }

  map<FrameworkID, FrameworkInfo> infos;

  MockFrameworksStorage* storage;
  FrameworksManager* manager;
};


TEST_F(FrameworksManagerTestFixture, AddFramework)
{
  // Test if initially FM returns empty list.
  Future<Result<map<FrameworkID, FrameworkInfo> > > future =
    process::dispatch(manager, &FrameworksManager::list);

  ASSERT_TRUE(future.await(2.0));
  EXPECT_TRUE(future.get().get().empty());

  // Add a dummy framework.
  FrameworkID id;
  id.set_value("id");

  FrameworkInfo info;
  info.set_name("test name");
  info.set_user("test user");

  // Add the framework.
  Future<Result<bool> > future2 =
    process::dispatch(manager, &FrameworksManager::add, id, info);

  ASSERT_TRUE(future2.await(2.0));
  EXPECT_TRUE(future2.get().get());

  // Check if framework manager returns the added framework.
  Future<Result<map<FrameworkID, FrameworkInfo> > > future3 =
    process::dispatch(manager, &FrameworksManager::list);

  ASSERT_TRUE(future3.await(2.0));

  map<FrameworkID, FrameworkInfo> result = future3.get().get();

  ASSERT_EQ(1, result.count(id));
  EXPECT_EQ("test name", result[id].name());
  EXPECT_EQ("test user", result[id].user());

  // Check if the framework exists.
  Future<Result<bool> > future4 =
    process::dispatch(manager, &FrameworksManager::exists, id);

  ASSERT_TRUE(future4.await(2.0));
  EXPECT_TRUE(future4.get().get());
}


TEST_F(FrameworksManagerTestFixture, RemoveFramework)
{
  Clock::pause();

  // Remove a non-existent framework.
  FrameworkID id;
  id.set_value("non-existent framework");

  Future<Result<bool> > future1 =
    process::dispatch(manager, &FrameworksManager::remove, id, seconds(0));

  ASSERT_TRUE(future1.await(2.0));
  EXPECT_TRUE(future1.get().isError());

  // Remove an existing framework.

  // First add a dummy framework.
  FrameworkID id2;
  id2.set_value("id2");

  FrameworkInfo info2;
  info2.set_name("test name");
  info2.set_user("test user");

  // Add the framework.
  Future<Result<bool> > future2 =
    process::dispatch(manager, &FrameworksManager::add, id2, info2);

  ASSERT_TRUE(future2.await(2.0));
  EXPECT_TRUE(future2.get().get());

  // Now remove the added framework.
  Future<Result<bool> > future3 =
    process::dispatch(manager, &FrameworksManager::remove, id2, seconds(1.0));

  Clock::update(Clock::now(manager) + 1.0);

  ASSERT_TRUE(future3.await(2.0));
  EXPECT_TRUE(future2.get().get());

  // Now check if the removed framework exists...it shouldn't.
  Future<Result<bool> > future4 =
    process::dispatch(manager, &FrameworksManager::exists, id2);

  ASSERT_TRUE(future4.await(2.0));
  EXPECT_FALSE(future4.get().get());

  Clock::resume();
}


TEST_F(FrameworksManagerTestFixture, ResurrectFramework)
{
  // Resurrect a non-existent framework.
  FrameworkID id;
  id.set_value("non-existent framework");

  Future<Result<bool> > future1 =
    process::dispatch(manager, &FrameworksManager::resurrect, id);

  ASSERT_TRUE(future1.await(2.0));
  EXPECT_FALSE(future1.get().get());

  // Resurrect an existent framework that is NOT being removed.
  // Add a dummy framework.
  FrameworkID id2;
  id2.set_value("id2");

  FrameworkInfo info2;
  info2.set_name("test name");
  info2.set_user("test user");

  // Add the framework.
  Future<Result<bool> > future2 =
    process::dispatch(manager, &FrameworksManager::add, id2, info2);

  ASSERT_TRUE(future2.await(2.0));
  EXPECT_TRUE(future2.get().get());

  Future<Result<bool> > future3 =
    process::dispatch(manager, &FrameworksManager::resurrect, id2);

  ASSERT_TRUE(future3.await(2.0));
  EXPECT_TRUE(future3.get().get());
}


// TODO(vinod): Using a paused clock in the tests means that
// future.await() may wait forever. This makes debugging hard.
TEST_F(FrameworksManagerTestFixture, ResurrectExpiringFramework)
{
  // This is the crucial test.
  // Resurrect an existing framework that is being removed,is being removed,
  // which should cause the remove to be unsuccessful.

  // Add a dummy framework.
  FrameworkID id;
  id.set_value("id");

  FrameworkInfo info;
  info.set_name("test name");
  info.set_user("test user");

  // Add the framework.
  process::dispatch(manager, &FrameworksManager::add, id, info);

  Clock::pause();

  // Remove after 2 secs.
  Future<Result<bool> > future1 =
    process::dispatch(manager, &FrameworksManager::remove, id, seconds(2.0));

  // Resurrect in the meanwhile.
  Future<Result<bool> > future2 =
    process::dispatch(manager, &FrameworksManager::resurrect, id);

  ASSERT_TRUE(future2.await(2.0));
  EXPECT_TRUE(future2.get().get());

  Clock::update(Clock::now(manager) + 2.0);

  ASSERT_TRUE(future1.await(2.0));
  EXPECT_FALSE(future1.get().get());

  Clock::resume();
}


TEST_F(FrameworksManagerTestFixture, ResurrectInterspersedExpiringFrameworks)
{
  // This is another crucial test.
  // Two remove messages are interspersed with a resurrect.
  // Only the second remove should actually remove the framework.

  // Add a dummy framework.
  FrameworkID id;
  id.set_value("id");

  FrameworkInfo info;
  info.set_name("test name");
  info.set_user("test user");

  // Add the framework.
  process::dispatch(manager, &FrameworksManager::add, id, info);

  Clock::pause();

  Future<Result<bool> > future1 =
    process::dispatch(manager, &FrameworksManager::remove, id, seconds(2.0));

  // Resurrect in the meanwhile.
  Future<Result<bool> > future2 =
    process::dispatch(manager, &FrameworksManager::resurrect, id);

  // Remove again.
  Future<Result<bool> > future3 =
    process::dispatch(manager, &FrameworksManager::remove, id, seconds(1.0));

  ASSERT_TRUE(future2.await(2.0));
  EXPECT_TRUE(future2.get().get());

  Clock::update(Clock::now(manager) + 1.0);

  ASSERT_TRUE(future3.await(2.0));
  EXPECT_TRUE(future3.get().get());

  Clock::update(Clock::now(manager) + 2.0);

  ASSERT_TRUE(future1.await(2.0));
  EXPECT_FALSE(future1.get().get());

  Clock::resume();
}


// Not deriving from fixture...because we want to set specific expectations.
// Specifically we simulate caching failure in FrameworksManager.
TEST(FrameworksManagerTest, CacheFailure)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  MockFrameworksStorage storage;
  process::spawn(storage);

  Result<map<FrameworkID, FrameworkInfo> > errMsg =
    Result<map<FrameworkID, FrameworkInfo> >::error("Fake Caching Error.");

  EXPECT_CALL(storage, list())
    .Times(2)
    .WillRepeatedly(Return(errMsg));

  EXPECT_CALL(storage, add(_, _))
    .WillOnce(Return(Result<bool>::some(true)));

  EXPECT_CALL(storage, remove(_))
    .Times(0);

  FrameworksManager manager(&storage);
  process::spawn(manager);

  // Test if initially FrameworksManager returns error.
  Future<Result<map<FrameworkID, FrameworkInfo> > > future1 =
    process::dispatch(manager, &FrameworksManager::list);

  ASSERT_TRUE(future1.await(2.0));
  ASSERT_TRUE(future1.get().isError());
  EXPECT_EQ(future1.get().error(), "Error caching framework infos.");

  // Add framework should function normally despite caching failure.
  FrameworkID id;
  id.set_value("id");

  FrameworkInfo info;
  info.set_name("test name");
  info.set_user("test user");

  // Add the framework.
  Future<Result<bool> > future2 =
    process::dispatch(manager, &FrameworksManager::add, id, info);

  ASSERT_TRUE(future2.await(2.0));
  EXPECT_TRUE(future2.get().get());

  // Remove framework should fail due to caching failure.
  Future<Result<bool> > future3 =
    process::dispatch(manager, &FrameworksManager::remove, id, seconds(0));

  ASSERT_TRUE(future3.await(2.0));
  ASSERT_TRUE(future3.get().isError());
  EXPECT_EQ(future3.get().error(), "Error caching framework infos.");

  process::terminate(manager);
  process::wait(manager);

  process::terminate(storage);
  process::wait(storage);
}
