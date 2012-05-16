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

#include <vector>

#include <boost/smart_ptr/scoped_ptr.hpp>

#include <gmock/gmock.h>

#include <process/process.hpp>

#include "master/allocator.hpp"
#include "master/master.hpp"

#include "tests/utils.hpp"

namespace mesos { namespace internal { namespace test {

using boost::scoped_ptr;

using mesos::internal::master::Master;

using process::PID;

using std::vector;

using testing::_;
using testing::AnyNumber;
using testing::Eq;
using testing::DoAll;
using testing::Return;

MATCHER_P2(WithResourceHints, resources, minResources, "") {
  return (testing::Matcher<Resources>(resources).Matches(arg.expectedResources) &&
          testing::Matcher<Resources>(minResources).Matches(arg.minResources));
}

MATCHER_P(EqProto, otherProto, "") {
  return arg.SerializeAsString() == otherProto.SerializeAsString();
}

class MasterAllocatorTest : public testing::Test {
protected:
  void SetUp()
  {
    process::Clock::pause();
    EXPECT_MESSAGE(filter, _, _, _).
      WillRepeatedly(Return(false));
    process::filter(&filter);
  }

  void TearDown()
  {
    EXPECT_CALL(allocator, taskRemoved(_)).Times(AnyNumber());
    EXPECT_CALL(allocator, executorRemoved(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(allocator, resourcesRecovered(_, _, _)).Times(AnyNumber());
    if (frameworkPid) {
      EXPECT_CALL(allocator, frameworkRemoved(_)).Times(1);
    }
    if (slavePid) {
      EXPECT_CALL(allocator, slaveRemoved(_)).Times(1);
    }
    process::terminate(masterPid);
    process::wait(masterPid);
    if (slavePid) {
      process::terminate(slavePid);
      process::wait(slavePid);
    }
    if (frameworkPid) {
      process::terminate(frameworkPid);
      process::wait(frameworkPid);
    }
    process::filter(0);
    process::Clock::resume();
    master.reset(0);
    masterPid = PID<Master>();
  }

  void detectMaster()
  {
    master.reset(new Master(&allocator));
    masterPid = process::spawn(master.get());
    EXPECT_CALL(allocator, initialize(master.get(), _)).
      Times(1);
    GotMasterTokenMessage tokenMessage;
    tokenMessage.set_token("test-token");
    process::post(masterPid, tokenMessage);
    NewMasterDetectedMessage msg;
    msg.set_pid(masterPid);
    process::post(masterPid, msg);
  }

  bool slavePong()
  {
    LOG(INFO) << "slavePong";
    slave->send(masterPid, "PONG");
    return true;
  }

  void registerSlave()
  {
    CHECK(master.get());
    using testing::Invoke;
    slave.reset(new FakeProtobufProcess);
    slave->setFilter(&filter);
    slavePid = slave->start();
    EXPECT_MESSAGE(filter, Eq("PING"), _, Eq(slavePid)).
      WillRepeatedly(InvokeWithoutArgs(this, &MasterAllocatorTest::slavePong));
    LOG(INFO) << "XXX slave = " << slavePid;
    LOG(INFO) << "XXX master = " << master;
    SlaveInfo slaveInfo;
    slaveInfo.set_hostname("test.invalid");
    slaveInfo.set_webui_hostname("test.invalid");
    slaveInfo.mutable_resources()->MergeFrom(
        Resources::parse("cpus:32;mem:1024"));
    RegisterSlaveMessage registerMessage;
    registerMessage.mutable_slave()->MergeFrom(slaveInfo);
    SlaveRegisteredMessage registeredMessage;
    trigger doneRegister;
    trigger slaveAdded;
    EXPECT_CALL(allocator, slaveAdded(_)).
      WillOnce(Trigger(&slaveAdded));
    slave->expectAndStore<SlaveRegisteredMessage>(
        masterPid, &registeredMessage, &doneRegister);
    slave->send(masterPid, registerMessage);
    WAIT_UNTIL(doneRegister);
    WAIT_UNTIL(slaveAdded);
    slaveId.MergeFrom(registeredMessage.slave_id());
    CHECK_GT(master->getActiveSlaves().size(), 0);
  }

  void registerFramework()
  {
    EXPECT_CALL(allocator, frameworkAdded(_)).Times(1);
    framework.reset(new FakeProtobufProcess);
    framework->setFilter(&filter);
    frameworkPid = framework->start();
    FrameworkInfo frameworkInfo;
    frameworkInfo.set_user("test-user");
    frameworkInfo.set_name("test-name");
    RegisterFrameworkMessage registerMessage;
    registerMessage.mutable_framework()->MergeFrom(frameworkInfo);
    trigger doneRegister;
    FrameworkRegisteredMessage registeredMessage;
    framework->expectAndStore<FrameworkRegisteredMessage>(
        masterPid, &registeredMessage, &doneRegister);
    framework->send(masterPid, registerMessage);
    WAIT_UNTIL(doneRegister);
    frameworkId.MergeFrom(registeredMessage.framework_id());
  }

  void unregisterFramework()
  {
    trigger allocatorGotRemoved;
    EXPECT_CALL(allocator, frameworkRemoved(_)).
      WillOnce(Trigger(&allocatorGotRemoved));
    UnregisterFrameworkMessage unregisterMessage;
    unregisterMessage.mutable_framework_id()->MergeFrom(frameworkId);
    trigger gotShutdown;
    slave->expectAndWait<ShutdownFrameworkMessage>(process::UPID(),
        &gotShutdown);
    framework->send(masterPid, unregisterMessage);
    WAIT_UNTIL(allocatorGotRemoved);
    WAIT_UNTIL(gotShutdown);
    process::terminate(frameworkPid);
    process::wait(frameworkPid);
    framework.reset(0);
    frameworkPid = PID<FakeProtobufProcess>();
  }

  void unregisterSlave()
  {
    EXPECT_CALL(allocator, slaveRemoved(_)).Times(1);
    UnregisterSlaveMessage unregisterMessage;
    unregisterMessage.mutable_slave_id()->MergeFrom(slaveId);
    slave->send(masterPid, unregisterMessage);
    process::terminate(slavePid);
    process::wait(slavePid);
    slave.reset(0);
    slavePid = PID<FakeProtobufProcess>();
  }

  void makeFullOffer(Offer* offer)
  {
    hashmap<master::Slave*, ResourceHints> offers;
    CHECK_GT(master->getActiveSlaves().size(), 0);
    master::Slave* slave = master->getActiveSlaves()[0];

    offers[slave].expectedResources = Resources::parse("cpus:32;mem:1024");
    offers[slave].minResources = Resources::parse("cpus:32;mem:1024");

    ResourceOffersMessage offerMessage;
    trigger gotResourceOffers;
    framework->expectAndStore(masterPid, &offerMessage, &gotResourceOffers);
    CHECK_GT(master->getActiveFrameworks().size(), 0);
    process::dispatch(masterPid, &Master::makeOffers,
        master->getActiveFrameworks()[0], offers);

    WAIT_UNTIL(gotResourceOffers);
    offer->MergeFrom(offerMessage.offers(0));
  }

  void launchTasks(const Offer& offer, const vector<TaskInfo>& tasks) {
    LaunchTasksMessage message;
    message.mutable_framework_id()->MergeFrom(offer.framework_id());
    message.mutable_offer_id()->MergeFrom(offer.id());
    message.mutable_filters();
    foreach (const TaskInfo& task, tasks) {
      message.add_tasks()->MergeFrom(task);
    }
    trigger runningTasks;
    slave->expectAndWait<RunTaskMessage>(process::UPID(), &runningTasks,
        tasks.size());
    framework->send(masterPid, message);
    WAIT_UNTIL(runningTasks);
  }

  static void addTask(const Offer& theOffer,
                      const Resources& resources,
                      const Resources& minResources,
                      std::string id,
                      vector<TaskInfo>* tasks) {
    TaskInfo task;
    task.set_name("");
    task.mutable_task_id()->set_value(id);
    task.mutable_slave_id()->MergeFrom(theOffer.slave_id());
    task.mutable_resources()->MergeFrom(resources);
    task.mutable_min_resources()->MergeFrom(minResources);
    tasks->push_back(task);
  }

  void sendTaskUpdate(const Offer& offer, const TaskInfo& task,
                      TaskState state) {
    StatusUpdateMessage message;
    StatusUpdate *update = message.mutable_update();
    update->mutable_framework_id()->MergeFrom(offer.framework_id());
    update->mutable_executor_id()->MergeFrom(
        DEFAULT_EXECUTOR_INFO.executor_id());
    update->mutable_slave_id()->MergeFrom(offer.slave_id());
    update->mutable_status()->mutable_task_id()->MergeFrom(
        task.task_id());
    update->mutable_status()->set_state(state);
    update->set_timestamp(process::Clock::now());
    update->set_uuid(UUID::random().toBytes());
    trigger frameworkGotUpdate;
    framework->expectAndWait<StatusUpdateMessage>(process::UPID(),
        &frameworkGotUpdate);
    slave->send(masterPid, message);
    WAIT_UNTIL(frameworkGotUpdate);
  }

  void sendExecutorExited(int status) {
    trigger gotRemoved;
    EXPECT_CALL(allocator,
                executorRemoved(_, _, EqProto(DEFAULT_EXECUTOR_INFO))).
      WillOnce(Trigger(&gotRemoved));
    ExitedExecutorMessage exitedMessage;
    exitedMessage.mutable_slave_id()->MergeFrom(slaveId);
    exitedMessage.mutable_framework_id()->MergeFrom(frameworkId);
    exitedMessage.mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
    exitedMessage.set_status(status);
    slave->send(masterPid, exitedMessage);
    WAIT_UNTIL(gotRemoved);
  }

  MockFilter filter;
  MockAllocator allocator;
  scoped_ptr<FakeProtobufProcess> slave;
  PID<FakeProtobufProcess> slavePid;
  scoped_ptr<FakeProtobufProcess> framework;
  PID<FakeProtobufProcess> frameworkPid;
  scoped_ptr<Master> master;
  PID<Master> masterPid;
  FrameworkID frameworkId;
  SlaveID slaveId;
};

TEST_F(MasterAllocatorTest, ReturnOffer) {
  detectMaster();
  vector<TaskInfo> empty;
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  trigger gotResourcesUnused;
  EXPECT_CALL(allocator,
              resourcesUnused(_, _, WithResourceHints(
                  Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024")))).
      WillOnce(Trigger(&gotResourcesUnused));
  launchTasks(theOffer, empty);
  WAIT_UNTIL(gotResourcesUnused);
}

TEST_F(MasterAllocatorTest, ReturnOfferMinOnly) {
  detectMaster();
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  trigger gotResourcesUnused;
  EXPECT_CALL(allocator,
              resourcesUnused(_, _, WithResourceHints(
                  Resources::parse("cpus:0;mem:0"),
                  Resources::parse("cpus:32;mem:1024")))).
    WillOnce(Trigger(&gotResourcesUnused));
  EXPECT_CALL(allocator, taskAdded(_));
  EXPECT_CALL(allocator, executorAdded(_, _, EqProto(DEFAULT_EXECUTOR_INFO)));
  vector<TaskInfo> tasks;
  addTask(theOffer, theOffer.resources(), Resources(), "taskId", &tasks);
  launchTasks(theOffer, tasks);
  WAIT_UNTIL(gotResourcesUnused);
}

TEST_F(MasterAllocatorTest, ReturnOfferAll) {
  detectMaster();
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  vector<TaskInfo> tasks;
  addTask(theOffer, theOffer.resources(), theOffer.min_resources(),
          "taskId", &tasks);
  launchTasks(theOffer, tasks);
}

TEST_F(MasterAllocatorTest, KillTask) {
  detectMaster();
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  vector<TaskInfo> tasks;
  EXPECT_CALL(allocator, taskAdded(_));
  EXPECT_CALL(allocator, executorAdded(_, _, EqProto(DEFAULT_EXECUTOR_INFO)));
  addTask(theOffer, theOffer.resources(), theOffer.min_resources(),
          "taskId", &tasks);
  launchTasks(theOffer, tasks);
  trigger gotReturned;
  EXPECT_CALL(allocator,
              resourcesRecovered(_, _, WithResourceHints(
                  Resources(theOffer.resources()),
                  Resources(theOffer.min_resources())))).
    WillOnce(Trigger(&gotReturned));
  EXPECT_CALL(allocator, taskRemoved(_));
  sendTaskUpdate(theOffer, tasks[0], TASK_KILLED);
  WAIT_UNTIL(gotReturned);
}

TEST_F(MasterAllocatorTest, UnregisterFramework) {
  detectMaster();
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  vector<TaskInfo> tasks;
  EXPECT_CALL(allocator, taskAdded(_));
  EXPECT_CALL(allocator, executorAdded(_, _, EqProto(DEFAULT_EXECUTOR_INFO)));
  addTask(theOffer, theOffer.resources(), theOffer.min_resources(),
          "taskId", &tasks);
  launchTasks(theOffer, tasks);
  EXPECT_CALL(allocator, taskRemoved(_));
  EXPECT_CALL(allocator, executorRemoved(_, _,
                                         EqProto(DEFAULT_EXECUTOR_INFO)));
  EXPECT_CALL(allocator, resourcesRecovered(_, _, _));
  unregisterFramework();
}

TEST_F(MasterAllocatorTest, UnregisterSlave) {
  detectMaster();
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  vector<TaskInfo> tasks;
  EXPECT_CALL(allocator, taskAdded(_));
  EXPECT_CALL(allocator, executorAdded(_, _, EqProto(DEFAULT_EXECUTOR_INFO)));
  addTask(theOffer, theOffer.resources(), theOffer.min_resources(),
          "taskId", &tasks);
  launchTasks(theOffer, tasks);
  EXPECT_CALL(allocator, taskRemoved(_));
  EXPECT_CALL(allocator, executorRemoved(_, _,
                                         EqProto(DEFAULT_EXECUTOR_INFO)));
  trigger statusUpdate;
  framework->expectAndWait<StatusUpdateMessage>(process::UPID(), &statusUpdate);
  trigger lostSlave;
  framework->expectAndWait<LostSlaveMessage>(process::UPID(), &lostSlave);
  unregisterSlave();
  WAIT_UNTIL(lostSlave);
  WAIT_UNTIL(statusUpdate);
}

TEST_F(MasterAllocatorTest, ExitedExecutor) {
  detectMaster();
  Offer theOffer;
  registerSlave();
  registerFramework();
  makeFullOffer(&theOffer);
  vector<TaskInfo> tasks;
  EXPECT_CALL(allocator, taskAdded(_));
  EXPECT_CALL(allocator, executorAdded(_, _, EqProto(DEFAULT_EXECUTOR_INFO)));
  addTask(theOffer, theOffer.resources(), theOffer.min_resources(),
          "taskId", &tasks);
  launchTasks(theOffer, tasks);
  EXPECT_CALL(allocator, taskRemoved(_));
  framework->expect<StatusUpdateMessage>();
  sendExecutorExited(1);
}

}}} // namespace mesos { namespace internal { namespace test {
