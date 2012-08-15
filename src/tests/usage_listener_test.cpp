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

#include "process/protobuf.hpp"
#include "process/process.hpp"
#include "process/dispatch.hpp"

#include "master/dominant_share_allocator.hpp"
#include "master/master.hpp"
#include "messages/messages.hpp"

#include "tests/utils.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::test;

using boost::scoped_ptr;

using mesos::internal::master::DominantShareAllocator;
using mesos::internal::master::Master;
using process::PID;
using process::UPID;

using testing::_;
using testing::DoAll;

class UsageListenerTestProcess
    : public ProtobufProcess<UsageListenerTestProcess> {
public:
  UsageListenerTestProcess(PID<Master> master_)
      : master(master_) {}

  void initialize() {
    install<UsageMessage>(&UsageListenerTestProcess::gotUsage);
    install<UsageListenerRegisteredMessage>(
        &UsageListenerTestProcess::registered);
    install<StatusUpdateMessage>(&UsageListenerTestProcess::gotUpdate,
                                 &StatusUpdateMessage::update);
    install<ExitedExecutorMessage>(&UsageListenerTestProcess::gotExit);
    RegisterUsageListenerMessage registerMessage;
    registerMessage.set_pid(self());
    send(master, registerMessage);
  }

  MOCK_METHOD1(gotUsage, void(const UsageMessage&));
  MOCK_METHOD1(gotUpdate, void(const StatusUpdate&));
  MOCK_METHOD1(gotExit, void(const ExitedExecutorMessage&));
  MOCK_METHOD1(registered, void(const UsageListenerRegisteredMessage&));

private:
  PID<Master> master;
};

class UsageListenerTest : public testing::Test {
protected:
  void SetUp() {
    EXPECT_CALL(allocator, initialize(_, _)).Times(1);
    EXPECT_CALL(allocator, gotUsage(_)).Times(testing::AtLeast(0));
    master.reset(new Master(&allocator));
    masterPid = process::spawn(master.get());
    listener.reset(new UsageListenerTestProcess(masterPid));
    trigger gotRegistered;
    EXPECT_CALL(*listener, registered(_)).
      WillOnce(Trigger(&gotRegistered));
    listenerPid = process::spawn(listener.get());
    WAIT_UNTIL(gotRegistered);
  }

  void TearDown() {
    process::terminate(masterPid);
    process::wait(masterPid);
    process::terminate(listenerPid);
    process::wait(listenerPid);
  }

  scoped_ptr<UsageListenerTestProcess> listener;
  process::UPID listenerPid;
  scoped_ptr<Master> master;
  PID<Master> masterPid;
  MockAllocator<DominantShareAllocator> allocator;
};

TEST_F(UsageListenerTest, ForwardUsage)
{
  UsageMessage message;
  message.mutable_framework_id()->set_value("dummy-framework");
  message.mutable_executor_id()->set_value("dummy-executor");
  message.set_timestamp(1000.0);
  UsageMessage receivedMessage;
  trigger gotUsage;
  EXPECT_CALL(*listener, gotUsage(_)).
    WillOnce(DoAll(Trigger(&gotUsage), testing::SaveArg<0>(&receivedMessage)));
  process::dispatch(masterPid, &Master::updateUsage, message);
  WAIT_UNTIL(gotUsage);
  EXPECT_EQ(message.DebugString(), receivedMessage.DebugString());
}

TEST_F(UsageListenerTest, ForwardStatus)
{
  StatusUpdate update;
  update.mutable_framework_id()->set_value("framework0");
  update.mutable_executor_id()->set_value("executor0");
  update.mutable_slave_id()->set_value("slave0");
  update.mutable_status()->mutable_task_id()->set_value("task0");
  update.mutable_status()->set_state(TASK_FINISHED);
  update.set_timestamp(42.0);
  update.set_uuid("1234567890");
  trigger gotUpdate;
  EXPECT_CALL(*listener, gotUpdate(_)).
    WillOnce(Trigger(&gotUpdate));
  process::dispatch(masterPid, &Master::statusUpdate, update, UPID());
  WAIT_UNTIL(gotUpdate);
}

TEST_F(UsageListenerTest, ForwardExecutorExit)
{
  ExitedExecutorMessage message;
  message.mutable_slave_id()->set_value("slave0");
  message.mutable_framework_id()->set_value("framework0");
  message.mutable_executor_id()->set_value("executor0");
  message.set_status(42);
  trigger gotExit;
  EXPECT_CALL(*listener, gotExit(_)).
    WillOnce(Trigger(&gotExit));
  process::dispatch(masterPid, &Master::exitedExecutor,
      message.slave_id(), message.framework_id(), message.executor_id(),
      message.status());
  WAIT_UNTIL(gotExit);
}


TEST_F(UsageListenerTest, HandleListenerDeath)
{
  process::terminate(listenerPid);
  process::wait(listenerPid);
  EXPECT_CALL(*listener, gotUsage(_)).Times(0);
  UsageMessage message;
  message.mutable_framework_id()->set_value("dummy-framework");
  message.mutable_executor_id()->set_value("dummy-executor");
  message.set_timestamp(1000.0);
  process::dispatch(masterPid, &Master::updateUsage, message);
}
