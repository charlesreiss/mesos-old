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

#include "master/master.hpp"
#include "messages/messages.hpp"

#include "tests/utils.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::test;

using boost::scoped_ptr;

using mesos::internal::master::Master;
using process::PID;

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

    RegisterUsageListener registerMessage;
    registerMessage.set_pid(self());
    send(master, registerMessage);
  }

  MOCK_METHOD1(gotUsage, void(const UsageMessage&));
  MOCK_METHOD1(registered, void(const UsageListenerRegisteredMessage&));

private:
  PID<Master> master;
};

class UsageListenerTest : public testing::Test {
protected:
  void SetUp() {
    EXPECT_CALL(allocator, initialize(_)).Times(1);
    EXPECT_CALL(allocator, timerTick()).Times(testing::AtLeast(0));
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
  MockAllocator allocator;
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
