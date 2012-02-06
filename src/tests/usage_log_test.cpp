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

#include "usage_log/usage_log.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::test;

using namespace mesos::internal::usage_log;

using boost::scoped_ptr;
using testing::DoAll;
using testing::SaveArg;
using testing::_;

class MockUsageLogWriter : public UsageLogWriter {
public:
  MOCK_METHOD1(write, void(const UsageLogRecord&));

  MOCK_METHOD0(deleted, void());

  virtual ~MockUsageLogWriter() {
    deleted();
  }
};

static const double kInterval = 5.0;

class UsageRecorderTest : public testing::Test {
protected:
  void SetUp()
  {
    process::Clock::pause();
    process::filter(&mockFilter);
    master.setFilter(&mockFilter);
    masterPid = master.start();

    logWriter = new MockUsageLogWriter;
    EXPECT_CALL(*logWriter, deleted());
    recorder.reset(new UsageRecorder(logWriter, masterPid, kInterval));
    trigger gotRegister;
    master.expectAndWait<RegisterUsageListenerMessage>(
        recorderPid, &gotRegister);
    recorderPid = process::spawn(recorder.get());
    WAIT_UNTIL(gotRegister);
    UsageListenerRegisteredMessage message;
    master.send(recorderPid, message);
  }

  void TearDown()
  {
    // final record will be flushed.
    EXPECT_CALL(*logWriter, write(_));
    process::terminate(recorder.get());
    process::wait(recorder.get());
    recorder.reset(0);
    process::terminate(masterPid);
    process::wait(masterPid);
    process::filter(0);
    process::Clock::resume();
  }

  ~UsageRecorderTest()
  {
    process::filter(0);
  }

  void readRecord(UsageLogRecord* record, trigger *gotRecord)
  {
    EXPECT_CALL(*logWriter, write(_)).
      WillOnce(DoAll(SaveArg<0>(record), Trigger(gotRecord))).
      RetiresOnSaturation();
  }

  UsageMessage dummyUsage(const std::string& id, const Resources& resources,
                          double endTime, double duration)
  {
    UsageMessage record;
    record.mutable_slave_id()->set_value("slave-" + id);
    record.mutable_framework_id()->set_value("framework-" + id);
    record.mutable_executor_id()->set_value("executor-" + id);
    record.mutable_resources()->MergeFrom(resources);
    record.set_timestamp(endTime);
    record.set_duration(duration);
    return record;
  }

  void expectEmptyUsage(double advance)
  {
    LOG(INFO) << "Expect empty";
    UsageLogRecord record;
    trigger gotRecord;
    readRecord(&record, &gotRecord);
    process::Clock::advance(advance);
    WAIT_UNTIL(gotRecord);
    double baseTime = process::Clock::now() - 10.0;
    EXPECT_EQ(0, record.usage_size());
    EXPECT_EQ(0, record.update_size());
    EXPECT_FALSE(record.has_min_seen_timestamp());
    EXPECT_FALSE(record.has_max_seen_timestamp());
    EXPECT_DOUBLE_EQ(baseTime, record.min_expect_timestamp());
    EXPECT_DOUBLE_EQ(baseTime + kInterval, record.max_expect_timestamp());
  }

  void expectExactUsage(double advance, const UsageMessage& usage)
  {
    LOG(INFO) << "Expect UsageMessage";
    UsageLogRecord record;
    trigger gotRecord;
    readRecord(&record, &gotRecord);
    process::Clock::advance(advance);
    WAIT_UNTIL(gotRecord);
    double baseTime = process::Clock::now() - 2 * kInterval;
    ASSERT_EQ(1, record.usage_size());
    EXPECT_EQ(usage.DebugString(), record.usage(0).DebugString());
    EXPECT_EQ(0, record.update_size());
    EXPECT_TRUE(record.has_min_seen_timestamp());
    EXPECT_TRUE(record.has_max_seen_timestamp());
    EXPECT_DOUBLE_EQ(record.min_seen_timestamp(),
                     usage.timestamp() - usage.duration());
    EXPECT_DOUBLE_EQ(record.max_seen_timestamp(),
                     usage.timestamp());
    EXPECT_DOUBLE_EQ(baseTime, record.min_expect_timestamp());
    EXPECT_DOUBLE_EQ(baseTime + kInterval, record.max_expect_timestamp());
  }

  void testUsageTiming(double sendTime, double baseTime, double duration,
                       double recvTime, bool recvBoth = false) {
    double start = process::Clock::now();
    baseTime += start;
    process::Clock::advance(sendTime);
    process::Clock::settle();
    UsageMessage usage = dummyUsage("0", Resources(),
          baseTime + duration, duration);
    master.send(recorderPid, usage);
    double remainingTime = recvTime - sendTime;
    if (recvTime > kInterval * 2) {
      expectEmptyUsage(kInterval * 2 - sendTime);
      remainingTime = recvTime - kInterval * 2;
    }
    expectExactUsage(remainingTime, usage);
    if (recvBoth) {
      expectExactUsage(kInterval, usage);
    }
    expectEmptyUsage(kInterval);
  }

  MockFilter mockFilter;
  FakeProtobufProcess master;
  UPID masterPid;
  MockUsageLogWriter* logWriter;
  scoped_ptr<UsageRecorder> recorder;
  PID<UsageRecorder> recorderPid;
};

// These tests depend on the value of kInterval.
TEST_F(UsageRecorderTest, NoUsage)
{
  // process will wait till next interval to emit message, to catch straggling
  // usage and updates.
  expectEmptyUsage(10.0);
}

TEST_F(UsageRecorderTest, CopyUsageMessageEarly)
{
  testUsageTiming(4.0, 0.1, 3.8, 10.0);
}

TEST_F(UsageRecorderTest, CopyUsageMessageLate)
{
  testUsageTiming(6.0, 0.1, 3.8, 10.0);
}

TEST_F(UsageRecorderTest, CopyUsageMessageOverlapFirst) {
  // [2.0 -> 5.2]: 3% < 10% of this overlaps [5.0, 10.0], so it is only emitted
  // once.
  testUsageTiming(6.0, 2.0, 3.2, 10.0);
}

TEST_F(UsageRecorderTest, CopyUsageMessageOverlapBoth) {
  // [2.0 -> 5.4]: 11% > 10% of this overlaps [5.0, 10.0], so it gets emitted
  // for both.
  testUsageTiming(6.0, 2.0, 3.4, 10.0, true);
}

TEST_F(UsageRecorderTest, CopyUsageMessageOverlapSecond) {
  // [4.9 -> 9.9]: not enough overlaps [0, 5.0]
  testUsageTiming(8.0, 4.9, 5.0, 15.0);
}

TEST_F(UsageRecorderTest, RecordStatusUpdate) {
  ASSERT_TRUE(false);
}

TEST_F(UsageRecorderTest, RecordStatusUpdateLate) {
  ASSERT_TRUE(false);
}
