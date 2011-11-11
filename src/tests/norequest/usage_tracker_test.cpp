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

#include "norequest/usage_tracker.hpp"
#include "boost/smart_ptr/scoped_ptr.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::norequest;

const double kStartTime = 100.0;
const Resources kDefaultSlaveResources(Resources::parse("cpus:32;mem:4096"));

class UsageTrackerTest : public ::testing::Test {
protected:
  void SetUp() {
    tracker.reset(getUsageTracker());
    tracker->timerTick(kStartTime);
  }
  void TearDown() {
    tracker.reset(0);
  }

  // Utility functions for producing types
  FrameworkID framework(const std::string& id) {
    FrameworkID framework_id;
    framework_id.set_value(id);
    return framework_id;
  }

  ExecutorID executor(const std::string& id) {
    ExecutorID executor_id;
    executor_id.set_value(id);
    return executor_id;
  }

  SlaveID slave(const std::string& id) {
    SlaveID slave_id;
    slave_id.set_value(id);
    return slave_id;
  }

  void placeSimple(const std::string& frameworkId, const std::string& slaveId,
      Resources min, Option<Resources> estimated, int tasks = 1) {
    tracker->placeUsage(framework(frameworkId), executor("testExecutor"),
        slave(slaveId), min, estimated, tasks);
  }

  void removeTaskSimple(const std::string& frameworkId,
                        const std::string& slaveId,
                        int tasksLeft = 0) {
    tracker->placeUsage(framework(frameworkId), executor("testExecutor"),
        slave(slaveId), Resources(), Resources(), tasksLeft);
  }

  void setupSlave(const std::string& slaveId) {
    tracker->setCapacity(slave(slaveId), kDefaultSlaveResources);
  }

  UsageMessage getUpdate(const std::string& slaveId,
      const std::string& frameworkId,
      double time, const Resources& resources) {
    UsageMessage update;
    update.mutable_slave_id()->set_value(slaveId);
    update.mutable_framework_id()->set_value(frameworkId);
    update.mutable_executor_id()->set_value("testExecutor");
    update.mutable_resources()->MergeFrom(resources);
    update.set_timestamp(time);
    return update;
  }

  void recordUsageSimple(const std::string& slaveId,
      const std::string& frameworkId,
      double time, const Resources& resources) {
    time += kStartTime;
    tracker->recordUsage(getUpdate(slaveId, frameworkId, time, resources));
  }

  boost::scoped_ptr<UsageTracker> tracker;
};

TEST_F(UsageTrackerTest, PlaceUsageOnce) {
  placeSimple("testFramework", "testSlave",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  EXPECT_EQ(tracker->nextUsedForFramework(framework("testFramework")),
      Resources::parse("cpus:15.0;mem:512"));
  EXPECT_EQ(tracker->chargeForFramework(framework("testFramework")),
      Resources::parse("cpus:5.5;mem:1024"));
  EXPECT_EQ(tracker->usedForFramework(framework("testFramework")),
      Resources::parse(""));
  EXPECT_EQ(tracker->gaurenteedForFramework(framework("testFramework")),
      Resources::parse("cpus:5.5;mem:1024"));
}

TEST_F(UsageTrackerTest, ForgetPlaced) {
  placeSimple("testFramework", "testSlave",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  tracker->timerTick(kStartTime + 1.0);
  removeTaskSimple("testFramework", "testSlave");
  tracker->timerTick(kStartTime + 2.0);
  tracker->timerTick(kStartTime + 3.0);
  tracker->timerTick(kStartTime + 4.0);
  EXPECT_EQ(Resources::parse(""),
      tracker->gaurenteedForFramework(framework("testFramework")));
}

TEST_F(UsageTrackerTest, FreeBySlaveSimple) {
  setupSlave("testSlave");
  EXPECT_EQ(Resources::parse("cpus:32;mem:4096"),
            tracker->freeForSlave(slave("testSlave")));
  EXPECT_EQ(Resources::parse("cpus:32;mem:4096"),
            tracker->gaurenteedFreeForSlave(slave("testSlave")));
  placeSimple("testFramework", "testSlave",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  EXPECT_EQ(Resources::parse("cpus:26.5;mem:3072"),
            tracker->gaurenteedFreeForSlave(slave("testSlave")));
  EXPECT_EQ(Resources::parse("cpus:17.0;mem:3584"),
            tracker->freeForSlave(slave("testSlave")));
}

TEST_F(UsageTrackerTest, FreeBySlaveActualUsage) {
  setupSlave("testSlave");
  placeSimple("testFramework", "testSlave",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  recordUsageSimple("testSlave", "testFramework", 0.1,
      Resources::parse("cpus:31;mem:4000"));
  EXPECT_EQ(Resources::parse("cpus:1;mem:96"),
            tracker->freeForSlave(slave("testSlave")));
}

TEST_F(UsageTrackerTest, FreeBySlaveTwoFrameworks) {
  setupSlave("testSlave");
  placeSimple("one", "testSlave",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  placeSimple("two", "testSlave",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  EXPECT_EQ(Resources::parse("cpus:21;mem:2048"),
            tracker->gaurenteedFreeForSlave(slave("testSlave")));
  EXPECT_EQ(Resources::parse("cpus:2.0;mem:3072"),
            tracker->freeForSlave(slave("testSlave")));
  recordUsageSimple("testSlave", "one", 0.1,
      Resources::parse("cpus:31.0;mem:512"));
  EXPECT_EQ(Resources::parse("cpus:21;mem:2048"),
            tracker->gaurenteedFreeForSlave(slave("testSlave")));
  EXPECT_EQ(Resources::parse("cpus:-14;mem:3072"),
            tracker->freeForSlave(slave("testSlave")));
}

TEST_F(UsageTrackerTest, FrameworkAccountingTwoSlaves) {
  setupSlave("slave1");
  setupSlave("slave2");
  placeSimple("testFramework", "slave1",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  placeSimple("testFramework", "slave2",
      Resources::parse("cpus:5.5;mem:1024"),
      Resources::parse("cpus:15.0;mem:512"));
  EXPECT_EQ(Resources::parse("cpus:11;mem:2048"),
            tracker->chargeForFramework(framework("testFramework")));
  EXPECT_EQ(Resources::parse("cpus:30;mem:1024"),
            tracker->nextUsedForFramework(framework("testFramework")));
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("cpus:10;mem:768"));
  EXPECT_EQ(Resources::parse("cpus:15.5;mem:2048"),
            tracker->chargeForFramework(framework("testFramework")));
  EXPECT_EQ(Resources::parse("cpus:25;mem:1280"),
            tracker->nextUsedForFramework(framework("testFramework")));
  EXPECT_EQ(Resources::parse("cpus:10;mem:768"),
            tracker->nextUsedForExecutor(slave("slave1"),
                                         framework("testFramework"),
                                         executor("testExecutor")));
  EXPECT_EQ(Resources::parse("cpus:15;mem:512"),
            tracker->nextUsedForExecutor(slave("slave2"),
                                         framework("testFramework"),
                                         executor("testExecutor")));
  EXPECT_EQ(Resources::parse("cpus:5.5:;mem:1024"),
            tracker->gaurenteedForExecutor(slave("slave1"),
                                           framework("testFramework"),
                                           executor("testExecutor")));
}

TEST_F(UsageTrackerTest, RecordUsageIncomplete) {
  setupSlave("slave1");
  placeSimple("testFramework", "slave1", Resources(),
      Resources::parse("cpus:1.0;mem:1024"));
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("mem:768"));
  EXPECT_EQ(Resources::parse("cpus:1.0;mem:768"),
            tracker->nextUsedForFramework(framework("testFramework")));
}

// TODO(charles): do we need to make this prediction mode configurable?
TEST_F(UsageTrackerTest, ReduceTasksFreesUsage) {
  setupSlave("slave1");
  placeSimple("testFramework", "slave1", Resources(),
      Resources::parse("cpus:1.0;mem:1024"), 2);
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("cpus:3.0;mem:768"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 1);
  EXPECT_EQ(Resources::parse("cpus:1.5;mem:384"),
            tracker->nextUsedForFramework(framework("testFramework")));
  EXPECT_EQ(kDefaultSlaveResources - Resources::parse("cpus:1.5;mem:384"),
            tracker->freeForSlave(slave("slave1")));
}

TEST_F(UsageTrackerTest, ReduceTaskFreesUsageZeroBase) {
  setupSlave("slave1");
  placeSimple("testFramework", "slave1", Resources(),
      Resources::parse("cpus:1.0;mem:1024"), 2);
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("cpus:3.0;mem:768"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 0);
  recordUsageSimple("slave1", "testFramework", 0.2,
                    Resources::parse("cpus:0.0;mem:256"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 1);
  recordUsageSimple("slave1", "testFramework", 0.3,
                    Resources::parse("cpus:4.0;mem:512"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 0);
  EXPECT_EQ(Resources::parse("cpus:0.0;mem:256"),
            tracker->nextUsedForFramework(framework("testFramework")));
  EXPECT_EQ(kDefaultSlaveResources - Resources::parse("cpus:0.0;mem:256"),
            tracker->freeForSlave(slave("slave1")));

}

TEST_F(UsageTrackerTest, AddTasksUsesPredictionNoZero) {
  setupSlave("slave1");
  placeSimple("testFramework", "slave1", Resources(),
      Resources::parse("cpus:1.0;mem:1024"), 2);
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("cpus:3.0;mem:768"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 4);
  EXPECT_EQ(Resources::parse("cpus:6.0;mem:1536"),
            tracker->nextUsedForFramework(framework("testFramework")));
  EXPECT_EQ(kDefaultSlaveResources - Resources::parse("cpus:6.0;mem:1536"),
            tracker->freeForSlave(slave("slave1")));
}

TEST_F(UsageTrackerTest, AddTasksUsesPredictionAfterZero) {
  setupSlave("slave1");
  placeSimple("testFramework", "slave1", Resources(),
      Resources::parse("cpus:1.0;mem:1024"), 2);
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("cpus:3.0;mem:768"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 0);
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 4);
  EXPECT_EQ(Resources::parse("cpus:6.0;mem:1536"),
            tracker->nextUsedForFramework(framework("testFramework")));
  EXPECT_EQ(kDefaultSlaveResources - Resources::parse("cpus:6.0;mem:1536"),
            tracker->freeForSlave(slave("slave1")));
}

TEST_F(UsageTrackerTest, AddTasksUsesPredictionZeroBase) {
  setupSlave("slave1");
  placeSimple("testFramework", "slave1", Resources(),
      Resources::parse("cpus:1.0;mem:1024"), 2);
  recordUsageSimple("slave1", "testFramework", 0.1,
                    Resources::parse("cpus:3.0;mem:768"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 0);
  // TODO(charles): will we actually get cpus:0 or just no cpu field?
  recordUsageSimple("slave1", "testFramework", 0.2,
                    Resources::parse("cpus:0.0;mem:256"));
  placeSimple("testFramework", "slave1", Resources(),
      Option<Resources>::none(), 4);
  EXPECT_EQ(Resources::parse("cpus:6.0;mem:1280"),
            tracker->nextUsedForFramework(framework("testFramework")));
  EXPECT_EQ(kDefaultSlaveResources - Resources::parse("cpus:6.0;mem:1280"),
            tracker->freeForSlave(slave("slave1")));
}
