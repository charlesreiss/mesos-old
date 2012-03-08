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
#include <cstdlib>
#include <gmock/gmock.h>
#include "tests/fake/util.hpp"
#include "tests/utils.hpp"

#include "fake/fake_scheduler.hpp"

#include <mesos/scheduler.hpp>

#ifdef __GNUC__
#define NORETURN __attribute__((noreturn))
#else
#define NORETURN /* empty */
#endif

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::test;
using namespace mesos::internal::fake;

using std::vector;
using testing::_;
using testing::DoAll;
using testing::Return;
using testing::SaveArg;

MATCHER_P(EqId, value, "")
{
  return testing::Matcher<std::string>(value).Matches(arg.value());
}

inline void UNIMPLEMENTED() NORETURN;
inline void UNIMPLEMENTED() {
  LOG(FATAL) << "Unimplemented";
  std::abort();
}

class MockSchedulerDriver : public SchedulerDriver {
public:
  Status start() { UNIMPLEMENTED(); }
  Status stop(bool failover) { UNIMPLEMENTED(); }
  Status abort() { UNIMPLEMENTED(); }
  Status join() { UNIMPLEMENTED(); }
  Status run() { UNIMPLEMENTED(); }

  MOCK_METHOD1(requestResources, Status(const std::vector<ResourceRequest>&));
  MOCK_METHOD3(launchTasks, Status(const OfferID&,
                                   const std::vector<TaskDescription>&,
                                   const Filters&));
  MOCK_METHOD1(killTask, Status(const TaskID&));
  MOCK_METHOD0(reviveOffers, Status());
  MOCK_METHOD3(sendFrameworkMessage, Status(const SlaveID&, const ExecutorID&,
                                            const std::string&));
};

class FakeSchedulerTest : public testing::Test {
public:
  void SetUp()
  {
    process::Clock::pause();
  }

  void TearDown()
  {
    process::Clock::resume();
  }

  void registerScheduler(double delay = 0.0)
  {
    scheduler.reset(new FakeScheduler(Attributes(), &tracker));
    if (delay > 0.0) {
      scheduler->setStartTime(process::Clock::now() + delay);
    }
    scheduler->registered(&schedulerDriver, DEFAULT_FRAMEWORK_ID);
  }

  Offer createOffer(const std::string& id,
      const std::string& slave, ResourceHints resources)
  {
    Offer result;
    result.mutable_id()->set_value(id);
    result.mutable_framework_id()->MergeFrom(DEFAULT_FRAMEWORK_ID);
    result.mutable_slave_id()->set_value(slave);
    result.set_hostname("ignored-hostname");
    result.mutable_resources()->MergeFrom(resources.expectedResources);
    result.mutable_min_resources()->MergeFrom(resources.minResources);
    return result;
  }

  vector<Offer> singleOffer(const std::string& id,
                            const std::string& slaveId,
                            ResourceHints resources) {
    vector<Offer> offers;
    offers.push_back(createOffer(id, slaveId, resources));
    return offers;
  }

  void makeAndAcceptOffer(
      const std::string& taskId,
      const ResourceHints& offerResources,
      const ResourceHints& taskResources,
      MockFakeTask* mockTask)
  {
    if (mockTask) {
      EXPECT_CALL(*mockTask, getResourceRequest()).
        WillRepeatedly(Return(taskResources));
      scheduler->addTask(TASK_ID(taskId), mockTask);
    }

    vector<TaskDescription> result;
    EXPECT_CALL(schedulerDriver, launchTasks(EqId("offer0"), _, _)).
      WillOnce(DoAll(SaveArg<1>(&result), Return(OK)));
    scheduler->resourceOffers(&schedulerDriver,
        singleOffer("offer0", "slave0", offerResources));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].task_id(), TASK_ID(taskId));
    EXPECT_EQ(result[0].resources(), taskResources.expectedResources);
    EXPECT_EQ(result[0].min_resources(), taskResources.minResources);
    EXPECT_EQ(result[0].executor().executor_id().value(), taskId);

    if (mockTask) {
      EXPECT_EQ(mockTask,
          tracker.getTaskFor(DEFAULT_FRAMEWORK_ID,
                             result[0].executor().executor_id(),
                             TASK_ID(taskId)));
    }
  }

  void makeAndAcceptOfferDefault(const std::string& id, MockFakeTask* mockTask)
  {
    makeAndAcceptOffer(id,
        ResourceHints(Resources::parse("cpus:8.0;mem:4096"),
                      Resources::parse("cpus:8.0;mem:4096")),
        ResourceHints(Resources::parse("cpus:2.0;mem:2048"),
                      Resources::parse("cpus:3.0;mem:1024")),
        mockTask);
  }

  void rejectOffer(const std::string& id,
                   const ResourceHints& offerResources)
  {
    vector<TaskDescription> result;
    EXPECT_CALL(schedulerDriver, launchTasks(EqId("offer0"), _, _)).
      WillOnce(DoAll(SaveArg<1>(&result), Return(OK)));
    scheduler->resourceOffers(&schedulerDriver,
        singleOffer("offer0", "slave0", offerResources));
    EXPECT_EQ(0, result.size());
  }

  void updateTask(const std::string& id, TaskState state) {
    TaskStatus status;
    status.mutable_task_id()->set_value(id);
    status.set_state(state);
    scheduler->statusUpdate(&schedulerDriver, status);
  }

protected:
  boost::scoped_ptr<FakeScheduler> scheduler;
  MockSchedulerDriver schedulerDriver;
  FakeTaskTracker tracker;
};

TEST_F(FakeSchedulerTest, NoTasks) {
  registerScheduler();

  vector<TaskDescription> result;
  EXPECT_CALL(schedulerDriver, launchTasks(EqId("offer0"), _, _)).
    WillOnce(DoAll(SaveArg<1>(&result), Return(OK)));
  scheduler->resourceOffers(&schedulerDriver,
      singleOffer("offer0", "slave0",
        ResourceHints(Resources::parse("cpus:8.0;mem:4096"),
                      Resources::parse("cpus:8.0;mem:4096"))));
  EXPECT_EQ(result.size(), 0);
}

TEST_F(FakeSchedulerTest, OneTask) {
  registerScheduler();
  MockFakeTask mockTask;
  makeAndAcceptOfferDefault("task0", &mockTask);
}

TEST_F(FakeSchedulerTest, CannotFitTask) {
  registerScheduler();
  MockFakeTask mockTask;
  EXPECT_CALL(mockTask, getResourceRequest()).
    WillRepeatedly(Return(
          ResourceHints(Resources::parse("cpus:9.0"), Resources())));
  scheduler->addTask(TASK_ID("task0"), &mockTask);
  vector<TaskDescription> result;
  EXPECT_CALL(schedulerDriver, launchTasks(EqId("offer0"), _, _)).
    WillOnce(DoAll(SaveArg<1>(&result), Return(OK)));
  scheduler->resourceOffers(&schedulerDriver,
      singleOffer("offer0", "slave0",
        ResourceHints(Resources::parse("cpus:8.0;mem:4096"),
                      Resources::parse("cpus:8.0;mem:4096"))));
  ASSERT_EQ(result.size(), 0);
}

TEST_F(FakeSchedulerTest, FinishTask) {
  registerScheduler();
  MockFakeTask mockTask;
  makeAndAcceptOfferDefault("task0", &mockTask);
  updateTask("task0", TASK_FINISHED);

  EXPECT_EQ(1, scheduler->count(TASK_FINISHED));
}

TEST_F(FakeSchedulerTest, RespawnTask) {
  registerScheduler();
  MockFakeTask mockTask;
  makeAndAcceptOfferDefault("task0", &mockTask);
  EXPECT_CALL(schedulerDriver, reviveOffers()).
    WillOnce(Return(OK));
  updateTask("task0", TASK_LOST);
  makeAndAcceptOfferDefault("task0", 0);

  EXPECT_EQ(1, scheduler->count(TASK_LOST));
}

TEST_F(FakeSchedulerTest, TwoTasksDontFit) {
  registerScheduler();
  MockFakeTask mockTask0, mockTask1;
  EXPECT_CALL(mockTask0, getResourceRequest()).
    WillRepeatedly(Return(
          ResourceHints(Resources::parse("cpus:9.0"), Resources())));
  scheduler->addTask(TASK_ID("task0"), &mockTask0);
  makeAndAcceptOfferDefault("task1", &mockTask1);
  // now accept task0 with a bigger offer
  makeAndAcceptOffer("task0",
        ResourceHints(Resources::parse("cpus:9.0;mem:4096"), Resources()),
        ResourceHints(Resources::parse("cpus:9.0"), Resources()), 0);
}

TEST_F(FakeSchedulerTest, TwoTasksLowestFirst) {
  registerScheduler();
  MockFakeTask mockTask0, mockTask1;
  EXPECT_CALL(mockTask1, getResourceRequest()).
    WillRepeatedly(Return(
          ResourceHints(Resources::parse("cpus:7.0"), Resources())));
  scheduler->addTask(TASK_ID("task1"), &mockTask1);
  makeAndAcceptOfferDefault("task0", &mockTask0);
}

TEST_F(FakeSchedulerTest, DelayStart) {
  registerScheduler(10.0);
  MockFakeTask mockTask;
  EXPECT_CALL(mockTask, getResourceRequest()).
    WillRepeatedly(Return(
          ResourceHints(Resources::parse("cpus:1.0"), Resources())));
  scheduler->addTask(TASK_ID("task1"), &mockTask);
  process::Clock::settle();
  rejectOffer("offer0", ResourceHints(Resources::parse("cpus:8.0;mem:1024"),
                                      Resources::parse("cpus:8.0;mem:1024")));
  process::Clock::advance(9.5);
  process::Clock::settle();
  rejectOffer("offer1", ResourceHints(Resources::parse("cpus:8.0;mem:1024"),
                                      Resources::parse("cpus:8.0;mem:1024")));
  trigger gotRevive;
  EXPECT_CALL(schedulerDriver, reviveOffers()).
    WillOnce(testing::DoAll(Trigger(&gotRevive), Return(OK)));
  process::Clock::advance(0.5);
  WAIT_UNTIL(gotRevive);
  makeAndAcceptOffer("task1",
                     ResourceHints(Resources::parse("cpus:8.0;mem:1024"),
                                   Resources::parse("cpus:8.0;mem:1024")),
                     ResourceHints(Resources::parse("cpus:1.0"), Resources()),
                     0);
}
