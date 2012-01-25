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

#include "fake/fake_scheduler.hpp"

#include <mesos/scheduler.hpp>

#ifdef __GNUC__
#define NORETURN __attribute__((noreturn))
#else
#define NORETURN /* empty */
#endif

using namespace mesos;
using namespace mesos::internal;
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

inline TaskID TASK_ID(const std::string& value) {
  TaskID result;
  result.set_value(value);
  return result;
}

class FakeSchedulerTest : public testing::Test {
public:
  void registerScheduler() {
    scheduler.reset(new FakeScheduler);
    scheduler->registered(&schedulerDriver, DEFAULT_FRAMEWORK_ID);
  }

  Offer createOffer(const std::string& id,
      const std::string& slave, ResourceHints resources) {
    Offer result;
    result.mutable_id()->set_value(id);
    result.mutable_framework_id()->MergeFrom(DEFAULT_FRAMEWORK_ID);
    result.mutable_slave_id()->set_value(slave);
    result.set_hostname("ignored-hostname");
    result.mutable_resources()->MergeFrom(resources.expectedResources);
    result.mutable_min_resources()->MergeFrom(resources.minResources);
    return result;
  }
protected:
  boost::scoped_ptr<FakeScheduler> scheduler;
  MockSchedulerDriver schedulerDriver;
};

TEST_F(FakeSchedulerTest, NoTasks) {
  registerScheduler();

  vector<Offer> offers;
  offers.push_back(createOffer("offer0", "slave0", ResourceHints(
          Resources::parse("cpu:8.0;mem:4096"),
          Resources::parse("cpu:8.0;mem:4096"))));
  vector<TaskDescription> result;
  EXPECT_CALL(schedulerDriver, launchTasks(EqId("offer0"), _, _)).
    WillOnce(DoAll(SaveArg<1>(&result), Return(OK)));
  scheduler->resourceOffers(&schedulerDriver, offers);
  EXPECT_EQ(result.size(), 0);
}

TEST_F(FakeSchedulerTest, OneTask) {
  registerScheduler();

  MockFakeTask mockTask;
  EXPECT_CALL(mockTask, getResourceRequest()).
    WillRepeatedly(Return(ResourceHints(
            Resources::parse("cpu:2.0;mem:2048"),
            Resources::parse("cpu:3.0;mem:1024"))));

  scheduler->addTask(TASK_ID("task0"), &mockTask);

  vector<Offer> offers;
  offers.push_back(createOffer("offer0", "slave0", ResourceHints(
          Resources::parse("cpu:8.0;mem:4096"),
          Resources::parse("cpu:8.0;mem:4096"))));
  vector<TaskDescription> result;
  EXPECT_CALL(schedulerDriver, launchTasks(EqId("offer0"), _, _)).
    WillOnce(DoAll(SaveArg<1>(&result), Return(OK)));
  scheduler->resourceOffers(&schedulerDriver, offers);
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].task_id(), TASK_ID("task0"));
  EXPECT_EQ(result[0].resources(), Resources::parse("cpu:2.0;mem:2048"));
  EXPECT_EQ(result[0].min_resources(), Resources::parse("cpu:3.0;mem:1024"));
  EXPECT_EQ(result[0].executor().executor_id().value(), "task0");
}
