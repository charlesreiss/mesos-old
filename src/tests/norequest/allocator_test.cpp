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

#include <vector>
#include <set>

#include <process/process.hpp>

#include "tests/utils.hpp"
#include "common/resources.hpp"
#include "norequest/allocator.hpp"
#include "norequest/usage_tracker.hpp"
#include "boost/smart_ptr/scoped_ptr.hpp"
#include "boost/ptr_container/ptr_vector.hpp"

using testing::_;
using testing::Eq;
using testing::Return;
using testing::Invoke;
using testing::AnyNumber;

using std::vector;

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::master;
using namespace mesos::internal::norequest;

template <class T>
inline std::ostream&
operator<<(std::ostream& out, const Option<T>& o) {
  if (o.isNone()) {
    return out << "None[" << typeid(T).name() << "]";
  } else {
    return out << "Some[" << o.get() << "]";
  }
}

MATCHER_P(EqOption, value, "") {
  if (arg.isSome()) {
    return value.isSome() && arg.get() == value.get();
  } else {
    return value.isNone();
  }
}

MATCHER_P(EqProto, value, "") {
  return value.SerializeAsString() == arg.SerializeAsString();
}

class MockUsageTracker : public UsageTracker {
public:
  MOCK_METHOD1(recordUsage, void(const UsageMessage&));
  MOCK_METHOD6(placeUsage, void(const FrameworkID&, const ExecutorID&,
                                const SlaveID&, const Resources&,
                                const Option<Resources>&,
                                int));
  MOCK_METHOD4(forgetExecutor, void(const FrameworkID&, const ExecutorID&,
                                    const SlaveID&, bool));
  MOCK_METHOD2(setCapacity, void(const SlaveID&, const Resources&));
  MOCK_METHOD1(timerTick, void(double));
  MOCK_CONST_METHOD1(chargeForFramework, Resources(const FrameworkID&));
  MOCK_CONST_METHOD1(gaurenteedForFramework, Resources(const FrameworkID&));
  MOCK_CONST_METHOD1(nextUsedForFramework, Resources(const FrameworkID&));
  MOCK_CONST_METHOD1(usedForFramework, Resources(const FrameworkID&));
  MOCK_CONST_METHOD1(freeForSlave, Resources(const SlaveID&));
  MOCK_CONST_METHOD1(gaurenteedFreeForSlave, Resources(const SlaveID&));
  MOCK_CONST_METHOD3(nextUsedForExecutor, Resources(const SlaveID&,
                                                    const FrameworkID&,
                                                    const ExecutorID&));
  MOCK_CONST_METHOD3(gaurenteedForExecutor, Resources(const SlaveID&,
                                                      const FrameworkID&,
                                                      const ExecutorID&));
};

class MockAllocatorMasterInterface : public AllocatorMasterInterface {
public:
  MOCK_METHOD1(getSlave, Slave*(const SlaveID&));
  MOCK_METHOD1(getFramework, Framework*(const FrameworkID&));
  MOCK_CONST_METHOD0(getActiveFrameworks, std::vector<Framework*>());
  MOCK_CONST_METHOD0(getActiveSlaves, std::vector<Slave*>());
  MOCK_METHOD2(makeOffers, void (Framework*,
                                 const hashmap<Slave*, ResourceHints>&));
};

MATCHER_P(WithId, idString, "") {
  return arg->id.value() == idString;
}

MATCHER_P(EqId, idString, "") {
  return arg.value() == idString;
}

MATCHER_P3(WithOffer, slave, resources, minResources, "") {
  if (arg.count(slave) > 0) {
    ResourceHints offerResources = arg.find(slave)->second;
    return offerResources.expectedResources == resources &&
           offerResources.minResources == minResources;
  } else {
    return false;
  }
}

class NoRequestAllocatorTest : public ::testing::Test {
protected:
  void SetUp()
  {
    process::Clock::pause();
    allocator.reset(new NoRequestAllocator(&master, &tracker));
    ON_CALL(master, getActiveFrameworks()).
      WillByDefault(Invoke(this, &NoRequestAllocatorTest::getFrameworkList));
    EXPECT_CALL(master, getActiveFrameworks()).
      Times(AnyNumber());
    ON_CALL(master, getActiveSlaves()).
      WillByDefault(Invoke(this, &NoRequestAllocatorTest::getSlaveList));
    EXPECT_CALL(master, getActiveSlaves()).
      Times(AnyNumber());
    ON_CALL(tracker, nextUsedForExecutor(_, _, _)).
      WillByDefault(Return(Resources()));
    EXPECT_CALL(tracker, nextUsedForExecutor(_, _, _)).
      Times(AnyNumber());
    ON_CALL(tracker, gaurenteedForExecutor(_, _, _)).
      WillByDefault(Return(Resources()));
    EXPECT_CALL(tracker, gaurenteedForExecutor(_, _, _)).
      Times(AnyNumber());
    // Use dummy resource 'default', so it's easy to see when the default 0
    // value is used.
    testing::DefaultValue<Resources>::Set(Resources::parse("default:0"));
  }

  void TearDown()
  {
    foreach (Task* task, tasks) {
      delete task;
    }
    process::Clock::resume();
  }

  vector<Framework*> getFrameworkList()
  {
    LOG(INFO) << "getFrameworkList; size = " << frameworks.size();
    if (frameworks.size() > 0) {
      return vector<Framework*>(frameworks.c_array(),
                                frameworks.c_array() + frameworks.size());
    } else {
      return vector<Framework*>();
    }
  }

  vector<Slave*> getSlaveList() {
    LOG(INFO) << "getSlaveList";
    if (slaves.size() > 0) {
      return vector<Slave*>(slaves.c_array(),
                            slaves.c_array() + slaves.size());
    } else {
      return vector<Slave*>();
    }
  }

  void makeFramework(const std::string& name) {
    FrameworkInfo info;
    FrameworkID id;
    id.set_value(name);
    frameworks.push_back(new Framework(info, id, UPID(), 0.0));
  }

  void makeAndAddFramework(const std::string& name) {
    makeFramework(name);
    allocator->frameworkAdded(&frameworks.back());
  }

  void setSlaveFree(const std::string& name,
                    const Resources& free,
                    const Resources& gaurenteed) {
    EXPECT_CALL(tracker, freeForSlave(EqId(name))).
      WillRepeatedly(Return(free));
    EXPECT_CALL(tracker, gaurenteedFreeForSlave(EqId(name))).
      WillRepeatedly(Return(gaurenteed));
  }

  void setSlaveOffered(Slave *slave, const Resources& normal,
                      const Resources& min) {
    slave->resourcesOffered = ResourceHints(normal, min);
    if (normal == Resources() && min == Resources()) {
      slave->offers.clear();
    } else {
      slave->offers.insert(0); // XXX hack
    }
  }

  void makeSlave(const std::string& name, const Resources& resources) {
    SlaveInfo info;
    info.mutable_resources()->MergeFrom(resources);
    SlaveID id;
    id.set_value(name);
    slaves.push_back(new Slave(info, id, UPID(), 0.0));
    slaves.back().active = true;
    setSlaveFree(name, resources, resources);
    setSlaveOffered(&slaves.back(), Resources(), Resources());
    EXPECT_CALL(master, getSlave(id)).
      WillRepeatedly(Return(&slaves.back()));
    EXPECT_CALL(tracker, setCapacity(slaves.back().id, resources))
      .Times(1);
  }

  void makeAndAddSlave(const std::string& name, const Resources& resources) {
    makeSlave(name, resources);
    allocator->slaveAdded(&slaves.back());
  }

  void expectOffer(Framework* framework, Slave* slave,
                   const Resources& resources, const Resources& minResources) {
    EXPECT_CALL(master, makeOffers(framework,
                                   WithOffer(slave, resources, minResources))).
      Times(1);
  }

  void returnOffer(Framework* framework, Slave* slave,
                   const Resources& resources, const Resources& minResources) {
    allocator->resourcesUnused(framework->id, slave->id,
        ResourceHints(resources, minResources));
  }

  void runUnequalFrameworksOneSlave() {
    EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework0"))).
      WillRepeatedly(Return(Resources::parse("cpus:5")));
    EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework1"))).
      WillRepeatedly(Return(Resources::parse("cpus:1")));
    makeAndAddFramework("framework0");
    makeAndAddFramework("framework1");
    makeSlave("slave0", Resources::parse("cpus:32;mem:1024"));
    expectOffer(&frameworks[1], &slaves[0],
                Resources::parse("cpus:32;mem:1024"),
                Resources::parse("cpus:32;mem:1024"));
    allocator->slaveAdded(&slaves[0]);
  }

  void initTwoFrameworksOneSlave() {
    allocator->stopMakingOffers();
    makeAndAddFramework("framework0");
    makeAndAddFramework("framework1");
    makeAndAddSlave("slave0", Resources::parse("cpus:32;mem:1024"));
    allocator->startMakingOffers();
  }

  void runAllRefuserTwoFrameworks() {
    initTwoFrameworksOneSlave();
    EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework0"))).
      WillRepeatedly(Return(Resources::parse("")));
    EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework1"))).
      WillRepeatedly(Return(Resources::parse("")));
    expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));
    EXPECT_CALL(tracker, timerTick(Eq(process::Clock::now())))
        .Times(1);
    allocator->timerTick();

    LOG(INFO) << "refusing 0 (1st time)";

    expectOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));
    returnOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));

    LOG(INFO) << "refusing 1 (1st time)";

    expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));
    returnOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));

    LOG(INFO) << "refusing 0 (2nd time)";

    expectOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));
    returnOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));

    LOG(INFO) << "refusing 1 (2nd time)";
    EXPECT_CALL(master, makeOffers(_, _)).Times(0);
    returnOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:32;mem:1024"),
                  Resources::parse("cpus:32;mem:1024"));
  }

  void expectPlaceUsage(const std::string& frameworkId,
                        const std::string& slaveId,
                        Option<Resources> prediction,
                        const Resources& minResources,
                        int numTasks,
                        Resources priorEstimate = Resources(),
                        Resources priorMin = Resources()) {
    // TODO(charles): We aren't actually checking the minResources!
    EXPECT_CALL(tracker, placeUsage(EqId(frameworkId),
                                    Eq(DEFAULT_EXECUTOR_ID),
                                    EqId(slaveId), Eq(minResources),
                                    EqOption(prediction), numTasks));
    EXPECT_CALL(tracker, gaurenteedForExecutor(EqId(slaveId),
                                               EqId(frameworkId),
                                               Eq(DEFAULT_EXECUTOR_ID))).
      Times(testing::AtMost(1));
    EXPECT_CALL(tracker, nextUsedForExecutor(EqId(slaveId),
                                             EqId(frameworkId),
                                             Eq(DEFAULT_EXECUTOR_ID))).
      Times(testing::AtMost(1));
  }

  Task* addTask(const std::string& taskId,
               Framework* framework,
               Slave* slave,
               const Resources& taskResources,
               const Resources& minTaskResources = Resources()) {
    Task *task = new Task;
    task->set_name("RUNNING TASK");
    task->mutable_task_id()->set_value(taskId);
    task->mutable_framework_id()->MergeFrom(framework->id);
    task->mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
    task->mutable_slave_id()->MergeFrom(slave->id);
    task->set_state(TASK_STARTING);
    task->mutable_resources()->MergeFrom(taskResources);
    task->mutable_min_resources()->MergeFrom(minTaskResources);
    tasks.insert(task);
    slave->addTask(task);
    allocator->taskAdded(task);
    return task;
  }

  void addExecutor(Framework* framework, Slave* slave,
                   const ExecutorInfo& info) {
    slave->executors[framework->id][info.executor_id()] = info;
    allocator->executorAdded(framework->id, slave->id,  info);
  }

  void removeTask(Task* task, Slave* slave) {
    slave->removeTask(task);
    allocator->taskRemoved(task);
    tasks.erase(task);
    delete task;
  }

  std::set<Task*> tasks;
  boost::ptr_vector<Slave> slaves;
  boost::ptr_vector<Framework> frameworks;
  boost::scoped_ptr<NoRequestAllocator> allocator;
  MockAllocatorMasterInterface master;
  MockUsageTracker tracker;
};

TEST_F(NoRequestAllocatorTest, AddFrameworkOffers) {
  makeAndAddSlave("slave0", Resources::parse("cpus:32;mem:1024"));
  makeFramework("framework0");
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  allocator->frameworkAdded(&frameworks[0]);
}

TEST_F(NoRequestAllocatorTest, TimerTick) {
  process::Clock::pause();
  EXPECT_CALL(tracker, timerTick(Eq(process::Clock::now())))
    .Times(1);
  allocator->timerTick();
}

TEST_F(NoRequestAllocatorTest, TwoFrameworkOffers) {
  runUnequalFrameworksOneSlave();
}

TEST_F(NoRequestAllocatorTest, ReOfferAfterRefuser) {
  runUnequalFrameworksOneSlave();
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  returnOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
}

TEST_F(NoRequestAllocatorTest, NoReOfferLoop) {
  runAllRefuserTwoFrameworks();
}

TEST_F(NoRequestAllocatorTest, ClearAllRefusersOnTick) {
  runAllRefuserTwoFrameworks();
  expectOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  EXPECT_CALL(tracker, timerTick(Eq(process::Clock::now()))).Times(1);
  allocator->timerTick();
}

TEST_F(NoRequestAllocatorTest, RefuserCountOnDeadFramework) {
  runAllRefuserTwoFrameworks();
  allocator->frameworkRemoved(&frameworks[1]);
  expectOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  allocator->offersRevived(&frameworks[0]);
}

TEST_F(NoRequestAllocatorTest, ReserveWhilePending) {
  runUnequalFrameworksOneSlave();
  setSlaveOffered(&slaves[0],
      Resources::parse("cpus:32;mem:1024"),
      Resources::parse("cpus:32;mem:1024"));
  makeSlave("slave1", Resources::parse("cpus:32;mem:1024"));
  expectOffer(&frameworks[1], &slaves[1],
              Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  allocator->slaveAdded(&slaves[1]);
}

TEST_F(NoRequestAllocatorTest, InitTwoFrameworksAndOneSlave) {
  initTwoFrameworksOneSlave();
}

TEST_F(NoRequestAllocatorTest, TaskAddedCallsPlaceUsage) {
  initTwoFrameworksOneSlave();
  expectPlaceUsage("framework1", "slave0",
      Option<Resources>(Resources::parse("cpus:24;mem:768")),
      Resources(), 1);
  addTask("task-framework1-1", &frameworks[1], &slaves[0],
          Resources::parse("cpus:24;mem:768"));
}

TEST_F(NoRequestAllocatorTest, TaskAddedTwicePlaceUsageCountTwo) {
  initTwoFrameworksOneSlave();
  expectPlaceUsage("framework1", "slave0",
                   Option<Resources>(Resources::parse("cpus:24;mem:768")),
                   Resources(), 1);
  addTask("task-framework1-1", &frameworks[1], &slaves[0],
          Resources::parse("cpus:24;mem:768"));
  expectPlaceUsage("framework1", "slave0",
                   Option<Resources>(Resources::parse("cpus:28;mem:800")),
                   Resources(), 2);
  EXPECT_CALL(tracker, nextUsedForExecutor(EqId("slave0"),
                                           EqId("framework1"), _)).
    WillRepeatedly(Return(Resources::parse("cpus:24;mem:768")));
  addTask("task-framework1-2", &frameworks[1], &slaves[0],
          Resources::parse("cpus:4;mem:32"));
}

TEST_F(NoRequestAllocatorTest, TaskRemovedCallsPlaceUsageAndOffers) {
  initTwoFrameworksOneSlave();
  expectPlaceUsage("framework1", "slave0",
                   Option<Resources>(Resources::parse("cpus:24;mem:768")),
                   Resources(), 1);
  Task* task = addTask("task-framework1-1", &frameworks[1], &slaves[0],
                       Resources::parse("cpus:24;mem:768"));
  task->set_state(TASK_FINISHED);
  // TODO(charles): how does this interact with executorRemoved?
  setSlaveFree("slave0", Resources::parse("cpus:32;mem:1024"),
                         Resources::parse("cpus:16;mem:1024"));
  expectPlaceUsage("framework1", "slave0", Resources(), Resources(), 0);
  expectOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:16;mem:1024"));
  removeTask(task, &slaves[0]);
}

TEST_F(NoRequestAllocatorTest, TaskPlacedHandlesMinUsage) {
  initTwoFrameworksOneSlave();
  expectPlaceUsage("framework1", "slave0", Option<Resources>(Resources()),
                   Resources::parse("cpus:8;mem:512"), 1);
  addTask("task-framework1-1", &frameworks[1], &slaves[0],
          Resources(), Resources::parse("cpus:8;mem:512"));
}

TEST_F(NoRequestAllocatorTest, ReOfferPartialAfterRefuser) {
  runUnequalFrameworksOneSlave();
  addTask("task1", &frameworks[1], &slaves[0],
          Resources::parse("cpus:24;mem:768"));
  EXPECT_CALL(tracker, freeForSlave(EqId("slave0"))).
    WillRepeatedly(Return(Resources::parse("cpus:8;mem:256")));
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:8;mem:256"),
              Resources::parse("cpus:32;mem:1024"));
  returnOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:8;mem:256"),
              Resources::parse("cpus:32;mem:1024"));
}

// Disabled due to autorevive after timerTick hack.
TEST_F(NoRequestAllocatorTest, DISABLED_ReOfferAfterRevive) {
  makeAndAddSlave("slave0", Resources::parse("cpus:32;mem:1024"));

  makeFramework("framework0");
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  allocator->frameworkAdded(&frameworks[0]);
  returnOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));

  EXPECT_CALL(tracker, timerTick(Eq(process::Clock::now()))).Times(1);
  allocator->timerTick();  // no offers yet

  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  allocator->offersRevived(&frameworks[0]);
}

TEST_F(NoRequestAllocatorTest, GaurenteedOffer) {
  EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework0"))).
    WillRepeatedly(Return(Resources()));
  EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework1"))).
    WillRepeatedly(Return(Resources()));
  makeAndAddFramework("framework0");
  makeAndAddFramework("framework1");
  makeSlave("slave0", Resources::parse("cpus:10;mem:5000"));
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:10;mem:5000"),
              Resources::parse("cpus:10;mem:5000"));
  allocator->slaveAdded(&slaves[0]);
  setSlaveFree("slave0", Resources::parse("cpus:0.5;mem:500"),
               Resources::parse("cpus:6.0;mem:4500"));
  EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework0"))).
    WillRepeatedly(Return(Resources::parse("cpus:9.5;mem:4500")));
  expectOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:0.5;mem:500"),
               Resources::parse("cpus:6.0;mem:4500"));
  returnOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:0.5;mem:500"),
              Resources::parse("cpus:6.0;mem:4500"));
}

TEST_F(NoRequestAllocatorTest, TwoFrameworksTimerTickOffer) {
  initTwoFrameworksOneSlave();
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  EXPECT_CALL(tracker, timerTick(Eq(process::Clock::now()))).Times(1);
  allocator->timerTick();
}

TEST_F(NoRequestAllocatorTest, ResourcesUnusedHandlesMinRes) {
  initTwoFrameworksOneSlave();
  expectOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:32;mem:1024"));
  EXPECT_CALL(tracker, timerTick(Eq(process::Clock::now()))).Times(1);
  allocator->timerTick();
  setSlaveOffered(&slaves[0], Resources::parse(""),
                  Resources::parse("cpus:32;mem:1024"));
  expectOffer(&frameworks[1], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse("cpus:0;mem:0"));
  returnOffer(&frameworks[0], &slaves[0], Resources::parse("cpus:32;mem:1024"),
              Resources::parse(""));
}

TEST_F(NoRequestAllocatorTest, ExecutorAddedDoesPlaceUsageNoTasks) {
  initTwoFrameworksOneSlave();
  ExecutorInfo executorInfo;
  executorInfo.mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
  executorInfo.set_uri("ignored-uri");
  executorInfo.mutable_resources()->MergeFrom(
      Resources::parse("cpus:0.5;mem:256"));
  executorInfo.mutable_min_resources()->MergeFrom(
      Resources::parse("cpus:0.1;mem:512"));
  expectPlaceUsage("framework0", "slave0",
      Resources::parse("cpus:0.5;mem:256"),
      Resources::parse("cpus:0.1;mem:512"), 0);
  allocator->executorAdded(frameworks[0].id, slaves[0].id, executorInfo);
}

TEST_F(NoRequestAllocatorTest, ExecutorAddedDoesPlaceUsageOneTask) {
  initTwoFrameworksOneSlave();
  expectPlaceUsage("framework0", "slave0",
      Resources::parse("cpus:24;mem:100"),
      Resources::parse("cpus:0.5;mem:200"), 1);
  addTask("task-framework0-0", &frameworks[0], &slaves[0],
      Resources::parse("cpus:24;mem:100"),
      Resources::parse("cpus:0.5;mem:200"));
  ExecutorInfo executorInfo;
  executorInfo.mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
  executorInfo.set_uri("ignored-uri");
  executorInfo.mutable_resources()->MergeFrom(
      Resources::parse("cpus:0.5;mem:256"));
  executorInfo.mutable_min_resources()->MergeFrom(
      Resources::parse("cpus:0.1;mem:512"));
  expectPlaceUsage("framework0", "slave0",
      Resources::parse("cpus:24.5;mem:356"),
      Resources::parse("cpus:0.6;mem:712"), 1);
  ON_CALL(tracker, nextUsedForExecutor(EqId("slave0"),
                                       EqId("framework0"),
                                       Eq(DEFAULT_EXECUTOR_ID))).
    WillByDefault(Return(Resources::parse("cpus:24;mem:100")));
  ON_CALL(tracker, gaurenteedForExecutor(EqId("slave0"),
                                         EqId("framework0"),
                                         Eq(DEFAULT_EXECUTOR_ID))).
    WillByDefault(Return(Resources::parse("cpus:0.5;mem:200")));
  allocator->executorAdded(frameworks[0].id, slaves[0].id, executorInfo);
}

TEST_F(NoRequestAllocatorTest, ExecutorRemovedDoesForgetUsage) {
  initTwoFrameworksOneSlave();
  ExecutorInfo executorInfo;
  executorInfo.mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
  executorInfo.set_uri("ignored-uri");
  executorInfo.mutable_resources()->MergeFrom(
      Resources::parse("cpus:0.5;mem:256"));
  executorInfo.mutable_min_resources()->MergeFrom(
      Resources::parse("cpus:0.1;mem:512"));
  expectPlaceUsage("framework0", "slave0",
      Resources::parse("cpus:0.5;mem:256"),
      Resources::parse("cpus:0.1;mem:512"), 0);
  allocator->executorAdded(frameworks[0].id, slaves[0].id, executorInfo);
  EXPECT_CALL(tracker, forgetExecutor(frameworks[0].id, DEFAULT_EXECUTOR_ID,
                                      slaves[0].id, false));
  allocator->executorRemoved(frameworks[0].id, slaves[0].id, executorInfo);
}

TEST_F(NoRequestAllocatorTest, GotUsageForwards) {
  UsageMessage update;
  update.mutable_slave_id()->set_value("slave-id");
  update.mutable_framework_id()->set_value("framework-id");
  update.mutable_executor_id()->set_value("executor-id");
  update.mutable_resources()->MergeFrom(Resources::parse("cpus:44.0"));
  update.set_timestamp(51.0);
  update.set_duration(2.0);
  EXPECT_CALL(tracker, recordUsage(EqProto(update)));
  allocator->gotUsage(update);
}

TEST_F(NoRequestAllocatorTest, ReOfferAfterUsage) {
  process::Clock::pause();
  initTwoFrameworksOneSlave();
  allocator->stopMakingOffers();
  expectPlaceUsage("framework1", "slave0",
                   Option<Resources>(Resources::parse("cpus:24;mem:768")),
                   Resources(), 1);
  addTask("task-framework1-1", &frameworks[1], &slaves[0],
          Resources::parse("cpus:24;mem:768"));
  allocator->startMakingOffers();
  UsageMessage update;
  update.mutable_slave_id()->set_value("slave0");
  update.mutable_framework_id()->set_value("framework1");
  update.mutable_executor_id()->MergeFrom(DEFAULT_EXECUTOR_ID);
  update.mutable_resources()->MergeFrom(Resources::parse("cpus:12;mem:384"));
  update.set_timestamp(process::Clock::now());
  update.set_duration(1.0);
  EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework0"))).
    WillRepeatedly(Return(Resources::parse("")));
  EXPECT_CALL(tracker, nextUsedForFramework(EqId("framework1"))).
    WillRepeatedly(Return(Resources::parse("cpus:5.0;mem:128")));
  setSlaveFree("slave0",
               Resources::parse("cpus:27;mem:896"),
               Resources::parse("cpus:32;mem:1024"));
  expectOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:27.0;mem:896"),
              Resources::parse("cpus:32.0;mem:1024"));
  EXPECT_CALL(tracker, recordUsage(EqProto(update)));
  allocator->gotUsage(update);
  process::Clock::resume();
}

TEST_F(NoRequestAllocatorTest, DelayOfferOnPendingOffer) {
  process::Clock::pause();
  initTwoFrameworksOneSlave();
  setSlaveFree("slave0", Resources::parse("cpus:2;mem:1"),
               Resources::parse("cpus:2;mem:1"));
  setSlaveOffered(&slaves[0], Resources::parse("cpus:1;mem:2"),
                  Resources::parse("cpus:1;mem:2"));
  // No offer in response to this.
  returnOffer(&frameworks[1], &slaves[0],
              Resources::parse("cpus:2;mem:1"),
              Resources::parse("cpus:2;mem:1"));
  setSlaveOffered(&slaves[0], Resources::parse(""), Resources::parse(""));
  setSlaveFree("slave0", Resources::parse("cpus:3;mem:3"), Resources::parse("cpus:3;mem:3"));
  expectOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:3;mem:3"),
              Resources::parse("cpus:3;mem:3"));
  returnOffer(&frameworks[1], &slaves[0],
              Resources::parse("cpus:1;mem:2"),
              Resources::parse("cpus:1;mem:2"));
}

TEST_F(NoRequestAllocatorTest, DelayOfferTillAccept) {
  process::Clock::pause();
  initTwoFrameworksOneSlave();
  setSlaveFree("slave0", Resources::parse("cpus:2;mem:1"), Resources::parse("cpus:2;mem:1"));
  setSlaveOffered(&slaves[0], Resources::parse("cpus:1;mem:2"), Resources::parse("cpus:1;mem:2"));
  // No offer in response to this.
  returnOffer(&frameworks[1], &slaves[0],
              Resources::parse("cpus:2;mem:1"),
              Resources::parse("cpus:2;mem:1"));
  setSlaveFree("slave0", Resources::parse("cpus:2;mem:1"), Resources::parse("cpus:2;mem:1"));
  setSlaveOffered(&slaves[0], Resources::parse(""), Resources::parse(""));
  expectOffer(&frameworks[0], &slaves[0],
              Resources::parse("cpus:2;mem:1"),
              Resources::parse("cpus:2;mem:1"));
  expectPlaceUsage("framework0", "slave0",
      Option<Resources>(Resources::parse("cpus:2;mem:1")),
      Resources(), 1);
  addTask("task0", &frameworks[0], &slaves[0],
          Resources::parse("cpus:2;mem:1"));
}
