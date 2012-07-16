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

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <process/process.hpp>

#include "detector/detector.hpp"

#include "fake/fake_isolation_module.hpp"
#include "fake/fake_scheduler.hpp"

#include "master/dominant_share_allocator.hpp"
#include "master/master.hpp"

#include "slave/slave.hpp"

#include "tests/utils.hpp"
#include "tests/fake/util.hpp"


using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;
using namespace mesos::internal::test;

using boost::scoped_ptr;

using mesos::internal::master::Master;
using mesos::internal::slave::Slave;

using process::PID;

using std::make_pair;

using testing::_;
using testing::DoAll;
using testing::Invoke;
using testing::Return;
using testing::SaveArg;

static const double kTick = 1.0;

class MasterSlaveFakeTest : public testing::Test {
public:
  void startMasterAndSlave()
  {
    Configuration conf;
    conf.set("fake_interval", kTick);
    process::Clock::pause();
    ASSERT_TRUE(GTEST_IS_THREADSAFE);
    EXPECT_CALL(allocator, initialize(_));
    master.reset(new Master(&allocator));
    masterPid = process::spawn(master.get());

    module.reset(new FakeIsolationModule(tasks));
    slave.reset(new Slave("slave", Resources::parse("cpu:8.0;mem:4096"), conf,
                          true, module.get()));
    slavePid = process::spawn(slave.get());

    trigger gotSlave;
    EXPECT_CALL(allocator, slaveAdded(_, _, _)).
      WillOnce(DoAll(SaveArg<0>(&masterSlaveId), Trigger(&gotSlave)));
    detector.reset(new BasicMasterDetector(masterPid, slavePid, true));
    process::Clock::advance(kTick);
    WAIT_UNTIL(gotSlave);
  }

  void startScheduler() {
    if (!scheduler.get()) {
      scheduler.reset(new FakeScheduler(Attributes(), &tasks));
    }
    trigger gotFramework;
    driver.reset(
      new MesosSchedulerDriver(scheduler.get(), DEFAULT_FRAMEWORK_INFO_WITH_ID,
                               std::string(masterPid)));
    EXPECT_CALL(allocator, frameworkAdded(_, _)).
      WillOnce(DoAll(SaveArg<0>(&masterFrameworkId), Trigger(&gotFramework)));
    driver->start();
    WAIT_UNTIL(gotFramework);
  }

  void stopScheduler() {
    trigger lostFramework;
    EXPECT_CALL(allocator, frameworkDeactivated(_));
    EXPECT_CALL(allocator, frameworkRemoved(_)).
      WillOnce(Trigger(&lostFramework));
    driver->stop();
    driver->join();
    WAIT_UNTIL(lostFramework);
  }

  void stopMasterAndSlave() {
    trigger lostSlave;
    EXPECT_CALL(allocator, slaveRemoved(_)).
      WillOnce(Trigger(&lostSlave));
    process::terminate(slavePid);
    process::wait(slavePid);
    slave.reset(0);
    WAIT_UNTIL(lostSlave);
    process::terminate(masterPid);
    process::wait(masterPid);
    master.reset(0);
    process::Clock::resume();
  }

  void addTask(const std::string& taskId, FakeTask* task) {
    scheduler->addTask(taskId, task);
  }

  void makeOfferOnTick(const ResourceHints& resources) {
    hashmap<SlaveID, ResourceHints> offers;
    offers[masterSlaveId] = resources;
    process::Clock::advance(kTick);
    process::dispatch(master->self(), &Master::offer, masterFrameworkId, offers);
  }

  void tick() {
    process::Clock::advance(kTick);
    process::Clock::settle();
  }

  void waitForStatus(trigger* trig) {
    StatusUpdateMessage message;
    EXPECT_MESSAGE(filter, message.GetTypeName(), _, masterPid).
      WillOnce(DoAll(Trigger(trig), Return(false))).
      RetiresOnSaturation();
  }

  void SetUp() {
    process::filter(&filter);
  }

  void TearDown() {
    process::filter(0);
  }

protected:
  MockFilter filter;
  MockAllocator<mesos::internal::master::DominantShareAllocator> allocator;
  FakeTaskTracker tasks;

  scoped_ptr<FakeScheduler> scheduler;
  FrameworkID masterFrameworkId;
  SlaveID masterSlaveId;
  scoped_ptr<MesosSchedulerDriver> driver;
  scoped_ptr<FakeIsolationModule> module;
  scoped_ptr<BasicMasterDetector> detector;

  scoped_ptr<Master> master;
  PID<Master> masterPid;
  scoped_ptr<Slave> slave;
  PID<Slave> slavePid;
};

TEST_F(MasterSlaveFakeTest, RunSchedulerNoOffers) {
  startMasterAndSlave();
  startScheduler();
  stopScheduler();
  stopMasterAndSlave();
}

TEST_F(MasterSlaveFakeTest, RunSchedulerRejectOffer) {
  startMasterAndSlave();
  startScheduler();
  trigger offerReturned;
  EXPECT_CALL(allocator, resourcesUnused(_, _, _, _)).
    WillOnce(Trigger(&offerReturned));
  makeOfferOnTick(ResourceHints::parse("cpu:8;mem:4096", "cpu:8;mem:4096"));
  WAIT_UNTIL(offerReturned);
  stopScheduler();
  stopMasterAndSlave();
}

TEST_F(MasterSlaveFakeTest, RunSchedulerRunOneTick) {
  startMasterAndSlave();
  startScheduler();
  MockFakeTask task;
  trigger tookUsage;
  EXPECT_CALL(task, getUsage(_, _)).
    WillRepeatedly(Return(Resources::parse("cpu:3;mem:1024")));
  EXPECT_CALL(task, takeUsage(_, _, Resources::parse("cpu:3;mem:1024"))).
    WillOnce(DoAll(Trigger(&tookUsage),
                   Return(TASK_RUNNING)));
  EXPECT_CALL(task, getResourceRequest()).
    WillRepeatedly(Return(ResourceHints::parse("cpu:4;mem:2048", "")));
  addTask("task0", &task);
  trigger offerComplete, gotStatus;
  EXPECT_CALL(allocator, resourcesUnused(_, _,
              ResourceHints::parse("cpu:4;mem:2048", "cpu:8;mem:4096"), _)).
    WillOnce(Trigger(&offerComplete));
  waitForStatus(&gotStatus);
  LOG(INFO) << "about to makeOffer";
  makeOfferOnTick(ResourceHints::parse("cpu:8;mem:4096", "cpu:8;mem:4096"));
  WAIT_UNTIL(offerComplete);
  WAIT_UNTIL(gotStatus);
  LOG(INFO) << "gotStatus";
  EXPECT_CALL(allocator, gotUsage(_));
  tick();  // task should schedule by now.
  WAIT_UNTIL(tookUsage);
  LOG(INFO) << "tookUsage";
  // FIXME: test allocator sees task start?
  trigger tookFinished;
  EXPECT_CALL(task, takeUsage(_, _, _)).
    WillOnce(DoAll(Trigger(&tookFinished), Return(TASK_FINISHED)));
  EXPECT_CALL(allocator, gotUsage(_));
  EXPECT_CALL(allocator, resourcesRecovered(_, _, _)).WillOnce(Return());
  LOG(INFO) << "about to tick";
  tick();
  WAIT_UNTIL(tookFinished);
  LOG(INFO) << "tookFinished";
  stopScheduler();
  stopMasterAndSlave();
}
