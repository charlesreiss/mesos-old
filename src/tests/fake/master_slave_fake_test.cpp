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

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <process/process.hpp>
#include <boost/scoped_ptr.hpp>

#include "detector/detector.hpp"

#include "fake/fake_isolation_module.hpp"
#include "fake/fake_scheduler.hpp"

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

using testing::_;
using testing::DoAll;
using testing::Return;
using testing::SaveArg;

class MasterSlaveFakeTest : public testing::Test {
public:
  void startMasterAndSlave()
  {
    process::Clock::pause();
    ASSERT_TRUE(GTEST_IS_THREADSAFE);
    master.reset(new Master(&allocator));
    masterPid = process::spawn(master.get());

    module.reset(new FakeIsolationModule(tasks));
    slave.reset(new Slave(Resources::parse("cpu:8.0;mem:4096"), true,
                          module.get()));
    slavePid = process::spawn(slave.get());

    trigger gotSlave;
    EXPECT_CALL(allocator, slaveAdded(_)).
      WillOnce(Trigger(&gotSlave));
    detector.reset(new BasicMasterDetector(masterPid, slavePid, true));
    WAIT_UNTIL(gotSlave);
  }

  void startScheduler() {
    if (!scheduler.get()) {
      scheduler.reset(new FakeScheduler);
    }
    trigger gotFramework;
    driver.reset(
      new MesosSchedulerDriver(scheduler.get(), "", DEFAULT_EXECUTOR_INFO,
                               masterPid));
    EXPECT_CALL(allocator, frameworkAdded(_)).
      WillOnce(Trigger(&gotFramework));
    driver->start();
    WAIT_UNTIL(gotFramework);
  }

  void stopScheduler() {
    trigger lostFramework;
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
  }

protected:
  FakeTaskMap tasks;
  MockAllocator allocator;

  scoped_ptr<FakeScheduler> scheduler;
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
