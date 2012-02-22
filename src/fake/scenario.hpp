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

#ifndef __FAKE_SCENARIO_HPP__
#define __FAKE_SCENARIO_HPP__

#include <istream>
#include <string>
#include <vector>
#include <map>

#include "boost/property_tree/ptree_fwd.hpp"
#include "boost/property_tree/ptree.hpp"
#include "boost/scoped_ptr.hpp"

#include "process/pid.hpp"

#include "configurator/configurator.hpp"
#include "configurator/configuration.hpp"
#include "detector/detector.hpp"
#include "fake/fake_isolation_module.hpp"
#include "fake/fake_scheduler.hpp"
#include "fake/fake_task.hpp"
#include "master/master.hpp"
#include "slave/slave.hpp"

namespace mesos {
namespace internal {
namespace fake {

typedef mesos::internal::slave::Slave Slave;
typedef mesos::internal::master::Master Master;

class Scenario {
public:
  static void registerOptions(Configurator* configurator);

  void spawnMaster();
  void spawnMaster(mesos::internal::master::Allocator* allocator);
  void spawnSlave(const Resources& resources);
  FakeScheduler* spawnScheduler(const std::string& name,
                                const Attributes& attributes,
                                const std::map<TaskID, FakeTask*>& tasks);

  FakeScheduler* getScheduler(const std::string& name) {
    return schedulers[name];
  }
  const std::map<std::string, FakeScheduler*>& getSchedulers() {
    return schedulers;
  }
  void finishSetup();
  void runFor(double seconds);
  void stop();
  const std::string& getLabel() const {
    return label;
  }
  void setLabel(const std::string& label_) {
    label = label_;
  }
  const std::string& getLabelColumns() const {
    return labelColumns;
  }
  void setLabelColumns(const std::string& labelColumns_) {
    labelColumns = labelColumns_;
  }
  Scenario();
  Scenario(const Configuration& conf_);
  ~Scenario() { stop(); }
private:
  void init();

  Configuration conf;
  FakeTaskTracker tracker;
  Master* master;
  process::PID<Master> masterPid;
  std::vector<Slave*> slaves;
  std::vector<process::PID<Slave> > slavePids;
  boost::scoped_ptr<BasicMasterDetector> masterMasterDetector;
  std::vector<BasicMasterDetector*> slaveMasterDetectors;
  std::map<std::string, MesosSchedulerDriver*> schedulerDrivers;
  std::map<std::string, FakeScheduler*> schedulers;
  std::vector<FakeTask*> allTasks;
  std::vector<FakeIsolationModule*> isolationModules;
  std::string label;
  std::string labelColumns;
};

void populateScenarioFrom(const boost::property_tree::ptree& spec,
                          Scenario* scenario);
void populateScenarioFrom(std::istream* in, Scenario* scenario);

}  // namespace fake
}  // namespace internal
}  // namespace mesos

#endif
