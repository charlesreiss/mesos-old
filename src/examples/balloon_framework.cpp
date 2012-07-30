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

#include <libgen.h>
#include <stdlib.h>

#include <sys/param.h>

#include <iostream>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

#include <mesos/scheduler.hpp>

using namespace mesos;


class BalloonScheduler : public Scheduler
{
public:
  BalloonScheduler(const ExecutorInfo& _executor,
                   const int32_t _balloonSize,
                   const int32_t _lowPriorityBalloonSize)
    : executor(_executor),
      balloonSize(_balloonSize),
      lowPriorityBalloonSize(_lowPriorityBalloonSize),
      tasksLaunched(0) {}

  virtual ~BalloonScheduler() {}

  virtual void registered(SchedulerDriver*,
                          const FrameworkID&,
                          const MasterInfo&)
  {
    std::cout << "Registered" << std::endl;
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo)
  {
    std::cout << "Reregistered" << std::endl;
  }

  virtual void disconnected(SchedulerDriver* driver)
  {
    std::cout << "Disconnected" << std::endl;
  }

  virtual void resourceOffers(SchedulerDriver* driver,
                              const std::vector<Offer>& offers)
  {
    std::cout << "Resource offers received" << std::endl;

    for (int i = 0; i < offers.size(); i++) {
      const Offer& offer = offers[i];

      // We just launch one task.
      if (tasksLaunched++ == 0) {
        double mem = getScalarResource(offer, "mem");
        assert(mem > 64);

        std::vector<TaskInfo> tasks;
        std::cout << "Starting the task" << std::endl;

        TaskInfo task;
        task.set_name("Balloon Task");
        task.mutable_task_id()->set_value("1");
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_executor()->MergeFrom(executor);
        task.set_data(boost::lexical_cast<std::string>(balloonSize)
            + " " + boost::lexical_cast<std::string>(lowPriorityBalloonSize));

        // Use up all the memory from the offer. 64MB for the executor itself.
        Resource* resource;
        resource = task.add_resources();
        resource->set_name("mem");
        resource->set_type(Value::SCALAR);
        resource->mutable_scalar()->set_value(mem - 64);

        tasks.push_back(task);
        driver->launchTasks(offer.id(), tasks);
      }
    }
  }

  virtual void offerRescinded(SchedulerDriver* driver,
                              const OfferID& offerId)
  {
    std::cout << "Offer rescinded" << std::endl;
  }

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    std::cout << "Task in state " << status.state() << std::endl;

    if (status.state() == TASK_FINISHED ||
        status.state() == TASK_FAILED ||
        status.state() == TASK_KILLED ||
        status.state() == TASK_LOST) {
      driver->stop();
    }
  }

  virtual void frameworkMessage(SchedulerDriver* driver,
                                const ExecutorID& executorId,
                                const SlaveID& slaveId,
                                const std::string& data)
  {
    std::cout << "Framework message: " << data << std::endl;
  }

  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& sid)
  {
    std::cout << "Slave lost" << std::endl;
  }

  virtual void executorLost(SchedulerDriver* driver,
                            const ExecutorID& executorID,
                            const SlaveID& slaveID,
                            int status)
  {
    std::cout << "Executor lost" << std::endl;
  }

  virtual void error(SchedulerDriver* driver, const std::string& message)
  {
    std::cout << "Error message: " << message << std::endl;
  }

private:
  double getScalarResource(const Offer& offer, const std::string& name)
  {
    double value = 0.0;

    for (int i = 0; i < offer.resources_size(); i++) {
      const Resource& resource = offer.resources(i);
      if (resource.name() == name &&
          resource.type() == Value::SCALAR) {
        value = resource.scalar().value();
      }
    }

    return value;
  }

  const ExecutorInfo executor;
  int32_t balloonSize;
  int32_t lowPriorityBalloonSize;
  int tasksLaunched;
};


int main(int argc, char** argv)
{
  if (argc != 3 && argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " <master> <balloon size in MB> <high-priority balloon size>" << std::endl;
    return -1;
  }

  // Find this executable's directory to locate executor.
  char buf[MAXPATHLEN];
  ::realpath(::dirname(argv[0]), buf);
  std::string uri = std::string(buf) + "/balloon-executor";
  if (getenv("MESOS_BUILD_DIR")) {
    uri = std::string(::getenv("MESOS_BUILD_DIR")) + "/src/balloon-executor";
  }

  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("default");
  executor.mutable_command()->set_value(uri);

  Resource *mem = executor.add_resources();
  mem->set_name("mem");
  mem->set_type(Value::SCALAR);
  mem->mutable_scalar()->set_value(64); // Executor takes 64MB.

  BalloonScheduler scheduler(executor, atoi(argv[2]),
      argc > 3 ? atoi(argv[3]) : 0);

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("Balloon Framework (C++)");

  MesosSchedulerDriver driver(&scheduler, framework, argv[1]);

  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
