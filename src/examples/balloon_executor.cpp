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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include <iostream>
#include <string>

#include <boost/lexical_cast.hpp>

#include <mesos/executor.hpp>

using namespace mesos;


static void balloon(size_t mb)
{
  size_t step = 64 * 1024 * 1024;
  for (size_t i = 0; i < mb / 64; i++) {
    char* buffer = (char *)malloc(step);
    ::memset(buffer, 1, step);
    ::sleep(1);
  }
}


class BalloonExecutor : public Executor
{
public:
  virtual ~BalloonExecutor() {}

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
    std::cout << "Registered" << std::endl;
  }

  virtual void reregistered(ExecutorDriver* driver,
                            const SlaveInfo& slaveInfo)
  {
    std::cout << "Reregistered" << std::endl;
  }

  virtual void disconnected(ExecutorDriver* driver)
  {
    std::cout << "Disconnected" << std::endl;
  }

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    std::cout << "Starting task " << task.task_id().value() << std::endl;

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_RUNNING);

    driver->sendStatusUpdate(status);

    std::istringstream istr(task.data());
    int balloonSize = 0, childBalloonSize = 0;
    istr >> balloonSize >> childBalloonSize;
    if (istr) {
      std::cerr << "Could not parse " << task.data();
    }

    pid_t child = 0;

    if (childBalloonSize > 0) {
      child = ::fork();
      if (child == -1) {
        ::setpriority(PRIO_PROCESS, getpid(), 10);
        balloon(childBalloonSize);
        ::_exit(0);
      }
    }

    // Simulate a memory leak situation.
    balloon(balloonSize);

    if (child) {
      waitpid(child, NULL, 0);
    }

    std::cout << "Finishing task " << task.task_id().value() << std::endl;

    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_FINISHED);

    driver->sendStatusUpdate(status);
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    std::cout << "Kill task " << taskId.value() << std::endl;
  }

  virtual void frameworkMessage(ExecutorDriver* driver, const std::string& data)
  {
    std::cout << "Framework message: " << data << std::endl;
  }

  virtual void shutdown(ExecutorDriver* driver)
  {
    std::cout << "Shutdown" << std::endl;
  }

  virtual void error(ExecutorDriver* driver, const std::string& message)
  {
    std::cout << "Error message: " << message << std::endl;
  }
};


int main(int argc, char** argv)
{
  BalloonExecutor executor;
  MesosExecutorDriver driver(&executor);
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
