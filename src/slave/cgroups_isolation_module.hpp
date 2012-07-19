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

#ifndef __CGROUPS_ISOLATION_MODULE_HPP__
#define __CGROUPS_ISOLATION_MODULE_HPP__

#include <string>

#include <process/future.hpp>
#include <process/id.hpp>

#include <stout/hashmap.hpp>

#include "launcher/launcher.hpp"

#include "slave/flags.hpp"
#include "slave/isolation_module.hpp"
#include "slave/reaper.hpp"
#include "slave/slave.hpp"

namespace mesos {
namespace internal {
namespace slave {

class CgroupsIsolationModule
  : public IsolationModule, public ProcessExitedListener
{
public:
  CgroupsIsolationModule();

  virtual ~CgroupsIsolationModule();

  virtual void initialize(const Flags& flags,
                          bool local,
                          const process::PID<Slave>& slave);

  virtual void launchExecutor(const FrameworkID& frameworkId,
                              const FrameworkInfo& frameworkInfo,
                              const ExecutorInfo& executorInfo,
                              const std::string& directory,
                              const ResourceHints& resources);

  virtual void killExecutor(const FrameworkID& frameworkId,
                            const ExecutorID& executorId);

  virtual void resourcesChanged(const FrameworkID& frameworkId,
                                const ExecutorID& executorId,
                                const ResourceHints& resources);

  virtual void processExited(pid_t pid, int status);

protected:
  // Main method executed after a fork() to create a Launcher for launching an
  // executor's process. The Launcher will chdir() to the child's working
  // directory, fetch the executor, set environment varibles, switch user, etc,
  // and finally exec() the executor process.
  virtual launcher::ExecutorLauncher* createExecutorLauncher(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory);

private:
  // No copying, no assigning.
  CgroupsIsolationModule(const CgroupsIsolationModule&);
  CgroupsIsolationModule& operator = (const CgroupsIsolationModule&);

  // The cgroup information for each registered executor.
  struct CgroupInfo
  {
    FrameworkID frameworkId;
    ExecutorID executorId;

    // PID of the executor.
    pid_t pid;

    // Whether the executor has been killed.
    bool killed;

    // Used to cancel the OOM listening.
    process::Future<uint64_t> oomNotifier;
  };

  // Return the subsystems used by the isolation module.
  // @return  The comma-separated subsystem names.
  std::string subsystems();

  // Return the path to the hierarchy root used by the isolation module. We use
  // a single hierarchy root for all the subsystems.
  // @return  The path to the hierarchy root.
  std::string hierarchy();

  // Return the canonicalized name of the cgroup used by a given executor in a
  // given framework.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  // @return  The canonicalized name of the cgroup.
  std::string cgroup(const FrameworkID& frameworkId,
                     const ExecutorID& executorId);

  // Register a cgroup in the isolation module.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  // @return  A pointer to the cgroup info registered.
  CgroupInfo* registerCgroupInfo(const FrameworkID& frameworkId,
                                 const ExecutorID& executorId)
  {
    CgroupInfo* info = new CgroupInfo;
    info->frameworkId = frameworkId;
    info->executorId = executorId;
    info->pid = -1;
    info->killed = false;
    infos[frameworkId][executorId] = info;
    return info;
  }

  // Unregister a cgroup in the isolation module.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  void unregisterCgroupInfo(const FrameworkID& frameworkId,
                            const ExecutorID& executorId)
  {
    if (infos.find(frameworkId) != infos.end()) {
      if (infos[frameworkId].find(executorId) != infos[frameworkId].end()) {
        delete infos[frameworkId][executorId];
        infos[frameworkId].erase(executorId);
        if (infos[frameworkId].empty()) {
          infos.erase(frameworkId);
        }
      }
    }
  }

  // Find a registered cgroup by the PID of the leading process.
  // @param   pid           The PID of the leading process in the cgroup.
  // @return  A pointer to the cgroup info if found, NULL otherwise.
  CgroupInfo* findCgroupInfo(pid_t pid)
  {
    foreachkey (const FrameworkID& frameworkId, infos) {
      foreachvalue (CgroupInfo* info, infos[frameworkId]) {
        if (info->pid == pid) {
          return info;
        }
      }
    }
    return NULL;
  }

  // Find a registered cgroup by the frameworkId and the executorId.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  // @return  A pointer to the cgroup info if found, NULL otherwise.
  CgroupInfo* findCgroupInfo(const FrameworkID& frameworkId,
                             const ExecutorID& executorId)
  {
    if (infos.find(frameworkId) != infos.end()) {
      if (infos[frameworkId].find(executorId) != infos[frameworkId].end()) {
        return infos[frameworkId][executorId];
      }
    }
    return NULL;
  }

  // Set controls for the cgroup used by a given executor in a given framework.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  // @param   resources     The handle for the resources.
  // @return  Whether the operation successes.
  Try<bool> setCgroupControls(const FrameworkID& frameworkId,
                              const ExecutorID& executorId,
                              const ResourceHints& resources);

  // Start listening on OOM events. This function will create an eventfd and
  // start polling on it.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  void oomListen(const FrameworkID& frameworkId,
                 const ExecutorID& executorId);

  // This function is invoked when the polling on eventfd has a result.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  void oomWaited(const FrameworkID& frameworkId,
                 const ExecutorID& executorId,
                 const process::Future<uint64_t>& future);

  // This function is invoked when the OOM event happens.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  void oom(const FrameworkID& frameworkId,
           const ExecutorID& executorId);

  // This callback is invoked when destroy cgroup has a result.
  // @param   frameworkId   The id of the given framework.
  // @param   executorId    The id of the given executor.
  // @param   future        The future describing the destroy process.
  void destroyWaited(const FrameworkID& frameworkId,
                     const ExecutorID& executorId,
                     const process::Future<bool>& future);

  Flags flags;
  bool local;
  process::PID<Slave> slave;
  bool initialized;
  Reaper* reaper;
  hashmap<FrameworkID, hashmap<ExecutorID, CgroupInfo*> > infos;
};


} // namespace mesos {
} // namespace internal {
} // namespace slave {


#endif // __CGROUPS_ISOLATION_MODULE_HPP__
