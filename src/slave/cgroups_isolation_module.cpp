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

#include <signal.h>
#include <unistd.h>

#include <sys/types.h>

#include <sstream>

#include <process/defer.hpp>
#include <process/dispatch.hpp>

#include <stout/foreach.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/uuid.hpp>

#include "common/units.hpp"

#include "linux/cgroups.hpp"
#include "linux/proc.hpp"

#include "slave/cgroups_isolation_module.hpp"

using namespace process;


namespace mesos {
namespace internal {
namespace slave {

const size_t CPU_SHARES_PER_CPU = 1024;
const size_t MIN_CPU_SHARES = 10;
const size_t MIN_MEMORY_MB = 32 * Megabyte;


CgroupsIsolationModule::CgroupsIsolationModule()
  : ProcessBase(ID::generate("cgroups-isolation-module")),
    initialized(false)
{
  // Spawn the reaper, note that it might send us a message before we
  // actually get spawned ourselves, but that's okay, the message will
  // just get dropped.
  reaper = new Reaper();
  spawn(reaper);
  dispatch(reaper, &Reaper::addProcessExitedListener, this);
}


CgroupsIsolationModule::~CgroupsIsolationModule()
{
  CHECK(reaper != NULL);
  terminate(reaper);
  process::wait(reaper); // Necessary for disambiguation.
  delete reaper;
}


void CgroupsIsolationModule::initialize(
    const Flags& _flags,
    bool _local,
    const PID<Slave>& _slave)
{
  flags = _flags;
  slave = _slave;

  if (flags.cgroup_oom_policy == "kill") {
    oomPolicy = OOM_KILL;
  } else if (flags.cgroup_oom_policy == "kill-priority") {
    oomPolicy = OOM_KILL_PRIORITY;
  } else {
    LOG(FATAL) << "Bad OOM policy " << flags.cgroup_oom_policy;
  }
  // Make sure that we have root permission.
  if (os::user() != "root") {
    LOG(FATAL) << "Cgroups isolation module needs root permission";
  }

  // Make sure that cgroups is enabled by the kernel.
  if (!cgroups::enabled()) {
    LOG(FATAL) << "Cgroups is not supported by the kernel";
  }

  // Configure cgroups hierarchy root path.
  hierarchy = flags.cgroups_hierarchy_root;

  LOG(INFO) << "Using " << hierarchy << " as cgroups hierarchy root";

  // Configure required/optional subsystems.
  hashset<std::string> requiredSubsystems;
  hashset<std::string> optionalSubsystems;

  requiredSubsystems.insert("cpu");
  requiredSubsystems.insert("cpuset");
  requiredSubsystems.insert("memory");
  requiredSubsystems.insert("freezer");

  optionalSubsystems.insert("blkio");

  // Probe cgroups subsystems.
  hashset<std::string> enabledSubsystems;
  hashset<std::string> busySubsystems;

  Try<std::set<std::string> > enabled = cgroups::subsystems();
  if (enabled.isError()) {
    LOG(FATAL) << "Failed to probe cgroups subsystems: " << enabled.error();
  } else {
    foreach (const std::string& name, enabled.get()) {
      enabledSubsystems.insert(name);

      Try<bool> busy = cgroups::busy(name);
      if (busy.isError()) {
        LOG(FATAL) << "Failed to probe cgroups subsystems: " << busy.error();
      }

      if (busy.get()) {
        busySubsystems.insert(name);
      }
    }
  }


  // Make sure that all the required subsystems are enabled by the kernel.
  foreach (const std::string& name, requiredSubsystems) {
    if (!enabledSubsystems.contains(name)) {
      LOG(FATAL) << "Required subsystem " << name
                 << " is not enabled by the kernel";
    }
  }

  // Prepare the cgroups hierarchy root.
  Try<bool> check = cgroups::checkHierarchy(hierarchy);
  if (check.isError()) {
    // The given hierarchy is not a cgroups hierarchy root. We will try to
    // create a cgroups hierarchy root there.
    if (os::exists(hierarchy)) {
      // The path specified by the given hierarchy already exists in the file
      // system. We try to remove it if it is an empty directory. This will
      // helps us better deal with slave reboots as we don't need to manually
      // remove the residue directory after a slave reboot.
      if (::rmdir(hierarchy.c_str()) < 0) {
        LOG(FATAL) << "Cannot create cgroups hierarchy root at " << hierarchy
                   << ". Consider removing it.";
      }
    }

    // The comma-separated subsystem names which will be passed to
    // cgroups::createHierarchy to create the hierarchy root.
    std::string subsystems;

    // Make sure that all the required subsystems are not busy so that we can
    // activate them in the given cgroups hierarchy root.
    foreach (const std::string& name, requiredSubsystems) {
      if (busySubsystems.contains(name)) {
        LOG(FATAL) << "Required subsystem " << name << " is busy";
      }

      subsystems.append(name + ",");
    }

    // Also activate those optional subsystems that are not busy.
    foreach (const std::string& name, optionalSubsystems) {
      if (enabledSubsystems.contains(name) && !busySubsystems.contains(name)) {
        subsystems.append(name + ",");
      }
    }

    // Create the cgroups hierarchy root.
    Try<bool> create = cgroups::createHierarchy(hierarchy,
                                                strings::trim(subsystems, ","));
    if (create.isError()) {
      LOG(FATAL) << "Failed to create cgroups hierarchy root at " << hierarchy
                 << ": " << create.error();
    }
  }

  // Probe activated subsystems in the cgroups hierarchy root.
  Try<std::set<std::string> > activated = cgroups::subsystems(hierarchy);
  foreach (const std::string& name, activated.get()) {
    activatedSubsystems.insert(name);
  }

  // Make sure that all the required subsystems are activated.
  foreach (const std::string& name, requiredSubsystems) {
    if (!activatedSubsystems.contains(name)) {
      LOG(FATAL) << "Required subsystem " << name
                 << " is not activated in hierarchy " << hierarchy;
    }
  }

  // Try to cleanup the cgroups in the cgroups hierarchy root that belong to
  // this module (which are created in the previous executions).
  Try<std::vector<std::string> > cgroups = cgroups::getCgroups(hierarchy);
  if (cgroups.isError()) {
    LOG(FATAL) << "Failed to peek cgroups in hierarchy " << hierarchy
               << ": " << cgroups.error();
  }

  foreach (const std::string cgroup, cgroups.get()) {
    if (isValidCgroupName(cgroup)) {
      LOG(INFO) << "Removing stale cgroup " << cgroup
                << " in hierarchy " << hierarchy;
      Future<bool> future = cgroups::destroyCgroup(hierarchy, cgroup);
      future.onAny(
          defer(PID<CgroupsIsolationModule>(this),
                &CgroupsIsolationModule::destroyWaited,
                cgroup,
                future));
    }
  }

  // Configure resource subsystem mapping.
  resourceSubsystemMap["cpus"] = "cpu";
  resourceSubsystemMap["mem"] = "memory";

  // Configure resource changed handlers.
  resourceChangedHandlers["cpus"] = &CgroupsIsolationModule::cpusChanged;
  resourceChangedHandlers["mem"] = &CgroupsIsolationModule::memChanged;

  if (flags.cgroup_outer_container) {
    Try<bool> create =
      cgroups::createCgroup(hierarchy, flags.cgroup_outer_container_name);
    if (create.isError()) {
      LOG(ERROR) << "Failed to create outer container: " << create.error();
    }
    // XXX share with slave
    long slaveMemory = 1024L * 1024L * 1024L;
    if (flags.resources.isNone()) {
      Try<long> osMemory = os::memory();
      if (osMemory.isSome()) {
        slaveMemory = osMemory.get();
      } else {
        LOG(ERROR) << "Couldn't determine system memory: " << osMemory.error();
      }
    } else {
      Resources slaveResources = Resources::parse(flags.resources.get());
      slaveMemory = static_cast<long>(
        slaveResources.get("mem", Value::Scalar()).value() * 1024.0 * 1024.0
      );
    }
    Try<bool> setMemoryResult =
      cgroups::writeControl(hierarchy,
          flags.cgroup_outer_container_name,
          "memory.limit_in_bytes",
          stringify(slaveMemory));
    CHECK(!setMemoryResult.isError()) << setMemoryResult.error();

    setupOuterOom();
  }

  initialized = true;
}


void CgroupsIsolationModule::launchExecutor(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const std::string& directory,
    const ResourceHints& resources)
{
  CHECK(initialized) << "Cannot launch executors before initialization";

  const ExecutorID& executorId = executorInfo.executor_id();

  // Register the cgroup information.
  registerCgroupInfo(frameworkId, executorId);

  LOG(INFO) << "Launching " << executorId
            << " (" << executorInfo.command().value() << ")"
            << " in " << directory
            << " with resources " << resources
            << " for framework " << frameworkId
            << " in cgroup " << getCgroupName(frameworkId, executorId);

  // Create a new cgroup for the executor.
  Try<bool> create =
    cgroups::createCgroup(hierarchy, getCgroupName(frameworkId, executorId));
  if (create.isError()) {
    LOG(FATAL) << "Failed to create cgroup for executor " << executorId
               << " of framework " << frameworkId
               << ": " << create.error();
  }

  // Copy the values of cpuset.cpus and cpuset.mems from the cgroups hierarchy
  // root. This is necessary because the newly created cgroup does not have
  // these two values set.
  // TODO(jieyu): Think about other ways that do not rely on the values from the
  // cgroups hierarchy root.
  Try<std::string> rootCpusetCpus =
    cgroups::readControl(hierarchy,
                         "/",
                         "cpuset.cpus");
  if (rootCpusetCpus.isError()) {
    LOG(FATAL) << "Failed to get cpuset.cpus in hierarchy root: "
               << rootCpusetCpus.error();
  }

  Try<std::string> rootCpusetMems =
    cgroups::readControl(hierarchy,
                         "/",
                         "cpuset.mems");
  if (rootCpusetMems.isError()) {
    LOG(FATAL) << "Failed to get cpuset.mems in hierarchy root: "
               << rootCpusetMems.error();
  }

  Try<bool> setCpusetCpus =
    cgroups::writeControl(hierarchy,
                          getCgroupName(frameworkId, executorId),
                          "cpuset.cpus",
                          rootCpusetCpus.get());
  if (setCpusetCpus.isError()) {
    LOG(FATAL) << "Failed to write cpuset.cpus for executor "
               << executorId << " of framework " << frameworkId
               << ": " << setCpusetCpus.error();
  }

  Try<bool> setCpusetMems =
    cgroups::writeControl(hierarchy,
                          getCgroupName(frameworkId, executorId),
                          "cpuset.mems",
                          rootCpusetMems.get());
  if (setCpusetMems.isError()) {
    LOG(FATAL) << "Failed to write cpuset.mems for executor "
               << executorId << " of framework " << frameworkId
               << ": " << setCpusetMems.error();
  }

  // Setup the initial resource constrains.
  resourcesChanged(frameworkId, executorId, resources);

  // Start listening on OOM events.
  oomListen(frameworkId, executorId);

  // Launch the executor using fork-exec.
  pid_t pid;
  if ((pid = ::fork()) == -1) {
    LOG(FATAL) << "Failed to fork to launch new executor";
  }

  if (pid) {
    // In parent process.
    LOG(INFO) << "Forked executor at = " << pid;

    // Store the pid of the leading process of the executor.
    CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
    CHECK(info != NULL) << "Cannot find cgroup info";
    info->pid = pid;

    // Tell the slave this executor has started.
    dispatch(slave,
             &Slave::executorStarted,
             frameworkId,
             executorId,
             pid);
  } else {
    // In child process.
    // Put self into the newly created cgroup.
    Try<bool> assign =
      cgroups::assignTask(hierarchy,
                          getCgroupName(frameworkId, executorId),
                          ::getpid());
    if (assign.isError()) {
      LOG(FATAL) << "Failed to assign for executor " << executorId
                 << " of framework " << frameworkId
                 << ": " << assign.error();
    }

    launcher::ExecutorLauncher* launcher =
      createExecutorLauncher(frameworkId,
                             frameworkInfo,
                             executorInfo,
                             directory);
    launcher->run();
  }
}


void CgroupsIsolationModule::killExecutor(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CHECK(initialized) << "Cannot kill executors before initialization";

  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  if (info == NULL || info->killed) {
    LOG(ERROR) << "Asked to kill an unknown/killed executor!";
    return;
  }

  LOG(INFO) << "Killing executor " << executorId
            << " of framework " << frameworkId;

  // Stop the OOM listener if needed.
  if (info->oomNotifier.isPending()) {
    info->oomNotifier.discard();
  }

  // Destroy the cgroup that is associated with the executor. Here, we don't
  // wait for it to succeed as we don't want to block the isolation module.
  // Instead, we register a callback which will be invoked when its result is
  // ready.
  Future<bool> future =
    cgroups::destroyCgroup(hierarchy,
                           getCgroupName(frameworkId, executorId));
  future.onAny(
      defer(PID<CgroupsIsolationModule>(this),
            &CgroupsIsolationModule::destroyWaited,
            getCgroupName(frameworkId, executorId),
            future));

  // We do not unregister the cgroup info here, instead, we ask the process
  // exit handler to unregister the cgroup info.
  info->killed = true;

  recentKills[frameworkId].insert(executorId);
}


void CgroupsIsolationModule::resourcesChanged(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ResourceHints& resources)
{
  CHECK(initialized) << "Cannot change resources before initialization";

  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  if (info == NULL || info->killed) {
    LOG(INFO) << "Asked to update resources for an unknown/killed executor";
    return;
  }

  LOG(INFO) << "Changing cgroup controls for executor " << executorId
            << " of framework " << frameworkId
            << " with resources " << resources;

  // For each resource, invoke the corresponding handler.
  // XXX FIXME: Handle minResources!
  for (Resources::const_iterator it = resources.expectedResources.begin();
       it != resources.expectedResources.end(); ++it) {
    const Resource& resource = *it;
    const std::string& name = resource.name();

    if (resourceChangedHandlers.contains(name)) {
      // We only call the resource changed handler either if the resource does
      // not depend on any subsystem, or the dependent subsystem is active.
      if (!resourceSubsystemMap.contains(name) ||
          activatedSubsystems.contains(resourceSubsystemMap[name])) {
        Try<bool> result =
          (this->*resourceChangedHandlers[name])(frameworkId,
                                                 executorId,
                                                 resources);
        if (result.isError()) {
          LOG(ERROR) << result.error();
        }
      }
    }
  }
}


void CgroupsIsolationModule::processExited(pid_t pid, int status)
{
  CgroupInfo* info = findCgroupInfo(pid);
  if (info != NULL) {
    FrameworkID frameworkId = info->frameworkId;
    ExecutorID executorId = info->executorId;

    LOG(INFO) << "Telling slave of lost executor " << executorId
              << " of framework " << frameworkId;

    dispatch(slave,
             &Slave::executorExited,
             frameworkId,
             executorId,
             status);

    if (!info->killed) {
      killExecutor(frameworkId, executorId);
    }

    unregisterCgroupInfo(frameworkId, executorId);
  } else {
    LOG(ERROR) << "Process exited with no associated cgroup: " << pid;
  }
}

void CgroupsIsolationModule::insertStats(
    const std::string& hierarchy,
    const std::string& container,
    const std::string& controller,
    const std::string& prefix,
    hashmap<std::string, int64_t>* counters)
{
  Try<std::string> statOutput =
    cgroups::readControl(hierarchy, container, controller);
  if (statOutput.isError()) {
    LOG(ERROR) << "Coult not read " << controller << " for " << container
               << ": " << statOutput.error();
    return;
  }
  Try<hashmap<std::string, unsigned long> > statResult =
    parseStat(statOutput.get());
  if (statResult.isError()) {
    LOG(ERROR) << "Could not parse " << controller << ": " <<
      statResult.error();
  }
  foreachpair (const std::string& key, unsigned long value,
               statResult.get()) {
    (*counters)[prefix + key] = value;
  }
}


Option<ResourceStatistics> CgroupsIsolationModule::collectResourceStatistics(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  ResourceStatistics stat;
  if (recentKills.count(frameworkId) &&
      recentKills[frameworkId].count(executorId)) {
    stat.miscAbsolute["killed"] += 1.0;
    recentKills[frameworkId].erase(executorId);
  }
  if (recentOoms.count(frameworkId) &&
      recentOoms[frameworkId].count(executorId)) {
    stat.miscAbsolute["oom"] += 1.0;
    recentOoms[frameworkId].erase(executorId);
  }

  LOG(INFO) << "Gathering statistics for " << frameworkId << " / " << executorId;
  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  if (info == NULL || info->killed) {
    return Option<ResourceStatistics>::none();
  }

  // Get CPU related statistics.
  Try<std::string> cpuStatOutput =
    cgroups::readControl(hierarchy,
                         getCgroupName(frameworkId, executorId),
                         "cpuacct.stat");
  if (cpuStatOutput.isError()) {
    LOG(ERROR) << "Failed to read cpuacct.stat: " << cpuStatOutput.error();
    return Option<ResourceStatistics>::none();
  }

  Try<hashmap<std::string, unsigned long> > cpuStatResult =
    parseStat(cpuStatOutput.get());
  if (cpuStatResult.isError()) {
    LOG(ERROR) << "Failed to parse cpuacct.stat: " << cpuStatResult.error();
    return Option<ResourceStatistics>::none();
  }

  hashmap<std::string, unsigned long> cpuStat = cpuStatResult.get();
  if (!cpuStat.contains("user") || !cpuStat.contains("system")) {
    LOG(ERROR) << "Did not find user/system time in cpuacct.stat";
    return Option<ResourceStatistics>::none();
  }

  // Get memory related statistics.
  Try<std::string> memStatOutput =
    cgroups::readControl(hierarchy,
                         getCgroupName(frameworkId, executorId),
                         "memory.stat");
  if (memStatOutput.isError()) {
    LOG(ERROR) << "Failed to read memory.stat: " << memStatOutput.error();
    return Option<ResourceStatistics>::none();
  }

  Try<hashmap<std::string, unsigned long> > memStatResult =
    parseStat(memStatOutput.get());
  if (memStatResult.isError()) {
    LOG(ERROR) << "Failed to parse memory.stat: " << memStatResult.error();
    return Option<ResourceStatistics>::none();
  }

  hashmap<std::string, unsigned long> memStat = memStatResult.get();
  if (!memStat.contains("total_rss")) {
    LOG(ERROR) << "Did not find total_rss in memory.stat";
    return Option<ResourceStatistics>::none();
  }

  // Construct resource statistics.
  stat.timestamp = Clock::now();
  stat.utime = (double)cpuStat["user"] / (double)HZ;
  stat.stime = (double)cpuStat["system"] / (double)HZ;
  stat.rss = memStat["total_rss"];

  foreachpair (const std::string& key, unsigned long value,
               memStatResult.get()) {
    stat.miscAbsolute["mem_" + key] = value;
  }
  insertStats(hierarchy, getCgroupName(frameworkId, executorId),
      "blkio.time", "disk_time_", &stat.miscCounters);
  insertStats(hierarchy, getCgroupName(frameworkId, executorId),
      "blkio.io_serviced", "disk_serviced_", &stat.miscCounters);
  insertStats(hierarchy, getCgroupName(frameworkId, executorId),
      "blkio.io_service_bytes", "disk_bytes_", &stat.miscCounters);

  return stat;
}

launcher::ExecutorLauncher* CgroupsIsolationModule::createExecutorLauncher(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const std::string& directory)
{
  return new launcher::ExecutorLauncher(
      frameworkId,
      executorInfo.executor_id(),
      executorInfo.command(),
      frameworkInfo.user(),
      directory,
      slave,
      flags.frameworks_home,
      flags.hadoop_home,
      !local,
      flags.switch_user,
      "");
}


Try<bool> CgroupsIsolationModule::cpusChanged(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ResourceHints& resources)
{
  Resource r;
  r.set_name("cpus");
  r.set_type(Value::SCALAR);

  Option<Resource> cpusResource = resources.expectedResources.get(r);
  if (cpusResource.isNone()) {
    LOG(WARNING) << "Resource cpus cannot be retrieved for executor "
                 << executorId << " of framework " << frameworkId;
  } else if (flags.cgroup_enforce_cpu_limits) {
    double cpus = cpusResource.get().scalar().value();
    size_t cpuShares =
      std::max((size_t)(CPU_SHARES_PER_CPU * cpus), MIN_CPU_SHARES);

    Try<bool> set =
      cgroups::writeControl(hierarchy,
                            getCgroupName(frameworkId, executorId),
                            "cpu.shares",
                            stringify(cpuShares));
    if (set.isError()) {
      return Try<bool>::error(set.error());
    }

    LOG(INFO) << "Write cpu.shares = " << cpuShares
              << " for executor " << executorId
              << " of framework " << frameworkId;
  }

  return true;
}


Try<bool> CgroupsIsolationModule::memChanged(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ResourceHints& resources)
{
  Resource r;
  r.set_name("mem");
  r.set_type(Value::SCALAR);

  Option<Resource> memResource = resources.expectedResources.get(r);
  if (memResource.isNone()) {
    LOG(WARNING) << "Resource mem cannot be retrieved for executor "
                 << executorId << " of framework " << frameworkId;
  } else if (flags.cgroup_enforce_memory_limits) {
    double mem = memResource.get().scalar().value();
    size_t limitInBytes =
      std::max((size_t)mem, MIN_MEMORY_MB) * 1024LL * 1024LL;

    Try<bool> set =
      cgroups::writeControl(hierarchy,
                            getCgroupName(frameworkId, executorId),
                            "memory.limit_in_bytes",
                            stringify(limitInBytes));
    if (set.isError()) {
      return Try<bool>::error(set.error());
    }

    LOG(INFO) << "Write memory.limit_in_bytes = " << limitInBytes
              << " for executor " << executorId
              << " of framework " << frameworkId;

    if (flags.cgroup_enforce_swap_limits) {
      double memSwap = mem + flags.cgroup_swap_limit_extra;

      size_t limitInBytes =
        std::max((size_t)memSwap, MIN_MEMORY_MB) * 1024LL * 1024LL;

      Try<bool> set =
        cgroups::writeControl(hierarchy,
                              getCgroupName(frameworkId, executorId),
                              "memory.memsw.limit_in_bytes",
                              stringify(limitInBytes));
      if (set.isError()) {
        return Try<bool>::error(set.error());
      }

      LOG(INFO) << "Write memory.memsw.limit_in_bytes = " << limitInBytes
                << " for executor " << executorId
                << " of framework " << frameworkId;
    }
  }

  return true;
}


void CgroupsIsolationModule::oomListen(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  CHECK(info != NULL) << "Cgroup info is not registered";


  info->oomNotifier =
    cgroups::listenEvent(hierarchy,
                         getCgroupName(frameworkId, executorId),
                         "memory.oom_control");

  // If the listening fails immediately, something very wrong happened.
  // Therefore, we report a fatal error here.
  if (info->oomNotifier.isFailed()) {
    LOG(FATAL) << "Failed to listen for OOM events for executor " << executorId
               << " of framework " << frameworkId
               << ": "<< info->oomNotifier.failure();
  }

  LOG(INFO) << "Start listening for OOM events for executor " << executorId
            << " of framework " << frameworkId;

  info->oomNotifier.onAny(
      defer(PID<CgroupsIsolationModule>(this),
            &CgroupsIsolationModule::oomWaited,
            frameworkId,
            executorId,
            info->tag,
            info->oomNotifier));
}


void CgroupsIsolationModule::oomWaited(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const std::string& tag,
    const Future<uint64_t>& future)
{
  LOG(INFO) << "OOM notifier is triggered for executor "
            << executorId << " of framework " << frameworkId
            << " with tag " << tag;

  if (future.isDiscarded()) {
    LOG(INFO) << "Discarded OOM notifier for executor "
              << executorId << " of framework " << frameworkId
              << " with tag " << tag;
  } else if (future.isFailed()) {
    LOG(ERROR) << "Listening on OOM events failed for executor "
               << executorId << " of framework " << frameworkId
               << " with tag " << tag << ": " << future.failure();
  } else {
    // Out-of-memory event happened, call the handler.
    oom(frameworkId, executorId, tag);
  }
}


void CgroupsIsolationModule::oom(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const std::string& tag)
{
  LOG(INFO) << "OOM detected in executor " << executorId
            << " of framework " << frameworkId
            << " with tag " << tag;

  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  if (info == NULL) {
    // It is likely that processExited is executed before this function (e.g.
    // The kill and OOM events happen at the same time, and the process exit
    // event arrives first.) Therefore, we should not report a fatal error here.
    LOG(INFO) << "OOM detected for an exited executor";
    return;
  }

  // To safely ignore the OOM event from the previous launch of the same
  // executor (with the same frameworkId and executorId).
  if (tag != info->tag) {
    LOG(INFO) << "OOM detected for the previous launch of the same executor";
    return;
  }

  // If killed is set, the OOM notifier will be discarded in oomWaited.
  // Therefore, we should not be able to reach this point.
  CHECK(!info->killed) << "OOM detected for a killed executor";

  recentOoms[frameworkId].insert(executorId);

  // TODO(jieyu): Have a mechanism to use a different policy (e.g. freeze the
  // executor) when OOM happens.
  switch(oomPolicy) {
  case OOM_KILL:
    killExecutor(frameworkId, executorId);
    break;
  case OOM_KILL_PRIORITY:
    {
      // TODO(Charles): Make this asynchronous, move to linux/cgroups.cpp
      {
        Future<bool> freezerState =
          cgroups::freezeCgroup(hierarchy,
              getCgroupName(frameworkId, executorId));
        freezerState.await();
        if (freezerState.isFailed()) {
          LOG(ERROR) << "Freezing for OOM on " << frameworkId
            << ", " << executorId << ": " << freezerState.failure();
        }
      }
      Try<std::set<pid_t> > tasks = cgroups::getTasks(hierarchy,
          getCgroupName(frameworkId, executorId));
      if (tasks.isError()) {
        killExecutor(frameworkId, executorId);
      } else {
        int minNice = 20;
        std::set<pid_t> atMinNice;
        foreach (pid_t pid, tasks.get()) {
          Try<proc::ProcessStatistics> process = proc::stat(pid);
          if (process.isError()) {
            LOG(ERROR) << "Couldn't get statistics for " << pid;
            if (atMinNice.size() == 0) {
              // Make sure we kill something.
              atMinNice.insert(pid);
            }
            continue;
          }
          int nice = process.get().nice;
          if (nice < minNice) {
            minNice = nice;
            atMinNice.clear();
          }
          if (nice == minNice) {
            atMinNice.insert(pid);
          }
        }
        LOG(INFO) << "OOM killing " << atMinNice.size() << " processes of "
                  << tasks.get().size() << " processes in executor "
                  << executorId << " of framework " << frameworkId;
        foreach (pid_t pid, tasks.get()) {
          if (::kill(pid, SIGKILL) == -1) {
            LOG(ERROR) << "OOM-killing " << pid << ": " << strerror(errno);
          }
        }
      }
      // TODO(Charles): Move elsewhere?
      info->oomNotifier = cgroups::listenEvent(hierarchy,
          getCgroupName(frameworkId, executorId), "memory.oom_control");
      info->oomNotifier.onAny(
          defer(PID<CgroupsIsolationModule>(this),
                &CgroupsIsolationModule::oomWaited,
                frameworkId,
                executorId,
                info->tag,
                info->oomNotifier));
      {
        Future<bool> freezerState =
          cgroups::thawCgroup(hierarchy, getCgroupName(frameworkId, executorId));
        freezerState.await();
        if (freezerState.isFailed()) {
          LOG(ERROR) << "Thawing for OOM on " << frameworkId
            << ", " << executorId << ": " << freezerState.failure();
        }
      }
    }
  }
}


void CgroupsIsolationModule::destroyWaited(
    const std::string& cgroup,
    const Future<bool>& future)
{
  if (future.isReady()) {
    LOG(INFO) << "Successfully destroyed the cgroup " << cgroup;
  } else {
    LOG(FATAL) << "Failed to destroy the cgroup " << cgroup
               << ": " << future.failure();
  }
}


void CgroupsIsolationModule::setupOuterOom()
{
  outerOomNotifier = cgroups::listenEvent(hierarchy,
                                          flags.cgroup_outer_container_name,
                                          "memory.oom_control");

  outerOomNotifier.onAny(
      defer(PID<CgroupsIsolationModule>(this),
            &CgroupsIsolationModule::outerOomWaited,
            outerOomNotifier));
}


void CgroupsIsolationModule::outerOomWaited(
    const process::Future<uint64_t>& future)
{
  if (future.isDiscarded()) {
    LOG(INFO) << "Outer OOM discarded";
  } else if (future.isFailed()) {
    LOG(ERROR) << "While listening for OOM events on outer container: "
               << future.failure();
  } else {
    outerOom();
  }
}


CgroupsIsolationModule::CgroupInfo* CgroupsIsolationModule::registerCgroupInfo(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CgroupInfo* info = new CgroupInfo;
  info->frameworkId = frameworkId;
  info->executorId = executorId;
  info->tag = UUID::random().toString();
  info->pid = -1;
  info->killed = false;
  infos[frameworkId][executorId] = info;
  return info;
}


void CgroupsIsolationModule::unregisterCgroupInfo(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  if (infos.contains(frameworkId)) {
    if (infos[frameworkId].contains(executorId)) {
      delete infos[frameworkId][executorId];
      infos[frameworkId].erase(executorId);
      if (infos[frameworkId].empty()) {
        infos.erase(frameworkId);
      }
    }
  }
}


void CgroupsIsolationModule::outerOom()
{
  LOG(INFO) << "Out of memory on outer container";
  // Choose first victim for now.
  bool killedOne = false;
  typedef hashmap<ExecutorID, CgroupInfo*> ExecutorInfoMap;
  foreachpair (const FrameworkID& frameworkId,
               const ExecutorInfoMap& executorMap, infos) {
    foreachpair (const ExecutorID& executorId, CgroupInfo* info, executorMap) {
      if (info->killed) continue;
      killExecutor(frameworkId, executorId);
      killedOne = true;
      break;
    }
  }
  if (!killedOne) {
    LOG(ERROR) << "Outer OOM, but couldn't find anything to kill";
  }

  setupOuterOom();
}


Try<hashmap<std::string, unsigned long> > CgroupsIsolationModule::parseStat(
    const std::string& input)
{
  hashmap<std::string, unsigned long> stat;

  std::istringstream in(input);
  while (!in.eof()) {
    std::string line;
    std::getline(in, line);

    if (in.fail()) {
      if (!in.eof()) {
        return Try<hashmap<std::string, unsigned long> >::error(
            "Reading error");
      }
    } else {
      if (line.empty()) {
        // Skip empty lines.
        continue;
      } else {
        // Parse line.
        std::string name;
        unsigned long value;

        size_t split = line.rfind(' ');
        if (split == std::string::npos)
          continue;
        name = line.substr(0, split);

        foreach (char& c, name) {
          if (c == ' ')
            c = '_';
        }

        std::istringstream ss(line.substr(split + 1));
        ss >> std::dec >> value;

        if (ss.fail() && !ss.eof()) {
          return Try<hashmap<std::string, unsigned long> >::error(
              "Parsing error");
        }

        stat[name] = value;
      }
    }
  }

  return stat;
}


CgroupsIsolationModule::CgroupInfo* CgroupsIsolationModule::findCgroupInfo(
    pid_t pid)
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


CgroupsIsolationModule::CgroupInfo* CgroupsIsolationModule::findCgroupInfo(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  if (infos.find(frameworkId) != infos.end()) {
    if (infos[frameworkId].find(executorId) != infos[frameworkId].end()) {
      return infos[frameworkId][executorId];
    }
  }
  return NULL;
}


std::string CgroupsIsolationModule::getCgroupName(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  CHECK(info != NULL) << "Cgroup info is not registered";

  std::ostringstream out;
  if (flags.cgroup_outer_container) {
    out << flags.cgroup_outer_container_name << "/";
  }
  out << "mesos_cgroup_framework_" << frameworkId
      << "_executor_" << executorId
      << "_tag_" << info->tag;
  return out.str();
}


bool CgroupsIsolationModule::isValidCgroupName(const std::string& name)
{
  std::string trimmedName = strings::trim(name, "/");
  if (strings::startsWith(trimmedName, "mesos_cgroup_framework_") &&
      strings::contains(trimmedName, "_executor_") &&
      strings::contains(trimmedName, "_tag_")) {
    return true;
  } else {
    return false;
  }
}

} // namespace mesos {
} // namespace internal {
} // namespace slave {
