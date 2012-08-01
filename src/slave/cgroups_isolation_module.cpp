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

#include <sys/param.h>
#include <sys/types.h>

#include <sstream>

#include <process/defer.hpp>
#include <process/dispatch.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#include "common/units.hpp"

#include "linux/cgroups.hpp"
#include "linux/proc.hpp"

#include "slave/cgroups_isolation_module.hpp"


// TODO(jieyu): The HZ defined in the header may not reflect the actual HZ used
// in the Linux kernel (e.g. headers are not updated while the kernel is).  And
// the Linux kernel does not have an interface exposed (e.g.  syscall) to get
// the actual HZ currently used in the kernel. GNU procps uses a heuristic to
// determine the actual HZ being used.
#ifndef HZ
#error "Hertz (CPU ticks per second) is not defined."
#endif

using namespace process;

namespace {

const int32_t CPU_SHARES_PER_CPU = 1024;
const int32_t MIN_CPU_SHARES = 10;
const int64_t MIN_MEMORY_MB = 32 * Megabyte;

} // namespace {


namespace mesos {
namespace internal {
namespace slave {


// The path to the default hierarchy root used by this module.
static const char* DEFAULT_HIERARCHY = "/cgroups";
// The default subsystems used by this module.
static const char* DEFAULT_SUBSYSTEMS = "blkio,cpu,cpuacct,freezer,memory";


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
  local = _local;
  slave = _slave;

  if (flags.cgroup_oom_policy == "kill") {
    oomPolicy = OOM_KILL;
  } else if (flags.cgroup_oom_policy == "kill-priority") {
    oomPolicy = OOM_KILL_PRIORITY;
  } else {
    LOG(FATAL) << "Bad OOM policy " << flags.cgroup_oom_policy;
  }

  // Check that we are root.
  if (os::user() != "root") {
    LOG(FATAL) << "Cgroups isolation modules requires slave to run as root";
  }

  // Check if cgroups module is available.
  if (!cgroups::enabled()) {
    LOG(FATAL) << "Kernel support for cgroups is not enabled";
  }

  // Check if the required subsystems are enabled.
  Try<bool> enabled = cgroups::enabled(subsystems());
  if (enabled.isError()) {
    LOG(FATAL) << enabled.error();
  } else if (!enabled.get()) {
    LOG(FATAL) << "Some required cgroups subsystems are not enabled";
  }

  // Prepare the cgroups hierarchy. Check if the required subsystems are busy.
  // If yes, check to see whether they are properly mounted at the given
  // location. If not, we try to create the hierarchy and mount the subsystems.
  Try<bool> busy = cgroups::busy(subsystems());
  if (busy.isError()) {
    LOG(FATAL) << busy.error();
  } else if (busy.get()) {
    if (!os::exists(hierarchy())) {
      LOG(FATAL) << "Some required cgroups subsystems are being used";
    } else {
      Try<bool> check = cgroups::checkHierarchy(hierarchy(), subsystems());
      if (check.isError()) {
        LOG(FATAL) << "The cgroups hierarchy is not valid: " << check.error();
      }
    }
  } else {
    if (os::exists(hierarchy())) {
      LOG(FATAL) << "Please remove the directory: " << hierarchy();
    } else {
      Try<bool> create = cgroups::createHierarchy(hierarchy(), subsystems());
      if (create.isError()) {
        LOG(FATAL) << create.error();
      }
    }
  }

  if (flags.cgroup_outer_container) {
    Try<bool> create =
      cgroups::createCgroup(hierarchy(), flags.cgroup_outer_container_name);
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
      cgroups::writeControl(hierarchy(),
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

  LOG(INFO) << "Launching " << executorId
            << " (" << executorInfo.command().value() << ")"
            << " in " << directory
            << " with resources " << resources
            << " for framework " << frameworkId;

  // Register the cgroup information.
  registerCgroupInfo(frameworkId, executorId);

  // Create a new cgroup for the executor.
  Try<bool> create =
    cgroups::createCgroup(hierarchy(), cgroup(frameworkId, executorId));
  if (create.isError()) {
    LOG(FATAL) << "Failed to create cgroup for executor " << executorId
               << " for framework " << frameworkId
               << ": " << create.error();
  }

  // Set resource controls for the cgroup.
  Try<bool> set = setCgroupControls(frameworkId,
                                    executorId,
                                    resources);
  if (set.isError()) {
    LOG(FATAL) << "Failed to set cgroup controls for executor " << executorId
               << " for framework " << frameworkId
               << " with resourcs " << resources
               << ": "<< set.error();
  }

  // Start listen on OOM events.
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
      cgroups::assignTask(hierarchy(),
                          cgroup(frameworkId, executorId),
                          ::getpid());
    if (assign.isError()) {
      LOG(FATAL) << "Failed to assign for executor " << executorId
                 << " for framework " << frameworkId
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
            << " for framework " << frameworkId;

  // Stop the OOM listener if needed.
  if (info->oomNotifier.isPending()) {
    info->oomNotifier.discard();
  }

  // Destroy the cgroup that is associated with the executor. Here, we don't
  // wait for it to succeed as we don't want to block the isolation module.
  // Instead, we register a callback which will be invoked when its result is
  // ready.
  Future<bool> future =
    cgroups::destroyCgroup(hierarchy(), cgroup(frameworkId, executorId));
  future.onAny(
      defer(PID<CgroupsIsolationModule>(this),
            &CgroupsIsolationModule::destroyWaited,
            frameworkId,
            executorId,
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

  Try<bool> set = setCgroupControls(frameworkId,
                                    executorId,
                                    resources);
  if (set.isError()) {
    LOG(ERROR) << "Failed to set cgroup controls for executor " << executorId
               << " for framework " << frameworkId
               << ": " << set.error();
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
    cgroups::readControl(hierarchy(),
                         cgroup(frameworkId, executorId),
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
    cgroups::readControl(hierarchy(),
                         cgroup(frameworkId, executorId),
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
  insertStats(hierarchy(), cgroup(frameworkId, executorId),
      "blkio.time", "disk_time_", &stat.miscCounters);
  insertStats(hierarchy(), cgroup(frameworkId, executorId),
      "blkio.io_serviced", "disk_serviced_", &stat.miscCounters);
  insertStats(hierarchy(), cgroup(frameworkId, executorId),
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


std::string CgroupsIsolationModule::subsystems()
{
  return DEFAULT_SUBSYSTEMS;
}


std::string CgroupsIsolationModule::hierarchy()
{
  return flags.cgroup_hierarchy;
}


std::string CgroupsIsolationModule::cgroup(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  std::ostringstream ss;
  if (flags.cgroup_outer_container) {
    ss << flags.cgroup_outer_container_name << '/';
  }
  ss << "mesos_cgroup_executor_" << executorId << "_framework_"
     << frameworkId;
  return ss.str();
}


Try<bool> CgroupsIsolationModule::setCgroupControls(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ResourceHints& resources)
{
  LOG(INFO) << "Changing cgroup controls for executor " << executorId
            << " of framework " << frameworkId
            << " with resources " << resources;

  // Setup cpu control.
  if (flags.cgroup_enforce_cpu_limits) {
    double cpu = resources.expectedResources.get("cpu", Value::Scalar()).value();
    int32_t cpuShares =
      std::max(CPU_SHARES_PER_CPU * (int32_t)cpu, MIN_CPU_SHARES);
    Try<bool> setCpuResult =
      cgroups::writeControl(hierarchy(),
                            cgroup(frameworkId, executorId),
                            "cpu.shares",
                            stringify(cpuShares));
    if (setCpuResult.isError()) {
      return Try<bool>::error(setCpuResult.error());
    }

    LOG(INFO) << "Write cpu.shares = " << cpuShares
              << " for executor " << executorId
              << " of framework " << frameworkId;
  }

  // Setup memory control.
  if (flags.cgroup_enforce_memory_limits) {
    double mem = resources.expectedResources.get("mem", Value::Scalar()).value();
    int64_t limitInBytes =
      std::max((int64_t)mem, MIN_MEMORY_MB) * 1024LL * 1024LL;
    Try<bool> setMemResult =
      cgroups::writeControl(hierarchy(),
                            cgroup(frameworkId, executorId),
                            "memory.limit_in_bytes",
                            stringify(limitInBytes));
    if (setMemResult.isError()) {
      return Try<bool>::error(setMemResult.error());
    }

    LOG(INFO) << "Write memory.limit_in_bytes = " << limitInBytes
              << " for executor " << executorId
              << " of framework " << frameworkId;
  }

  return true;
}


void CgroupsIsolationModule::oomListen(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  CHECK(info != NULL) << "Cgroup info is not registered";

  info->oomNotifier = cgroups::listenEvent(hierarchy(),
                                           cgroup(frameworkId, executorId),
                                           "memory.oom_control");

  // If the listening fails immediately, something very wrong happens.
  // Therefore, we report a fatal error here.
  if (info->oomNotifier.isFailed()) {
    LOG(FATAL) << "Failed to listen OOM events for executor " << executorId
               << " for framework " << frameworkId
               << ": "<< info->oomNotifier.failure();
  }

  LOG(INFO) << "Start listening OOM events for executor " << executorId
            << " for framework " << frameworkId;

  info->oomNotifier.onAny(
      defer(PID<CgroupsIsolationModule>(this),
            &CgroupsIsolationModule::oomWaited,
            frameworkId,
            executorId,
            info->oomNotifier));
}


void CgroupsIsolationModule::oomWaited(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Future<uint64_t>& future)
{
  LOG(INFO) << "OOM notifier is triggered for executor " << executorId
            << " of framework " << frameworkId;

  if (future.isDiscarded()) {
    LOG(INFO) << "Discarded OOM notifier for executor " << executorId
              << " of framework " << frameworkId;
  } else if (future.isFailed()) {
    LOG(ERROR) << "Listening on OOM events failed for executor " << executorId
               << " of framework " << frameworkId
               << ": " << future.failure();
  } else {
    // Out-of-memory event happened, call the handler.
    oom(frameworkId, executorId);
  }
}


void CgroupsIsolationModule::oom(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  LOG(INFO) << "OOM detected in executor " << executorId
            << " of framework " << frameworkId;

  recentOoms[frameworkId].insert(executorId);

  CgroupInfo* info = findCgroupInfo(frameworkId, executorId);
  CHECK(info != NULL && !info->killed)
    << "OOM detected for an unknown/killed executor";

  switch(oomPolicy) {
  case OOM_KILL:
    killExecutor(frameworkId, executorId);
    break;
  case OOM_KILL_PRIORITY:
    {
      // TODO(Charles): Make this asynchronous, move to linux/cgroups.cpp
      {
        Future<std::string> freezerState =
          cgroups::freezeCgroup(hierarchy(), cgroup(frameworkId, executorId));
        freezerState.await();
        if (freezerState.isFailed()) {
          LOG(ERROR) << "Freezing for OOM on " << frameworkId
            << ", " << executorId << ": " << freezerState.failure();
        }
      }
      Try<std::set<pid_t> > tasks = cgroups::getTasks(hierarchy(),
          cgroup(frameworkId, executorId));
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
      info->oomNotifier = cgroups::listenEvent(hierarchy(),
          cgroup(frameworkId, executorId), "memory.oom_control");
      info->oomNotifier.onAny(
          defer(PID<CgroupsIsolationModule>(this),
                &CgroupsIsolationModule::oomWaited,
                frameworkId,
                executorId,
                info->oomNotifier));
      {
        Future<std::string> freezerState =
          cgroups::thawCgroup(hierarchy(), cgroup(frameworkId, executorId));
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
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Future<bool>& future)
{
  LOG(INFO) << "Destroy result is available for executor " << executorId
            << " of framework " << frameworkId;

  if (!future.isReady()) {
    LOG(ERROR) << "Failed to destroy the cgroup for executor " << executorId
               << " of framework " << frameworkId
               << ": " << future.failure();
  }
}


void CgroupsIsolationModule::setupOuterOom()
{
  outerOomNotifier = cgroups::listenEvent(hierarchy(),
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


} // namespace mesos {
} // namespace internal {
} // namespace slave {
