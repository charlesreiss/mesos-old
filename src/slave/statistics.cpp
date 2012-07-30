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

#include <unistd.h>

#include <sys/param.h>

#include <glog/logging.h>

#include <process/clock.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>

#include <stout/foreach.hpp>

#ifdef __linux__
#include "linux/proc.hpp"
#endif

#include "slave/statistics.hpp"

using namespace process;

namespace mesos {
namespace internal {
namespace slave {


void ResourceStatistics::fillUsageMessage(
    const Option<ResourceStatistics>& prev,
    UsageMessage* message) const
{
  double swap = 0;
  if (miscAbsolute.find("mem_swap") != miscAbsolute.end()) {
    swap = double(miscAbsolute.find("mem_swap")->second) / 1024. / 1024.;
  }
  message->set_timestamp(timestamp);
  mesos::Resource* mem = message->add_resources();
  mem->set_name("mem");
  mem->set_type(Value::SCALAR);
  mem->mutable_scalar()->set_value(double(rss) / 1024. / 1024. + swap);
  if (prev.isSome()) {
    double duration = timestamp - prev.get().timestamp;
    message->set_duration(duration);
    mesos::Resource cpu;
    cpu.set_name("cpus");
    double prevCpu = prev.get().utime + prev.get().stime;
    double curCpu = utime + stime;
    cpu.mutable_scalar()->set_value((curCpu - prevCpu) / duration);
    cpu.set_type(Value::SCALAR);
    message->add_resources()->MergeFrom(cpu);
    foreachpair (const std::string& name, int64_t value, miscCounters) {
      if (prev.get().miscCounters.count(name) == 0) continue;
      mesos::Resource* pseudo = message->add_pseudo_resources();
      pseudo->set_name(name);
      pseudo->set_type(Value::SCALAR);
      pseudo->mutable_scalar()->set_value(
          double(value - prev.get().miscCounters[name]) / duration);
    }
  }
  foreachpair (const std::string& name, int64_t value, miscAbsolute) {
    mesos::Resource* pseudo = message->add_pseudo_resources();
    pseudo->set_name(name);
    pseudo->set_type(Value::SCALAR);
    pseudo->mutable_scalar()->set_value(value);
  }
  mesos::Resource* cpu_raw = message->add_pseudo_resources();
  cpu_raw->set_name("cpu_user_ctr");
  cpu_raw->set_type(Value::SCALAR);
  cpu_raw->mutable_scalar()->set_value(utime);
  cpu_raw = message->add_pseudo_resources();
  cpu_raw->set_name("cpu_system_ctr");
  cpu_raw->set_type(Value::SCALAR);
  cpu_raw->mutable_scalar()->set_value(stime);
}



// Resource usage meter for the average CPU usage (user + kernel) in percentage.
class AvgCpuUsageMeter : public ResourceMeter<double>
{
public:
  AvgCpuUsageMeter() : timestamp(-1), utime(0), stime(0), avg(-1) {}
  virtual ~AvgCpuUsageMeter() {}

  virtual void update(const ResourceStatistics& stat)
  {
    if (timestamp < 0) {
      timestamp = stat.timestamp;
      utime = stat.utime;
      stime = stat.stime;
    } else {
      // Recalculate the average usage.
      if (stat.timestamp - timestamp <= 0) {
        avg = -1;
      } else {
        avg = (stat.utime + stat.stime - utime - stime) /
          (stat.timestamp - timestamp);
      }
    }
  }

  virtual Option<double> value() const
  {
    if (avg < 0) {
      return Option<double>::none();
    } else {
      return avg;
    }
  }

protected:
  double timestamp; // Timestamp of the first snapshot.
  double utime;     // User mode time spent of the first snapshot.
  double stime;     // Kernel mode time spent of of the first snapshot.
  double avg;
};


// Resource usage meter for maximum resident size (in bytes).
class MaxRSSMeter : public ResourceMeter<long>
{
public:
  MaxRSSMeter() : maxRSS(-1) {}
  virtual ~MaxRSSMeter() {}

  virtual void update(const ResourceStatistics& stat)
  {
    if (stat.rss > maxRSS) {
      maxRSS = stat.rss;
    }
  }

  virtual Option<long> value() const
  {
    if (maxRSS == -1) {
      return Option<long>::none();
    } else {
      return maxRSS;
    }
  }

protected:
  long maxRSS;
};


void ResourceUsageProcess::initialize()
{
  // Register meter factories.
  meterFactories["AvgCpuUsage"] =
    new ResourceMeterFactory<AvgCpuUsageMeter>();
  meterFactories["MaxRSS"] =
    new ResourceMeterFactory<MaxRSSMeter>();
}


Try<bool> ResourceUsageProcess::watch(const FrameworkID& frameworkId,
                                      const ExecutorID& executorId,
                                      const std::string& name)
{
  if (!meterFactories.contains(name)) {
    return Try<bool>::error("Meter does not exist");
  }

  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info == NULL) {
    info = createExecutorInfo(frameworkId, executorId);
    delay(info->interval,
          self(),
          &ResourceUsageProcess::collect,
          frameworkId,
          executorId);
  }

  info->meters[name] = meterFactories[name]->CreateMeter();
  return true;
}


Try<bool> ResourceUsageProcess::watch(const FrameworkID& frameworkId,
                                      const ExecutorID& executorId)
{
  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info == NULL) {
    info = createExecutorInfo(frameworkId, executorId);
    delay(info->interval,
          self(),
          &ResourceUsageProcess::collect,
          frameworkId,
          executorId);
  }

  foreachpair (const std::string& name,
               ResourceMeterFactoryBase* factory,
               meterFactories) {
    info->meters[name] = factory->CreateMeter();
  }

  return true;
}


Try<bool> ResourceUsageProcess::unwatch(const FrameworkID& frameworkId,
                                        const ExecutorID& executorId,
                                        const std::string& name)
{
  if (!meterFactories.contains(name)) {
    return Try<bool>::error("Meter does not exist");
  }

  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info == NULL) {
    return Try<bool>::error("The given executor is not currently watched.");
  }

  if (!info->meters.contains(name)) {
    return Try<bool>::error("Meter is not watched");
  }

  delete info->meters[name];
  info->meters.erase(name);
  if (info->meters.empty()) {
    removeExecutorInfo(frameworkId, executorId);
  }

  return true;
}


Try<bool> ResourceUsageProcess::unwatch(const FrameworkID& frameworkId,
                                        const ExecutorID& executorId)
{
  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info == NULL) {
    return Try<bool>::error("The given executor is not currently watched.");
  }

  removeExecutorInfo(frameworkId, executorId);
  return true;
}


Try<bool> ResourceUsageProcess::setInterval(const FrameworkID& frameworkId,
                                            const ExecutorID& executorId,
                                            double interval)
{
  if (interval < 0) {
    return Try<bool>::error("Invalid interval value");
  }

  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info == NULL) {
    return Try<bool>::error("The given executor is not currently watched.");
  }

  info->interval = interval;
  return true;
}



void ResourceUsageProcess::collect(const FrameworkID& frameworkId,
                                   const ExecutorID& executorId)
{
  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info != NULL) {
    Future<Option<ResourceStatistics> > future =
      dispatch(collector,
               &ResourceStatisticsCollector::collectResourceStatistics,
               frameworkId,
               executorId);

    future.onAny(
        defer(self(),
              &ResourceUsageProcess::received,
              frameworkId,
              executorId,
              future));

    delay(info->interval,
          self(),
          &ResourceUsageProcess::collect,
          frameworkId,
          executorId);
  }
}


void ResourceUsageProcess::received(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Future<Option<ResourceStatistics> >& future)
{
  if (future.isReady() && future.get().isSome()) {
    ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
    if (info != NULL) {
      ResourceStatistics stat = future.get().get();

      foreachvalue (ResourceMeterBase* meter, info->meters) {
        meter->update(stat);
      }
    }
  }
}


ResourceUsageProcess::ExecutorInfo* ResourceUsageProcess::findExecutorInfo(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  if (executorInfos.contains(frameworkId)) {
    if (executorInfos[frameworkId].contains(executorId)) {
      return executorInfos[frameworkId][executorId];
    }
  }
  return NULL;
}


ResourceUsageProcess::ExecutorInfo* ResourceUsageProcess::createExecutorInfo(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  ExecutorInfo* info = new ExecutorInfo;
  info->interval = DEFAULT_INTERVAL;
  executorInfos[frameworkId][executorId] = info;
  return info;
}


void ResourceUsageProcess::removeExecutorInfo(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
  if (info != NULL) {
    foreachvalue (ResourceMeterBase* meter, info->meters) {
      delete meter;
    }
    delete info;

    executorInfos[frameworkId].erase(executorId);
    if (executorInfos[frameworkId].empty()) {
      executorInfos.erase(frameworkId);
    }
  }
}


Try<ResourceStatistics> collect(pid_t pid)
{
#ifdef __linux__
  Try<proc::ProcessStatistics> stat = proc::stat(pid);
  if (stat.isError()) {
    return Try<ResourceStatistics>::error(stat.error());
  }

  // TODO(jieyu): The HZ defined in the header may not reflect the actual HZ
  // used in the Linux kernel (e.g. headers are not updated while the kernel
  // is).  And the Linux kernel does not have an interface exposed (e.g.
  // syscall) to get the actual HZ currently used in the kernel. GNU procps uses
  // a heuristic to determine the actual HZ being used.
#ifndef HZ
#error "Hertz (CPU ticks per second) is not defined."
#endif

  ResourceStatistics rstat;
  rstat.timestamp = Clock::now();
  rstat.utime = (double)stat.get().utime / (double)HZ;
  rstat.stime = (double)stat.get().stime / (double)HZ;
  rstat.rss = stat.get().rss * ::getpagesize();
  return rstat;
#elif defined __APPLE__
  // TODO(jieyu): Find a way to implement it without relying on Mach interfaces
  // as those interfaces are subject to change (e.g. libproc.h).
  return Try<ResourceStatistics>::error("Unimplemented");
#else
  return Try<ResourceStatistics>::error("Unsupported platform");
#endif
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
