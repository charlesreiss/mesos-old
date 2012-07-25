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

#ifndef __SLAVE_STATISTICS_HPP__
#define __SLAVE_STATISTICS_HPP__

#include <sys/types.h>

#include <string>

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/process.hpp>

#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/result.hpp>
#include <stout/try.hpp>

#include "common/type_utils.hpp"

namespace mesos {
namespace internal {
namespace slave {


// A snapshot of the resource usage statistics. This can be a snapshot for a
// process, or a snapshot for a group of processes (e.g. owned by a given
// executor of a given framework).
struct ResourceStatistics
{
  // The timestamp (seconds since epoch) when this snapshot is taken.
  double timestamp;

  // CPU related statistics.
  double utime; // Time spent in user mode, measured in seconds.
  double stime; // Time spent in kernel mode, measured in seconds.

  // Memory related statistics.
  long rss;   // Resident set size in bytes.

  // Miscellaenous statistics (XXX)
  hashmap<std::string, int64_t> miscCounters;
  hashmap<std::string, int64_t> miscAbsolute;

  void fillUsageMessage(const Option<ResourceStatistics>& prev,
                        UsageMessage* message) const;
};


// An interface class that defines the resource collectors. Resource statistics
// providers should implement this interface so that the resource usage monitor
// can collect statistics.
class ResourceStatisticsCollector
  : public process::Process<ResourceStatisticsCollector>
{
public:
  // Return the snapshot of the resource statistics for a given executor of a
  // given framework. Return none if the snapshot is not available.
  virtual Option<ResourceStatistics> collectResourceStatistics(
      const FrameworkID& frameworkId,
      const ExecutorID& executorId) = 0;
};


// The base class for resource usage meters. Each resource usage meter measures
// a specific type of resource usage (e.g. average CPU usage, maximum RSS).
class ResourceMeterBase
{
public:
  // Update the meter according to the given new snapshot.
  virtual void update(const ResourceStatistics& stat) = 0;
};


// Parameterized base class for resource usage meters. The parameter T is the
// type of the meter value (e.g. double, long, etc.).
template <typename T>
class ResourceMeter : public ResourceMeterBase
{
public:
  // Return the current value of the meter.
  virtual Option<T> value() const = 0;
};


// The base class for resource usage meter factories. Resource usage meters
// should be created through the corresponding factory.
class ResourceMeterFactoryBase
{
public:
  virtual ResourceMeterBase* CreateMeter() = 0;
};


// Parameterized class for resource usage meter factories.
template <class T>
class ResourceMeterFactory : public ResourceMeterFactoryBase
{
public:
  virtual ResourceMeterBase* CreateMeter() { return new T; }
};


// The process that is used to collect resource statistics and report resource
// usage for each executor in each framework.
class ResourceUsageProcess : public process::Process<ResourceUsageProcess>
{
public:
  explicit ResourceUsageProcess(ResourceStatisticsCollector* _collector)
    : collector(_collector) {}

  explicit ResourceUsageProcess(ResourceStatisticsCollector& _collector)
    : collector(&_collector) {}

  virtual ~ResourceUsageProcess() {}

  // Start watching a given resource usage meter of a given executor in a given
  // framework. Error will be returned if the given meter is not valid.
  Try<bool> watch(const FrameworkID& frameworkId,
                  const ExecutorID& executorId,
                  const std::string& name);

  // Start watching all registered resource usage meters of a given executor in
  // a given framework.
  Try<bool> watch(const FrameworkID& frameworkId,
                  const ExecutorID& executorId);

  // Stop watching a given resource usage meter of a given executor in a given
  // framework. Error will be returned if the given meter is not valid, or the
  // given executor is not currently being watched.
  Try<bool> unwatch(const FrameworkID& frameworkId,
                    const ExecutorID& executorId,
                    const std::string& name);

  // Stop watching all resource usage meters of a given executor in a given
  // framework.
  Try<bool> unwatch(const FrameworkID& frameworkId,
                    const ExecutorID& executorId);

  // Set the time interval (in secs) between two collection requests for a given
  // executor in a given framework.
  Try<bool> setInterval(const FrameworkID& frameworkId,
                        const ExecutorID& executorId,
                        double interval);

  // Return the current value of the meter for a given executor in a given
  // framework. None will be returned if the value is not currently available.
  // Error will be returned if the given meter is not valid, or the given
  // executor is not currently being watched.
  template <typename T>
  Result<T> get(const FrameworkID& frameworkId,
                const ExecutorID& executorId,
                const std::string& name)
  {
    ExecutorInfo* info = findExecutorInfo(frameworkId, executorId);
    if (info == NULL) {
      return Result<T>::error("The given executor is not currently watched");
    }

    if (!info->meters.contains(name)) {
      return Result<T>::error("Meter is not watched");
    }

    const ResourceMeter<T>* meter =
      dynamic_cast<const ResourceMeter<T>*>(info->meters[name]);
    if (meter == NULL) {
      return Result<T>::error("Meter type mismatch");
    }

    Option<T> value = meter->value();
    if (value.isNone()) {
      return Result<T>::none();
    } else {
      return value.get();
    }
  }

protected:
  virtual void initialize();

private:
  // Information about each monitored executor.
  struct ExecutorInfo {
    // Time interval (in secs) between two collection requests.
    double interval;
    // The resource usage meters to watch for.
    hashmap<std::string, ResourceMeterBase*> meters;
  };

  void collect(const FrameworkID& frameworkId,
               const ExecutorID& executorId);

  void received(const FrameworkID& frameworkId,
                const ExecutorID& executorId,
                const process::Future<Option<ResourceStatistics> >& future);

  ExecutorInfo* findExecutorInfo(const FrameworkID& frameworkId,
                                 const ExecutorID& executorId);

  ExecutorInfo* createExecutorInfo(const FrameworkID& frameworkId,
                                   const ExecutorID& executorId);

  void removeExecutorInfo(const FrameworkID& frameworkId,
                          const ExecutorID& executorId);

  // The statistics collector from which statistics are pulled.
  ResourceStatisticsCollector* collector;

  // Registered factories for resource usage meters.
  hashmap<std::string, ResourceMeterFactoryBase*> meterFactories;

  // Currently watched executors.
  hashmap<FrameworkID, hashmap<ExecutorID, ExecutorInfo*> > executorInfos;

  // The default interval (in secs) between two collection requests.
  static const double DEFAULT_INTERVAL = 1.0;
};


// Provide interfaces to get resource usage information for each executor
// running on the slave.
class ResourceUsage
{
public:
  explicit ResourceUsage(ResourceStatisticsCollector* collector)
  {
    process = new ResourceUsageProcess(collector);
    process::spawn(process);
  }

  explicit ResourceUsage(ResourceStatisticsCollector& collector)
  {
    process = new ResourceUsageProcess(collector);
    process::spawn(process);
  }

  ~ResourceUsage()
  {
    process::terminate(process);
    process::wait(process); // Necessary for disambiguation.
  }

  void watch(const FrameworkID& frameworkId,
             const ExecutorID& executorId)
  {
    process::dispatch(process,
                      &ResourceUsageProcess::watch,
                      frameworkId,
                      executorId);
  }

  void unwatch(const FrameworkID& frameworkId,
               const ExecutorID& executorId)
  {
    process::dispatch(process,
                      &ResourceUsageProcess::unwatch,
                      frameworkId,
                      executorId);
  }

  void setInterval(const FrameworkID& frameworkId,
                   const ExecutorID& executorId,
                   double interval)
  {
    process::dispatch(process,
                      &ResourceUsageProcess::setInterval,
                      frameworkId,
                      executorId,
                      interval);
  }

  template <typename T>
  process::Future<Result<T> > get(const FrameworkID& frameworkId,
                                  const ExecutorID& executorId,
                                  const std::string& name)
  {
    return process::dispatch(process,
                             &ResourceUsageProcess::get<T>,
                             frameworkId,
                             executorId,
                             name);
  }

private:
  // The process that is used to collect resource usage.
  ResourceUsageProcess *process;
};


// Collect statistics from a given process specified by the pid.
Try<ResourceStatistics> collect(pid_t pid);

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __SLAVE_STATISTICS_HPP__
