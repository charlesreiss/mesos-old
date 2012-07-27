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

#include <gmock/gmock.h>

#include <stout/stringify.hpp>

#include <process/clock.hpp>

#include "slave/statistics.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::slave;

using namespace process;


class MockCollector : public ResourceStatisticsCollector
{
public:
  MockCollector()
  {
    clock = 0;
    frameworkId.set_value("framework");
    executorId.set_value("executor");
  }

  virtual ~MockCollector() {}

  virtual Option<ResourceStatistics> collectResourceStatistics(
      const FrameworkID& frameworkId,
      const ExecutorID& executorId)
  {
    return generateStatistics();
  }

  ResourceStatistics generateStatistics()
  {
    int currClock = ++clock;

    ResourceStatistics stat;
    stat.timestamp = 1.0 * currClock;
    stat.utime = 0.2 * currClock;
    stat.stime = 0.1 * currClock;
    stat.rss = 1024;

    return stat;
  }

  const FrameworkID& getFrameworkId() { return frameworkId; }
  const ExecutorID& getExecutorId() { return executorId; }

private:
  int clock;
  FrameworkID frameworkId;
  ExecutorID executorId;
};


TEST(StatisticsTest, collect)
{
  Try<ResourceStatistics> statStart = collect(::getpid());
  ASSERT_TRUE(statStart.isSome());

  double start = Clock::now();

  // Use CPU time by using a loop. (Here, just for an estimate, assuming a 2GHz
  // CPU, this loop should take about 4 second if CPI is 1 and each iteration
  // takes 1 instruction.)
  static long dummy = 1; // Heuristic to avoid dead code elimination.
  for (long i = 0; i < 8000000000; i++) {
    dummy += i;
  }

  double end = Clock::now();

  Try<ResourceStatistics> statEnd = collect(::getpid());
  ASSERT_TRUE(statEnd.isSome());

  EXPECT_NEAR(end - start, statEnd.get().utime - statStart.get().utime, 0.1);
}


TEST(StatisticsTest, ResourceUsage)
{
  MockCollector collector;
  spawn(collector);

  FrameworkID frameworkId = collector.getFrameworkId();
  ExecutorID executorId = collector.getExecutorId();

  ResourceUsage usage(collector);
  usage.watch(frameworkId, executorId);
  usage.setInterval(frameworkId, executorId, 0.1);

  Future<Result<double> > avgCpuUsage;
  Future<Result<long> > maxRSS;

  ::sleep(2.0);

  avgCpuUsage = usage.get<double>(frameworkId, executorId, "AvgCpuUsage");
  avgCpuUsage.await(0.1);
  ASSERT_TRUE(avgCpuUsage.isReady());
  ASSERT_TRUE(avgCpuUsage.get().isSome());
  EXPECT_DOUBLE_EQ(0.3, avgCpuUsage.get().get());

  maxRSS = usage.get<long>(frameworkId, executorId, "MaxRSS");
  maxRSS.await(0.1);
  ASSERT_TRUE(maxRSS.isReady());
  ASSERT_TRUE(maxRSS.get().isSome());
  EXPECT_EQ(1024, maxRSS.get().get());

  terminate(collector);
  wait(collector);
}

TEST(StatisticsTest, FillUsageMessage)
{
  ResourceStatistics stat1;
  ResourceStatistics stat2;

  stat1.timestamp = 5.0;
  stat2.timestamp = 7.0;

  stat1.utime = 4.0;
  stat1.stime = 3.0;
  // total 7
  stat2.utime = 5.0;
  stat2.stime = 6.0;
  // total 11

  stat1.rss = 1024L * 1024L;
  stat2.rss = 1024L * 1024L;

  stat1.miscCounters["counter"] = 42;
  stat2.miscCounters["counter"] = 44;
  stat1.miscAbsolute["absolute"] = 44;
  stat2.miscAbsolute["absolute"] = 46;

  UsageMessage message;
  stat2.fillUsageMessage(Option<ResourceStatistics>::none(), &message);

  Resources resources = message.resources();
  EXPECT_DOUBLE_EQ(1.0, resources.get("mem", Value::Scalar()).value());
  EXPECT_EQ(1, resources.size());
  Resources pseudoResources = message.pseudo_resources();
  EXPECT_DOUBLE_EQ(46.0, pseudoResources.get("absolute", Value::Scalar()).value());
  foreach (const Resource& resource, pseudoResources) {
    EXPECT_NE("counter", resource.name());
  }

  message.Clear();

  stat2.fillUsageMessage(stat1, &message);

  resources = message.resources();
  EXPECT_DOUBLE_EQ(1.0, resources.get("mem", Value::Scalar()).value());
  EXPECT_DOUBLE_EQ(2.0, resources.get("cpus", Value::Scalar()).value());
  EXPECT_EQ(2, resources.size());

  pseudoResources = message.pseudo_resources();
  EXPECT_DOUBLE_EQ(46.0, pseudoResources.get("absolute", Value::Scalar()).value());
  EXPECT_DOUBLE_EQ(1.0, pseudoResources.get("counter", Value::Scalar()).value());
}
