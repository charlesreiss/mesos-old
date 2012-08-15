#include <gmock/gmock.h>
#include "boost/scoped_ptr.hpp"

#include "fake/fake_task_simple.hpp"

using boost::scoped_ptr;

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;

class ConstantTaskTest : public testing::Test
{
protected:
  void SetUp()
  {
    task.reset(new ConstantTask(
          Resources::parse("mem:1024;cpus:4"),
          ResourceHints::parse("mem:4096;cpus:4", "mem:1280;cpus:2")));
  }

  scoped_ptr<ConstantTask> task;
};

TEST_F(ConstantTaskTest, TakeUsageSuccess)
{
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:16")),
            task->takeUsage(seconds(1.0), seconds(5.0),
                            Resources::parse("mem:1024;cpus:4")));
  // If it doesn't get the CPU it wants, it still runs.
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:8")),
            task->takeUsage(seconds(5.0), seconds(9.0),
                            Resources::parse("mem:1024;cpus:2")));
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:4")),
            task->takeUsage(seconds(5.0), seconds(9.0),
                            Resources::parse("mem:1024;cpus:1")));
}

TEST_F(ConstantTaskTest, TakeUsageFailure)
{
  EXPECT_EQ(UsageInfo(TASK_LOST, Resources::parse("")),
            task->takeUsage(seconds(1.0), seconds(5.0),
                            Resources::parse("mem:768;cpus:4")));
}

class BatchTaskTest : public testing::Test
{
protected:
  void SetUp() {
    task.reset(new BatchTask(Resources::parse("mem:1024"),
                             ResourceHints::parse("mem:1280;cpus:2.0",
                                                  "mem:1280"),
                             30.0, 3.0));
  }

  scoped_ptr<BatchTask> task;
};

TEST_F(BatchTaskTest, GetUsageInitial)
{
  EXPECT_EQ(Resources::parse("cpus:3.0;mem:1024"),
            task->getUsage(seconds(1.0), seconds(2.0)));
  EXPECT_EQ(Resources::parse("cpus:3.0;mem:1024"),
            task->getUsage(seconds(1.0), seconds(11.0)));
  EXPECT_EQ(Resources::parse("cpus:1.0;mem:1024"),
            task->getUsage(seconds(1.0), seconds(31.0)));
}

TEST_F(BatchTaskTest, GetUsageAfterTakeUsage)
{
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:20.0")),
            task->takeUsage(seconds(1.0), seconds(11.0),
                            Resources::parse("cpus:2.0;mem:1024")));
  EXPECT_EQ(Resources::parse("cpus:3.0;mem:1024"),
            task->getUsage(seconds(11.0), seconds(12.0)));
  EXPECT_EQ(Resources::parse("cpus:1.0;mem:1024"),
            task->getUsage(seconds(11.0), seconds(21.0)));
}

TEST_F(BatchTaskTest, TakeUsageNormal)
{
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:20.0")),
            task->takeUsage(seconds(1.0), seconds(11.0),
                            Resources::parse("cpus:2.0;mem:1024")));
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:5.0")),
            task->takeUsage(seconds(11.0), seconds(16.0),
                            Resources::parse("cpus:1.0;mem:1024")));
  EXPECT_EQ(UsageInfo(TASK_FINISHED, Resources::parse("cpuTime:5.0")),
            task->takeUsage(seconds(16.0), seconds(18.5),
                            Resources::parse("cpus:2.0;mem:1024")));
}

TEST_F(BatchTaskTest, TakeUsageFailure)
{
  EXPECT_EQ(UsageInfo(TASK_RUNNING, Resources::parse("cpuTime:20.0")),
            task->takeUsage(seconds(1.0), seconds(11.0),
                            Resources::parse("cpus:2.0;mem:1024")));
  EXPECT_EQ(Resources::parse("cpus:1.0;mem:1024"),
            task->getUsage(seconds(11.0), seconds(21.0)));
  EXPECT_EQ(UsageInfo(TASK_LOST, Resources::parse("")),
            task->takeUsage(seconds(11.0), seconds(12.0),
                            Resources::parse("cpus:3.0;mem:768")));
  EXPECT_EQ(Resources::parse("cpus:3.0;mem:1024"),
            task->getUsage(seconds(12.0), seconds(22.0)));
}
