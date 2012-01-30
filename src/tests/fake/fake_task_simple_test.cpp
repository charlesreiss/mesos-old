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
          Resources::parse("mem:1024;cpu:4"),
          ResourceHints::parse("mem:4096;cpu:4", "mem:1280;cpu:2")));
  }

  scoped_ptr<ConstantTask> task;
};

TEST_F(ConstantTaskTest, TakeUsageSuccess)
{
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(1.0), seconds(5.0),
                            Resources::parse("mem:1024;cpu:4")));
  // If it doesn't get the CPU it wants, it still runs.
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(5.0), seconds(9.0),
                            Resources::parse("mem:1024;cpu:2")));
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(5.0), seconds(9.0),
                            Resources::parse("mem:1024;cpu:1")));
}

TEST_F(ConstantTaskTest, TakeUsageFailure)
{
  EXPECT_EQ(TASK_LOST,
            task->takeUsage(seconds(1.0), seconds(5.0),
                            Resources::parse("mem:768;cpu:4")));
}

class BatchTaskTest : public testing::Test
{
protected:
  void SetUp() {
    task.reset(new BatchTask(Resources::parse("mem:1024"),
                             ResourceHints::parse("mem:1280;cpu:2.0",
                                                  "mem:1280"),
                             30.0, 3.0));
  }

  scoped_ptr<BatchTask> task;
};

TEST_F(BatchTaskTest, GetUsageInitial)
{
  EXPECT_EQ(Resources::parse("cpu:3.0;mem:1024"),
            task->getUsage(seconds(1.0), seconds(2.0)));
  EXPECT_EQ(Resources::parse("cpu:3.0;mem:1024"),
            task->getUsage(seconds(1.0), seconds(11.0)));
  EXPECT_EQ(Resources::parse("cpu:1.0;mem:1024"),
            task->getUsage(seconds(1.0), seconds(31.0)));
}

TEST_F(BatchTaskTest, GetUsageAfterTakeUsage)
{
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(1.0), seconds(11.0),
                            Resources::parse("cpu:2.0;mem:1024")));
  EXPECT_EQ(Resources::parse("cpu:3.0;mem:1024"),
            task->getUsage(seconds(11.0), seconds(12.0)));
  EXPECT_EQ(Resources::parse("cpu:1.0;mem:1024"),
            task->getUsage(seconds(11.0), seconds(21.0)));
}

TEST_F(BatchTaskTest, TakeUsageNormal)
{
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(1.0), seconds(11.0),
                            Resources::parse("cpu:2.0;mem:1024")));
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(11.0), seconds(16.0),
                            Resources::parse("cpu:1.0;mem:1024")));
  EXPECT_EQ(TASK_FINISHED,
            task->takeUsage(seconds(16.0), seconds(18.5),
                            Resources::parse("cpu:2.0;mem:1024")));
}

TEST_F(BatchTaskTest, TakeUsageFailure)
{
  EXPECT_EQ(TASK_RUNNING,
            task->takeUsage(seconds(1.0), seconds(11.0),
                            Resources::parse("cpu:2.0;mem:1024")));
  EXPECT_EQ(Resources::parse("cpu:1.0;mem:1024"),
            task->getUsage(seconds(11.0), seconds(21.0)));
  EXPECT_EQ(TASK_LOST,
            task->takeUsage(seconds(11.0), seconds(12.0),
                            Resources::parse("cpu:3.0;mem:768")));
  EXPECT_EQ(Resources::parse("cpu:3.0;mem:1024"),
            task->getUsage(seconds(12.0), seconds(22.0)));
}
