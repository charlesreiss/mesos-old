#include <gmock/gmock.h>
#include "boost/scoped_ptr.hpp"

#include "fake/fake_task_pattern.hpp"

using boost::scoped_ptr;

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;

using testing::Return;

namespace {
const double kClose = 1e-6;
}

TEST(PatternTest, ConstantPattern)
{
  std::vector<double> counts(1, 10.0);
  Pattern pattern(counts, seconds(30.0));
  EXPECT_NEAR(10.0 / 300.0,
              pattern.countDuring(seconds(9.0), seconds(9.1)), kClose);
  EXPECT_NEAR(10.0 / 15.0,
              pattern.countDuring(seconds(29.0), seconds(31.0)), kClose);
  EXPECT_NEAR(10.0 / 2.0,
              pattern.countDuring(seconds(25.0), seconds(40.0)), kClose);
}

TEST(PatternTest, TwoPattern)
{
  std::vector<double> counts;
  counts.push_back(10.0);
  counts.push_back(20.0);
  Pattern pattern(counts, seconds(30.0));
  EXPECT_NEAR(10.0 / 300.0, pattern.countDuring(seconds(9.0), seconds(9.1)),
              kClose);
  EXPECT_NEAR(10.0 / 30.0 + 20.0 / 30.0,
              pattern.countDuring(seconds(29.0), seconds(31.0)),
              kClose);
  EXPECT_NEAR(10.0 / 6.0 + 20.0 / 3.0,
              pattern.countDuring(seconds(25.0), seconds(40.0)),
              kClose);
  EXPECT_NEAR(20.0 / 6.0 + 10.0 / 3.0,
              pattern.countDuring(seconds(55.0), seconds(70.0)),
              kClose);
  EXPECT_NEAR(10.0, pattern.countDuring(seconds(120.0), seconds(150.0)),
              kClose);
  EXPECT_NEAR(60.0, pattern.countDuring(seconds(60.0), seconds(180.0)),
              kClose);
}

TEST(PatternTest, ThreePattern)
{
  std::vector<double> counts;
  counts.push_back(10.0);
  counts.push_back(20.0);
  counts.push_back(30.0);
  Pattern pattern(counts, seconds(30.0));
  EXPECT_NEAR(10.0 / 2.0 + 20.0 + 30.0 / 3.0,
              pattern.countDuring(seconds(15.0), seconds(70.0)), kClose);
}

class MockPattern : public GenericPattern {
public:
  MOCK_CONST_METHOD2(countDuring, double(seconds, seconds));
};

namespace {
const double kBaseTime(500.0);
const Resources kConstUsage(Resources::parse("mem:100;cpus:1.0"));
}

class PatternTaskTest : public testing::Test {
public:
  void start(double cpuPerUnit)
  {
    const ResourceHints request(
        Resources::parse("mem:500;cpus:50.0"),
        Resources::parse("mem:250;cpus:75.0"));
    pattern = new MockPattern;
    task.reset(new PatternTask(
          kConstUsage, request, pattern, cpuPerUnit, seconds(kBaseTime)));
    EXPECT_EQ(request, task->getResourceRequest());
  }

  void expectExtraUsage(double from, double to, const Resources& usage,
                        double amount)
  {
    EXPECT_CALL(*pattern, countDuring(seconds(from), seconds(to))).
      WillRepeatedly(Return(amount));
    EXPECT_EQ(usage + kConstUsage,
        task->getUsage(seconds(from + kBaseTime), seconds(to + kBaseTime)));
  }

  void expectConsume(TaskState result, double from, double to,
      const Resources& takeResources, double amount)
  {
    EXPECT_CALL(*pattern, countDuring(seconds(from), seconds(to))).
      WillRepeatedly(Return(amount));
    EXPECT_EQ(result,
        task->takeUsage(seconds(from + kBaseTime), seconds(to + kBaseTime),
          takeResources + kConstUsage));
  }

  MockPattern* pattern;  // owned by PatternTask
  boost::scoped_ptr<PatternTask> task;
};

TEST_F(PatternTaskTest, Start)
{
  start(2.0);
}

TEST_F(PatternTaskTest, NormalUsage)
{
  start(2.0);
  expectExtraUsage(1.0, 2.0, Resources::parse("cpus:50.0"), 25.0);
  expectExtraUsage(2.0, 3.0, Resources::parse("cpus:0.0"), 0.0);
  expectExtraUsage(3.0, 5.0, Resources::parse("cpus:5.0"), 5.0);
}

TEST_F(PatternTaskTest, ViolationsSimple)
{
  start(2.0);
  expectConsume(TASK_RUNNING, 1.0, 2.0, Resources::parse("cpus:40.0"), 25.0);
  EXPECT_DOUBLE_EQ(5.0, task->getViolations());
}

TEST_F(PatternTaskTest, ViolationsSimpleTwoSeconds)
{
  start(2.0);
  expectConsume(TASK_RUNNING, 1.0, 3.0, Resources::parse("cpus:40.0"), 50.0);
  EXPECT_DOUBLE_EQ(10.0, task->getViolations());
}

TEST_F(PatternTaskTest, ViolationsTwice)
{
  start(2.0);
  expectConsume(TASK_RUNNING, 1.0, 2.0, Resources::parse("cpus:40.0"), 25.0);
  expectConsume(TASK_RUNNING, 2.0, 3.0, Resources::parse("cpus:20.0"), 10.0);
  expectConsume(TASK_RUNNING, 3.0, 5.0, Resources::parse("cpus:20.0"), 40.0);
  EXPECT_DOUBLE_EQ(5.0 + 20.0, task->getViolations());
}

TEST_F(PatternTaskTest, MissConstantUsage)
{
  start(0.5);
  expectConsume(TASK_LOST, 1.0, 2.0, Resources::parse("mem:-10;cpus:1000.0"),
      25.0);
  EXPECT_DOUBLE_EQ(25.0, task->getViolations());
}
