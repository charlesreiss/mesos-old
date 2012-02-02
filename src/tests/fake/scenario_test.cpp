#include <gmock/gmock.h>

#include "fake/scenario.hpp"
#include "fake/fake_task_simple.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;

TEST(FakeScenarioTest, SampleBatch) {
  process::Clock::pause();
  Scenario scenario;
  scenario.spawnMaster();
  for (int i = 0; i < 4; ++i) {
    scenario.spawnSlave(Resources::parse("cpu:4;mem:1024"));
  }
  std::map<TaskID, FakeTask*> tasks;
  for (int i = 0; i < 16; ++i) {
    TaskID id;
    id.set_value(std::string("task") + boost::lexical_cast<std::string>(i));
    tasks[id] = new BatchTask(Resources::parse("mem:512"),
                              ResourceHints::parse("mem:512;cpu:1",
                                                   "mem:512;cpu:1"),
                              30.0, 3.0);
  }
  scenario.spawnScheduler("batch", tasks);
  scenario.finishSetup();
  EXPECT_EQ(16, scenario.getScheduler("batch")->countPending());
  scenario.runFor(16.0 * 30.0 / 4.0 + 0.1);
  EXPECT_EQ(0, scenario.getScheduler("batch")->countPending());
  EXPECT_EQ(0, scenario.getScheduler("batch")->countRunning());
  scenario.stop();
  process::Clock::resume();
}
