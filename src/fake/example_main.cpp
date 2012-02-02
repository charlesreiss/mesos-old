#include <glog/logging.h>

#include "fake/fake_scenario.hpp"
#include "fake/fake_task_simple.hpp"
#include "master/master.hpp"
#include "slave/slave.hpp"

#include <map>

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;

int main(int argc, char **argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  google::InitGoogleLogging("fake_example");
  process::intiialize(false);

  process::Clock::pause();
  Scenario scenario;
  scenario.spawnMaster();
  for (int i = 0; i < 4; ++i) {
    scenario.spawnSlave(Resources::parse("cpu:4;mem:1024"));
  }
  scenario.finishSetup();
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
  // TODO(Charles Reiss): convert to test case about finishing on time.
  for (int i = 0; i < 16 * 30; ++i) {
    LOG(ERROR) << scenario.getScheduler("batch")->countPending() << " pending "
               << " at " << process::Clock(now);
    scenario.runFor(1.0);
  }
  scenario.stop();
  process::Clock::resume();

  return EXIT_SUCCESS;
}
