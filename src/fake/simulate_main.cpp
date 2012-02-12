#include <algorithm>
#include <iostream>
#include <string>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/random/mersenne_twister.hpp"
#include "boost/random/exponential_distribution.hpp"

#include "process/process.hpp"

#include "common/logging.hpp"
#include "configurator/configurator.hpp"
#include "fake/scenario.hpp"
#include "fake/fake_task_simple.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;

void header(const Configuration& conf)
{
  foreachpair (const std::string& key, const std::string& value,
               conf.getMap()) {
    std::cout << "# " << key << "=" << value << "\n";
  }
  std::string batchCountsString = conf.get<std::string>("batch_counts", "");
  int numBatches =
      std::count(batchCountsString.begin(), batchCountsString.end(), ',') + 1;
  for (int i = 0; i < numBatches; ++i) {
    std::cout << "batch" << i << "_cpu,batch" << i << "_finish,";
  }
  std::cout << "total_time" << "\n";
}

static void run(const Configuration& conf, int seed_mix)
{
  process::Clock::pause();
  Scenario scenario(conf);
  scenario.spawnMaster();
  const int numSlaves = conf.get<int>("num_slaves", 1);
  const Resources resources(
      Resources::parse(conf.get<std::string>("slave_resources", "")));
  for (int i = 0; i < numSlaves; ++i) {
    scenario.spawnSlave(resources);
  }

  boost::random::mt19937 rng(conf.get<int>("seed", 42) + seed_mix);
  boost::random::exponential_distribution<> lengthDist(
      1.0 / conf.get<double>("batch_length", 30.0));
  std::vector<std::string> batchCountsStrings;
  std::string batchCountsCommaString =
      conf.get<std::string>("batch_counts", "");
  boost::algorithm::split(batchCountsStrings, batchCountsCommaString,
                          boost::algorithm::is_any_of(","));
  const ResourceHints batchRequest(
      ResourceHints::parse(conf.get<std::string>("batch_request", ""), ""));
  const Resources constResources(
      Resources::parse(conf.get<std::string>("batch_use_constant", "")));
  const double batchMaxCpus(conf.get<double>("batch_use_cpus", 2.0));
  std::vector<FakeScheduler*> batchSchedulers;
  std::vector<double> totalCpuTimes;
  foreach (std::string countString, batchCountsStrings) {
    int count = boost::lexical_cast<int>(countString);
    std::string name = "batch-" + boost::lexical_cast<std::string>(
        batchSchedulers.size());
    std::map<TaskID, FakeTask*> tasks;
    double curCpuTime = 0.0;
    for (int i = 0; i < count; ++i) {
      TaskID taskId;
      taskId.set_value("task-" + boost::lexical_cast<std::string>(i));
      double taskTime = lengthDist(rng);
      curCpuTime += taskTime;
      tasks[taskId] = new BatchTask(constResources, batchRequest,
                                    taskTime, batchMaxCpus);
      LOG(INFO) << "Created " << *tasks[taskId];
    }
    totalCpuTimes.push_back(curCpuTime);
    batchSchedulers.push_back(scenario.spawnScheduler(name, tasks));
  }

  double start = process::Clock::now();
  scenario.finishSetup();


  const double interval = conf.get<double>("fake_interval", 0.5);
  bool allDone;
  std::vector<bool> done(batchSchedulers.size(), false);
  std::vector<double> finishTime(batchSchedulers.size());
  do {
    scenario.runFor(interval);
    allDone = true;
    for (int i = 0; i < batchSchedulers.size(); ++i) {
      if (!done[i]) {
        FakeScheduler* scheduler = batchSchedulers[i];
        done[i] = (scheduler->countPending() + scheduler->countRunning()) == 0;
        if (done[i]) {
          finishTime[i] = process::Clock::now() - start;
        } else {
          allDone = false;
        }
      }
    }
  } while (!allDone);

  double end = process::Clock::now();
  scenario.stop();

  for (int i = 0; i < finishTime.size(); ++i) {
    std::cout << totalCpuTimes[i] << "," << finishTime[i] << ",";
  }
  std::cout << (end - start) << std::endl;

  process::Clock::resume();
}

int main(int argc, char **argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  Configurator configurator;
  Logging::registerOptions(&configurator);
  Master::registerOptions(&configurator);
  Scenario::registerOptions(&configurator);

  configurator.addOption<int>("seed", "Random seed");
  configurator.addOption<double>("batch_length",
                                 "Average batch task lengths");
  configurator.addOption<std::string>("batch_counts",
                                      "Number of batch tasks");
  configurator.addOption<double>("batch_use_cpus", 2.0, "max batch CPU usage");
  configurator.addOption<std::string>("batch_use_constant", "mem:8");
  configurator.addOption<std::string>("batch_request",
                                      "batch resource request",
                                      "cpus:1.0;mem:10");
  configurator.addOption<std::string>("slave_resources",
                                      "simulated slave resources (per slave)",
                                      "cpus:4.0;mem:40");
  configurator.addOption<int>("num_slaves", "number of simulated slaves", 1);
  configurator.addOption<int>("repeat", "number of reptitions", 1);

  if (argc == 2 && std::string("--help") == argv[1]) {
    std::cerr << "Usage: " << argv[0] << std::endl
              << configurator.getUsage();
    return EXIT_FAILURE;
  }

  Configuration conf;
  try {
    conf = configurator.load(argc, argv, true);
  } catch (ConfigurationException& e) {
    std::cerr << "Configuration error: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  Logging::init(argv[0], conf);

  process::initialize(false);

  header(conf);

  const int repeats = conf.get<int>("repeat", 1);
  for (int i = 0; i < repeats; ++i) {
    run(conf, i);
  }

  return EXIT_SUCCESS;
}
