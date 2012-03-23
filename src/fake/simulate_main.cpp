#include <algorithm>
#include <iostream>
#include <fstream>
#include <numeric>
#include <string>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/random/mersenne_twister.hpp"
#include "boost/random/exponential_distribution.hpp"
#include "boost/scoped_ptr.hpp"

#include "process/process.hpp"

#include "common/logging.hpp"
#include "configurator/configurator.hpp"
#include "fake/scenario.hpp"
#include "fake/fake_task_simple.hpp"
#include "usage_log/usage_log.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::fake;

using mesos::internal::usage_log::UsageRecorder;
using mesos::internal::usage_log::TextFileUsageLogWriter;

static void headerForBatch(const std::string& name)
{
  std::cout << name << "_cpu," << name << "_finish_time,"
            << name << "_start_time," << name << "_duration,"
            << name << "_finished,"
            << name << "_lost," << name << "_failed,"
            << name << "_kill," << name << "_score,";
}

void headerFromScenario(const Configuration& conf, Scenario* scenario)
{
  if (scenario->getLabelColumns() != "") {
    std::cout << scenario->getLabelColumns() << ',';
  } else {
    std::cout << "run_id,";
  }
  foreachkey (const std::string& name, scenario->getSchedulers()) {
    headerForBatch(name);
  }
  std::cout << "all_total_time,all_finish_time\n";
}

static void run(const Configuration& conf, bool needHeader,
    const std::string& id, Scenario* scenario)
{
  if (needHeader) {
    headerFromScenario(conf, scenario);
    std::cout << std::flush;
  }
  double start = process::Clock::now();
  const double interval = conf.get<double>("fake_interval", 0.5);
  const double runFor = conf.get<double>("run_for", 0.0);
  bool allDone;
  const std::map<std::string, FakeScheduler*>& schedulersMap =
    scenario->getSchedulers();
  std::vector<FakeScheduler*> schedulers;
  std::vector<double> totalCpuTimes;
  foreachvalue (FakeScheduler* scheduler, schedulersMap) {
    schedulers.push_back(scheduler);
    totalCpuTimes.push_back(
        scheduler->getAttributes().get("total_time", Value::Scalar()).value());
  }
  std::vector<bool> done(schedulers.size(), false);
  std::vector<double> finishTime(schedulers.size());
  do {
    scenario->runFor(interval);
    allDone = true;
    for (int i = 0; i < schedulers.size(); ++i) {
      if (!done[i]) {
        FakeScheduler* scheduler = schedulers[i];
        if (scheduler->getAttributes().get("type", Value::Text()).value()
            == "serve") {
          done[i] = true;
        } else {
          done[i] = (scheduler->countPending() + scheduler->countRunning()) == 0;
        }
        if (done[i]) {
          finishTime[i] = process::Clock::now() - start;
          scenario->stopScheduler(scheduler->getAttributes().get("name",
                Value::Text()).value());
        } else {
          LOG(INFO) << "scheduler " << i << " (" <<
            scheduler->getAttributes().get("name", Value::Text()).value()
            << ") not done at " << (process::Clock::now() - start);
          allDone = false;
        }
      }
    }
    if (process::Clock::now() <= start + runFor) {
      allDone = false;
    }
  } while (!allDone);

  double end = process::Clock::now();

  if (scenario->getLabel() != "") {
    std::cout << scenario->getLabel() << ',';
  } else {
    std::cout << id << ",";
  }

  for (int i = 0; i < finishTime.size(); ++i) {
    double curFinish = finishTime[i];
    double curStart = schedulers[i]->getStartTime(start);
    std::cout << totalCpuTimes[i] << "," << curFinish << ","
              << curStart << "," << (curFinish - curStart) << ","
              << schedulers[i]->count(TASK_FINISHED) << ","
              << schedulers[i]->count(TASK_LOST) << ","
              << schedulers[i]->count(TASK_FAILED) << ","
              << schedulers[i]->count(TASK_KILLED) << ","
              << schedulers[i]->getScore() << ",";
  }
  std::cout << std::accumulate(totalCpuTimes.begin(), totalCpuTimes.end(), 0.0)
            << "," << (end - start) << std::endl << std::flush;

  LOG(INFO) << "about to call stop() after " << (end - start) << " s simulated";
  scenario->stop();
  LOG(INFO) << "returned from stop()";
}

static std::string readRecord(std::istream* in)
{
  std::string result;
  for (;;) {
    std::string line;
    std::getline(*in, line);
    if (line == "")
      break;
    result += line;
    result += '\n';
  }
  return result;
}

static void runFromFile(const Configuration& conf, const std::string& file)
{
  process::Clock::pause();
  std::ifstream in(file.c_str());
  CHECK(in.good());
  bool haveHeader = false;
  int id = 0;
  for (;;) {
    boost::scoped_ptr<UsageRecorder> recorder;
    std::string record = readRecord(&in);
    if (0 == record.size()) {
      LOG(INFO) << "done reading the file";
      return;
    }
    std::istringstream recordIn(record);
    Scenario scenario(conf);
    populateScenarioFrom(&recordIn, &scenario);
    if (conf.get<std::string>("usage_log_base", "") != "") {
      std::string logFile = conf.get<std::string>("usage_log_base", "") +
        boost::lexical_cast<std::string>(id);
      recorder.reset(new UsageRecorder(
            new TextFileUsageLogWriter(logFile),
            scenario.getMaster(),
            1.0));
      process::spawn(recorder.get());
    }
    run(conf, !haveHeader, boost::lexical_cast<std::string>(id),
        &scenario);
    if (recorder.get()) {
      process::terminate(recorder.get());
      process::wait(recorder.get());
      recorder.reset(0);
    }
    ++id;
    haveHeader = true;
  }
  process::Clock::resume();
}

int main(int argc, char **argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  Configurator configurator;
  Logging::registerOptions(&configurator);
  Master::registerOptions(&configurator);
  Scenario::registerOptions(&configurator);

  configurator.addOption<std::string>("json_file", "JSON file");
  configurator.addOption<int>("seed", "Random seed");
  configurator.addOption<int>("repeat", "number of reptitions", 1);
  configurator.addOption<std::string>("usage_log_base",
      "base name for log files", "");
  configurator.addOption<double>("run_for",
      "seconds to run simulation for at minimum", 0.0);

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

  if (conf.get<std::string>("json_file", "") != "") {
    runFromFile(conf, conf.get<std::string>("json_file", ""));
  } else {
    LOG(FATAL) << "Must specify --json_file";
  }

  LOG(ERROR) << "About to exit normally";

  return EXIT_SUCCESS;
}
