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

#ifndef __USAGE_LOG_HPP__
#define __USAGE_LOG_HPP__

#include <fstream>
#include "boost/scoped_ptr.hpp"

#include <process/process.hpp>
#include <process/protobuf.hpp>

#include "usage_log/usage_log.pb.h"

#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/text_format.h>

namespace mesos {
namespace internal {
namespace usage_log {

class UsageLogWriter {
public:
  virtual void write(const UsageLogRecord& record) = 0;
  virtual ~UsageLogWriter() {}
};

class TextFileUsageLogWriter : public UsageLogWriter {
public:
  TextFileUsageLogWriter(const std::string& filename);
  virtual void write(const UsageLogRecord& record);

private:
  std::ofstream out;
  google::protobuf::TextFormat::Printer printer;
};

using namespace process;

class UsageRecorder;
class UsageRecorder : public ProtobufProcess<UsageRecorder>
{
public:
  UsageRecorder(UsageLogWriter* out_,
                const process::UPID& master_,
                double interval_);

protected:
  void initialize();
  void finalize();

private:
  // Entrypoints for messages from the master.
  void recordUsageMessage(const UsageMessage&);
  void recordStatusUpdateMessage(const StatusUpdateMessage&);

  // We collect two sets of pending measurements. Index 0 contains
  // the next measurements we are about to emit, and index 1 contains
  // the current time interval of measurements. Every tick, we emit
  // the index 0 measurements, move the index 1 measurements to index 0, and
  // clear the index 1 measurements.
  void emit();
  void advance();
  void tick();

  bool doneFirstTick;
  double interval;
  double endTime[2];
  std::vector<UsageMessage> pendingUsage[2];
  std::vector<StatusUpdate> pendingUpdates[2];
  boost::scoped_ptr<UsageLogWriter> out;
  process::UPID master;
};

}  // namespace usage_log
}  // namespace internal
}  // namespace mesos

#endif
