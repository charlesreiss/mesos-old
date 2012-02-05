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

#include "usage_log/usage_log.hpp"

#include <limits>
#include <algorithm>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "process/timer.hpp"

#include "common/foreach.hpp"

namespace mesos {
namespace internal {
namespace usage_log {

TextFileUsageLogWriter::TextFileUsageLogWriter(const std::string& filename)
{
  out.open(filename.c_str());
  out_proto.reset(new google::protobuf::io::OstreamOutputStream(&out));
  printer.SetSingleLineMode(true);
}

void TextFileUsageLogWriter::write(const UsageLogRecord& record)
{
  printer.Print(record, out_proto.get());
}

UsageRecorder::UsageRecorder(UsageLogWriter* out_, const UPID& master_,
                             double interval_)
    : doneFirstTick(false), interval(interval_), out(out_), master(master_)
{
}

void UsageRecorder::initialize()
{
  endTime[0] = process::Clock::now();
  endTime[1] = process::Clock::now() + interval;
  process::delay(interval, self(), &UsageRecorder::tick);
  RegisterUsageListenerMessage registerMessage;
  registerMessage.set_pid(self());
  send(master, registerMessage);
}

void UsageRecorder::finalize()
{
  emit();
}

void UsageRecorder::emit()
{
  UsageLogRecord record;
  record.set_min_expect_timestamp(endTime[0] - interval);
  record.set_max_expect_timestamp(endTime[0]);
  double minTimestamp = std::numeric_limits<double>::infinity();
  double maxTimestamp = 0;
  foreach (const UsageMessage& usage, pendingUsage[0]) {
    record.add_usage()->MergeFrom(usage);
    minTimestamp = std::min(minTimestamp, usage.timestamp());
    maxTimestamp = std::max(maxTimestamp, usage.timestamp());
  }
  foreach (const StatusUpdate& update, pendingUpdates[0]) {
    record.add_update()->MergeFrom(update);
    minTimestamp = std::min(minTimestamp, update.timestamp());
    maxTimestamp = std::max(maxTimestamp, update.timestamp());
  }
  if (minTimestamp <= maxTimestamp) {
    record.set_min_seen_timestamp(minTimestamp);
    record.set_max_seen_timestamp(maxTimestamp);
  }
  out->write(record);
}

void UsageRecorder::advance()
{
  std::swap(pendingUsage[0], pendingUsage[1]);
  pendingUsage[1].clear();
  std::swap(pendingUpdates[0], pendingUpdates[1]);
  pendingUpdates[1].clear();
  endTime[0] = endTime[1];
  endTime[1] = endTime[0] + interval;
}

void UsageRecorder::tick()
{
  if (doneFirstTick) {
    emit();
  }
  advance();
  process::delay(process::Clock::now() - endTime[0], self(),
                 &UsageRecorder::tick);
  doneFirstTick = true;
}

}  // namespace usage_log
}  // namespace internal
}  // namespace mesos
