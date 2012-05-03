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
  printer.SetSingleLineMode(true);
}

void TextFileUsageLogWriter::write(const UsageLogRecord& record)
{
  std::string tempString;
  printer.PrintToString(record, &tempString);
  out << tempString << std::endl;
}

BinaryFileUsageLogWriter::BinaryFileUsageLogWriter(const std::string& filename)
{
  out.open(filename.c_str());
}

void BinaryFileUsageLogWriter::write(const UsageLogRecord& record)
{
  int size = record.ByteSize();
  out.write(reinterpret_cast<char*>(&size), sizeof(size));
  record.SerializeToOstream(&out);
}

UsageRecorder::UsageRecorder(UsageLogWriter* out_, const UPID& master_,
                             double interval_)
    : doneFirstTick(false), interval(interval_), out(out_), master(master_)
{
}

void UsageRecorder::initialize()
{
  // interval 0 is initially [now - interval = A, now = B]
  // interval 1 is intiially [now = B, now + interval = C];
  // at the next tick at time C, we advance the intervals to [B, C];[C, D]
  // at the tick at time D, we emit [B, C].
  endTime[0] = process::Clock::now();
  endTime[1] = process::Clock::now() + interval;
  process::delay(interval, self(), &UsageRecorder::tick);
  RegisterUsageListenerMessage registerMessage;
  registerMessage.set_pid(self());
  send(master, registerMessage);

  install<UsageMessage>(&UsageRecorder::recordUsageMessage);
  install<StatusUpdateMessage>(&UsageRecorder::recordStatusUpdateMessage);
}

void UsageRecorder::finalize()
{
  emit();
}

void UsageRecorder::recordUsageMessage(const UsageMessage& message)
{
  // Assumes all measurements are really at least this long for the purpose
  // of placing them in a bucket.
  const double kMinMeasurement = 0.1;
  // Minimum portion of the measurement which must overlap for it to be recorded
  // in a bucket.
  const double kMinOverlap = 0.1;
  double effectiveDuration = std::max(message.duration(), kMinMeasurement);
  double effectiveEnd = message.timestamp();
  double effectiveStart = effectiveEnd - effectiveDuration;
  CHECK_GT(effectiveEnd, effectiveStart);
  CHECK_LT(endTime[0], endTime[1]);
  for (int i = 0; i < 2; ++i) {
    // start time within this measurement period
    double start = i == 0 ? 0 : endTime[i - 1];
    start = std::max(effectiveStart, start);
    // end time within this measurement period
    double end = std::min(effectiveEnd, endTime[i]);
    double overlap = std::max(0.0, (end - start) / effectiveDuration);
    if (overlap >= kMinOverlap) {
      pendingUsage[i].push_back(message);
    }
  }
}

void UsageRecorder::recordStatusUpdateMessage(const StatusUpdateMessage& message)
{
  if (message.update().timestamp() <= endTime[0]) {
    pendingUpdates[0].push_back(message.update());
  } else {
    pendingUpdates[1].push_back(message.update());
  }
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
    minTimestamp = std::min(minTimestamp, usage.timestamp() - usage.duration());
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
  LOG(INFO) << "Recording " << record.DebugString()
            << " at delta "
            << (process::Clock::now() - record.min_expect_timestamp());
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
  process::delay((endTime[0] + interval) - process::Clock::now(), self(),
                 &UsageRecorder::tick);
  doneFirstTick = true;
}

}  // namespace usage_log
}  // namespace internal
}  // namespace mesos
