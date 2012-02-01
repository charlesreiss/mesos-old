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

#include "boost/scoped_ptr.hpp"

#include <process/process.hpp>
#include <process/protobuf.hpp>

#include "usage_log/usage_log.pb.h"

namespace mesos {
namespace internal {
namespace usage_log {

class UsageLogWriter {
public:
  virtual void write(const UsageLogRecord& record) = 0;
  virtual ~UsageLogWriter() {}
};

class FileUsageLogWriter : public UsageLogWriter {
public:
  FileUsageLogWriter(const std::string& filename);
  virtual void write(const UsageLogRecord& record);
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
  double interval;
  boost::scoped_ptr<UsageLogWriter> out;
  process::UPID master;
};

}  // namespace usage_log
}  // namespace internal
}  // namespace mesos

#endif
