// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

// Adapted from Apache Arrow, Apache Kudu, TensorFlow

#include "ray/common/status.h"
#include <map>

#include <assert.h>

namespace ray {

#define STATUS_CODE_OK "OK"
#define STATUS_CODE_OUT_OF_MEMORY "Out of memory"
#define STATUS_CODE_KEY_ERROR "Key error"
#define STATUS_CODE_TYPE_ERROR "Type error"
#define STATUS_CODE_INVALID "Invalid"
#define STATUS_CODE_IO_ERROR "IOError"
#define STATUS_CODE_OBJECT_EXISTS "ObjectExists"
#define STATUS_CODE_OBJECT_STORE_FULL "ObjectStoreFull"
#define STATUS_CODE_UNKNOWN_ERROR "Unknown error"
#define STATUS_CODE_NOT_IMPLEMENTED "NotImplemented"
#define STATUS_CODE_REDIS_ERROR "RedisError"
#define STATUS_CODE_TIMED_OUT "TimedOut"
#define STATUS_CODE_INTERRUPTED "Interrupted"
#define STATUS_CODE_INTENTIONAL_SYSTEM_EXIT "IntentionalSystemExit"
#define STATUS_CODE_UNEXPECTED_SYSTEM_EXIT "UnexpectedSystemExit"
#define STATUS_CODE_UNKNOWN "Unknown"
#define STATUS_SEPARATOR ": "

Status::Status(StatusCode code, const std::string &msg) {
  assert(code != StatusCode::OK);
  state_ = new State;
  state_->code = code;
  state_->msg = msg;
}

void Status::CopyFrom(const State *state) {
  delete state_;
  if (state == nullptr) {
    state_ = nullptr;
  } else {
    state_ = new State(*state);
  }
}

std::string Status::CodeAsString() const {
  if (state_ == NULL) {
    return STATUS_CODE_OK;
  }

  static std::map<StatusCode, std::string> code_to_str = {
      {StatusCode::OK, STATUS_CODE_OK},
      {StatusCode::OutOfMemory, STATUS_CODE_OUT_OF_MEMORY},
      {StatusCode::KeyError, STATUS_CODE_KEY_ERROR},
      {StatusCode::TypeError, STATUS_CODE_TYPE_ERROR},
      {StatusCode::Invalid, STATUS_CODE_INVALID},
      {StatusCode::IOError, STATUS_CODE_IO_ERROR},
      {StatusCode::ObjectExists, STATUS_CODE_OBJECT_EXISTS},
      {StatusCode::ObjectStoreFull, STATUS_CODE_OBJECT_STORE_FULL},
      {StatusCode::UnknownError, STATUS_CODE_UNKNOWN_ERROR},
      {StatusCode::NotImplemented, STATUS_CODE_NOT_IMPLEMENTED},
      {StatusCode::RedisError, STATUS_CODE_REDIS_ERROR},
      {StatusCode::TimedOut, STATUS_CODE_TIMED_OUT},
      {StatusCode::Interrupted, STATUS_CODE_INTERRUPTED},
      {StatusCode::IntentionalSystemExit, STATUS_CODE_INTENTIONAL_SYSTEM_EXIT},
      {StatusCode::UnexpectedSystemExit, STATUS_CODE_UNEXPECTED_SYSTEM_EXIT}};

  if (!code_to_str.count(code())) {
    return STATUS_CODE_UNKNOWN;
  }
  return code_to_str[code()];
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += STATUS_SEPARATOR;
  result += state_->msg;
  return result;
}

Status Status::FromString(const std::string &value) {
  static std::map<std::string, StatusCode> str_to_code = {
      {STATUS_CODE_OK, StatusCode::OK},
      {STATUS_CODE_OUT_OF_MEMORY, StatusCode::OutOfMemory},
      {STATUS_CODE_KEY_ERROR, StatusCode::KeyError},
      {STATUS_CODE_TYPE_ERROR, StatusCode::TypeError},
      {STATUS_CODE_INVALID, StatusCode::Invalid},
      {STATUS_CODE_IO_ERROR, StatusCode::IOError},
      {STATUS_CODE_OBJECT_EXISTS, StatusCode::ObjectExists},
      {STATUS_CODE_OBJECT_STORE_FULL, StatusCode::ObjectStoreFull},
      {STATUS_CODE_UNKNOWN_ERROR, StatusCode::UnknownError},
      {STATUS_CODE_NOT_IMPLEMENTED, StatusCode::NotImplemented},
      {STATUS_CODE_REDIS_ERROR, StatusCode::RedisError},
      {STATUS_CODE_TIMED_OUT, StatusCode::TimedOut},
      {STATUS_CODE_INTERRUPTED, StatusCode::Interrupted},
      {STATUS_CODE_INTENTIONAL_SYSTEM_EXIT, StatusCode::IntentionalSystemExit},
      {STATUS_CODE_UNEXPECTED_SYSTEM_EXIT, StatusCode::UnexpectedSystemExit}};

  size_t pos = value.find(STATUS_SEPARATOR);
  if (pos != std::string::npos) {
    std::string code_str = value.substr(0, pos);
    RAY_CHECK(str_to_code.count(code_str));
    StatusCode code = str_to_code[code_str];
    return Status(code, value.substr(pos + strlen(STATUS_SEPARATOR)));
  } else {
    // Status ok does not include ":".
    return Status();
  }
}

}  // namespace ray
