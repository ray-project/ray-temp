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

#pragma once
#include <memory>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

enum class RunMode { SINGLE_PROCESS, CLUSTER };

/// TODO(Guyang Song): Make configuration complete and use to initialize.
class RayConfig {
 public:
  WorkerType worker_type = WorkerType::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string redis_ip;

  int redis_port = 6379;

  std::string redis_password = "5241590000000000";

  int node_manager_port = 62665;

  std::string lib_name = "";

  std::string store_socket = "";

  std::string raylet_socket = "";

  std::string session_dir = "";

  bool use_ray_remote = false;

  static std::shared_ptr<RayConfig> GetInstance();

  void SetRedisAddress(const std::string address) {
    auto pos = address.find(':');
    RAY_CHECK(pos != std::string::npos);
    redis_ip = address.substr(0, pos);
    redis_port = std::stoi(address.substr(pos + 1, address.length()));
  }

 private:
  static std::shared_ptr<RayConfig> config_;
};

}  // namespace api
}  // namespace ray