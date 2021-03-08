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


#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/util/logging.h>

using namespace ::ray::api;

int default_worker_main(int argc, char **argv) {
  RAY_LOG(INFO) << "CPP default worker started";
  RAY_CHECK(argc == 7);

  auto config = ray::api::RayConfig::GetInstance();
  config->run_mode = RunMode::CLUSTER;
  config->worker_type = ray::WorkerType::WORKER;
  config->store_socket = std::string(argv[1]);
  config->raylet_socket = std::string(argv[2]);
  config->node_manager_port = std::stoi(std::string(argv[3]));
  std::string redis_address = std::string(std::string(argv[4]));
  config->SetRedisAddress(redis_address);
  config->redis_password = std::string(std::string(argv[5]));
  config->session_dir = std::string(std::string(argv[6]));

  Ray::Init();

  ::ray::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}

int main(int argc, char **argv) {
  default_worker_main(argc, argv);
  return 0;
}
