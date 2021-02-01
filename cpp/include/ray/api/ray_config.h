
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

  RunMode run_mode = RunMode::CLUSTER;

  std::string redis_ip;

  int redis_port = 6379;

  std::string redis_password = "5241590000000000";

  int node_manager_port = 62665;

  std::string lib_name = "";

  std::string store_socket = "";

  std::string raylet_socket = "";

  std::string session_dir = "";

  int min_workers = 1;

  int max_workers = 0;

  int num_workers = min_workers;

  static std::shared_ptr<RayConfig> GetInstance();

  static std::shared_ptr<RayConfig> GetInstance(std::string address, bool local_mode,
                                                int min_workers, int max_workers);

 private:
  static std::shared_ptr<RayConfig> config_;
};

}  // namespace api
}  // namespace ray