#include "ray/common/runtime_env_manager.h"
namespace ray {

void RuntimeEnvManagerBase::IncrPackageReference(const std::string &hex_id,
                                                 const rpc::RuntimeEnv &runtime_env) {
  if (!runtime_env.working_dir_uri().empty()) {
    const auto &uri = runtime_env.working_dir_uri();
    uri_reference_[uri]++;
    id_to_uris_[hex_id].push_back(uri);
  }
}

void RuntimeEnvManagerBase::DecrPackageReference(const std::string &hex_id) {
  for (const auto &uri : id_to_uris_[hex_id]) {
    --uri_reference_[uri];
    auto ref_cnt = uri_reference_[uri];
    RAY_CHECK(ref_cnt >= 0);
    if (ref_cnt == 0) {
      uri_reference_.erase(uri);
      RAY_LOG(DEBUG) << "Erase: " << uri;
      DeleteURI(uri);
    }
  }
  id_to_uris_.erase(hex_id);
}

}