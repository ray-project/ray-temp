#include <chrono>

#include "ray/core_worker/profiling.h"

namespace ray {

namespace worker {

ProfileEvent::ProfileEvent(const std::shared_ptr<Profiler> profiler,
                           const std::string &event_type)
    : profiler_(profiler) {
  rpc_event_.set_event_type(event_type);
  rpc_event_.set_start_time(current_sys_time_seconds());
}

Profiler::Profiler(WorkerContext &worker_context, const std::string &node_ip_address,
                   std::unique_ptr<gcs::RedisGcsClient> &gcs_client)
    : worker_context_(worker_context), gcs_client_(gcs_client) {
  profile_info_.set_component_type(WorkerTypeString(worker_context_.GetWorkerType()));
  profile_info_.set_component_id(worker_context_.GetWorkerID().Binary());
  profile_info_.set_node_ip_address(node_ip_address);
}

void Profiler::Start() {
  thread_ = std::thread(&Profiler::PeriodicallyFlushEvents, this);
}

void Profiler::AddEvent(const rpc::ProfileTableData::ProfileEvent &event) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (killed_) {
    return;
  }
  if (!thread_.joinable()) {
    RAY_LOG(WARNING)
        << "Tried to add profile event but background thread isn't running. "
        << "Either Profiler::Start() wasn't run yet or the thread exited unexpectedly.";
    return;
  }
  profile_info_.add_profile_events()->CopyFrom(event);
}

void Profiler::PeriodicallyFlushEvents() {
  while (true) {
    // Push events every 1 second until killed_ is set.
    {
      std::unique_lock<std::mutex> lock(mutex_);
      kill_cond_.wait_for(lock, std::chrono::seconds(1));
      if (killed_) {
        return;
      }

      if (profile_info_.profile_events_size() == 0) {
        continue;
      }
      // TODO(edoakes): this should be migrated to use the new GCS client interface
      // instead of the raw table interface once it's ready.
      if (!gcs_client_->profile_table().AddProfileEventBatch(profile_info_).ok()) {
        RAY_LOG(WARNING) << "Failed to push profile events to GCS.";
      } else {
        RAY_LOG(DEBUG) << "Pushed " << profile_info_.profile_events_size()
                       << "events to GCS.";
      }
      profile_info_.clear_profile_events();
    }
  }
}

}  // namespace worker

}  // namespace ray
