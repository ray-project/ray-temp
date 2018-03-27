#include <mutex>

#include "object_directory.h"

namespace ray {

std::mutex gcs_mutex;

ObjectDirectory::ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> gcs_client) {
  gcs_client_ = gcs_client;
};

ray::Status ObjectDirectory::ReportObjectAdded(const ObjectID &object_id,
                                               const ClientID &client_id) {
  // TODO(hme): Determine whether we need to do lookup to append.
  std::lock_guard<std::mutex> lock(gcs_mutex);
  JobID job_id = JobID::from_random();
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = false;
  data->num_evictions = object_evictions_[object_id];
  ray::Status status = gcs_client_->object_table().Append(
      job_id, object_id, data, [](gcs::AsyncGcsClient *client, const UniqueID &id,
                                  const std::shared_ptr<ObjectTableDataT> data) {
        // Do nothing.
      });
  return status;
};

ray::Status ObjectDirectory::ReportObjectRemoved(const ObjectID &object_id,
                                                 const ClientID &client_id) {
  JobID job_id = JobID::from_random();
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = true;
  data->num_evictions = object_evictions_[object_id];
  ray::Status status = gcs_client_->object_table().Append(
      job_id, object_id, data, [](gcs::AsyncGcsClient *client, const UniqueID &id,
                                  const std::shared_ptr<ObjectTableDataT> data) {
        // Do nothing.
      });
  // Increment the number of times we've evicted this object.
  object_evictions_[object_id]++;
  return status;
};

ray::Status ObjectDirectory::GetInformation(const ClientID &client_id,
                                            const InfoSuccessCallback &success_cb,
                                            const InfoFailureCallback &fail_cb) {
  std::lock_guard<std::mutex> lock(gcs_mutex);
  const ClientTableDataT &data = gcs_client_->client_table().GetClient(client_id);
  ClientID result_client_id = ClientID::from_binary(data.client_id);
  if (result_client_id == ClientID::nil() || !data.is_insertion) {
    fail_cb(ray::Status::RedisError("ClientID not found."));
  } else {
    const auto &info = RemoteConnectionInfo(client_id, data.node_manager_address,
                                            (uint16_t)data.object_manager_port);
    success_cb(info);
  }
  return ray::Status::OK();
};

ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                          const OnLocationsSuccess &success_cb,
                                          const OnLocationsFailure &fail_cb) {
  std::lock_guard<std::mutex> lock(gcs_mutex);
  ray::Status status_code = ray::Status::OK();
  if (existing_requests_.count(object_id) == 0) {
    existing_requests_[object_id] = ODCallbacks({success_cb, fail_cb});
    status_code = ExecuteGetLocations(object_id);
  } else {
    // Do nothing. A request is in progress.
  }
  return status_code;
};

ray::Status ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id) {
  JobID job_id = JobID::from_random();
  ray::Status status = gcs_client_->object_table().Lookup(
      job_id, object_id,
      [this, object_id](gcs::AsyncGcsClient *client, const ObjectID &object_id,
                        const std::vector<ObjectTableDataT> &data) {
        GetLocationsComplete(object_id, data);
      });
  return status;
};

void ObjectDirectory::GetLocationsComplete(
    const ObjectID &object_id, const std::vector<ObjectTableDataT> &location_entries) {
  auto request = existing_requests_.find(object_id);
  // Do not invoke a callback if the request was cancelled.
  if (request == existing_requests_.end()) {
    return;
  }
  // Build the set of current locations based on the entries in the log.
  std::unordered_set<ClientID, UniqueIDHasher> locations;
  for (auto entry : location_entries) {
    ClientID client_id = ClientID::from_binary(entry.manager);
    if (!entry.is_eviction) {
      locations.insert(client_id);
    } else {
      locations.erase(client_id);
    }
  }
  // Invoke the callback.
  std::vector<ClientID> locations_vector(locations.begin(), locations.end());
  if (locations_vector.empty()) {
    request->second.fail_cb(object_id);
  } else {
    request->second.success_cb(locations_vector, object_id);
  }
  existing_requests_.erase(request);
}

ray::Status ObjectDirectory::Cancel(const ObjectID &object_id) {
  existing_requests_.erase(object_id);
  return ray::Status::OK();
};

ray::Status ObjectDirectory::Terminate() { return ray::Status::OK(); };

}  // namespace ray
