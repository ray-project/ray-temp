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

#include "ray/object_manager/object_manager.h"

#include <chrono>

#include "ray/common/common_protocol.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace object_manager_protocol = ray::object_manager::protocol;

namespace ray {

ObjectStoreRunner::ObjectStoreRunner(const ObjectManagerConfig &config,
                                     SpillObjectsCallback spill_objects_callback) {
  if (config.object_store_memory > 0) {
    plasma::plasma_store_runner.reset(new plasma::PlasmaStoreRunner(
        config.store_socket_name, config.object_store_memory, config.huge_pages,
        config.plasma_directory, ""));
    // Initialize object store.
    store_thread_ =
        std::thread(&plasma::PlasmaStoreRunner::Start, plasma::plasma_store_runner.get(),
                    spill_objects_callback);
    // Sleep for sometime until the store is working. This can suppress some
    // connection warnings.
    std::this_thread::sleep_for(std::chrono::microseconds(500));
  }
}

ObjectStoreRunner::~ObjectStoreRunner() {
  if (plasma::plasma_store_runner != nullptr) {
    plasma::plasma_store_runner->Stop();
    store_thread_.join();
    plasma::plasma_store_runner.reset();
  }
}

ObjectManager::ObjectManager(asio::io_service &main_service, const NodeID &self_node_id,
                             const ObjectManagerConfig &config,
                             std::shared_ptr<ObjectDirectoryInterface> object_directory,
                             RestoreSpilledObjectCallback restore_spilled_object,
                             SpillObjectsCallback spill_objects_callback)
    : self_node_id_(self_node_id),
      config_(config),
      object_directory_(std::move(object_directory)),
      object_store_internal_(config, spill_objects_callback),
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size),
      rpc_work_(rpc_service_),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      object_manager_server_("ObjectManager", config_.object_manager_port,
                             config_.rpc_service_threads_number),
      object_manager_service_(rpc_service_, *this),
      client_call_manager_(main_service, config_.rpc_service_threads_number),
      restore_spilled_object_(restore_spilled_object) {
  RAY_CHECK(config_.rpc_service_threads_number > 0);
  main_service_ = &main_service;

  push_manager_.reset(new PushManager(std::max(
      1L,
      static_cast<int64_t>(config_.max_bytes_in_flight / config_.object_chunk_size))));

  if (plasma::plasma_store_runner) {
    store_notification_ = std::make_shared<ObjectStoreNotificationManager>(main_service);
    plasma::plasma_store_runner->SetNotificationListener(store_notification_);
  } else {
    store_notification_ = std::make_shared<ObjectStoreNotificationManagerIPC>(
        main_service, config_.store_socket_name);
  }

  store_notification_->SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        HandleObjectAdded(object_info);
      });
  store_notification_->SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });

  // Start object manager rpc server and send & receive request threads
  StartRpcService();
}

ObjectManager::~ObjectManager() { StopRpcService(); }

void ObjectManager::Stop() {
  if (plasma::plasma_store_runner != nullptr) {
    plasma::plasma_store_runner->Stop();
  }
}

void ObjectManager::RunRpcService() { rpc_service_.run(); }

void ObjectManager::StartRpcService() {
  rpc_threads_.resize(config_.rpc_service_threads_number);
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i] = std::thread(&ObjectManager::RunRpcService, this);
  }
  object_manager_server_.RegisterService(object_manager_service_);
  object_manager_server_.Run();
}

void ObjectManager::StopRpcService() {
  rpc_service_.stop();
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i].join();
  }
  object_manager_server_.Shutdown();
}

void ObjectManager::HandleObjectAdded(
    const object_manager::protocol::ObjectInfoT &object_info) {
  // Notify the object directory that the object has been added to this node.
  ObjectID object_id = ObjectID::FromBinary(object_info.object_id);
  RAY_LOG(DEBUG) << "Object added " << object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;
  ray::Status status =
      object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // Handle the unfulfilled_push_requests_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &client_id = pair.first;
      main_service_->post([this, object_id, client_id]() { Push(object_id, client_id); });
      // When push timeout is set to -1, there will be an empty timer in pair.second.
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }

  // The object is local, so we no longer need to Pull it from a remote
  // manager. Cancel any outstanding Pull requests for this object.
  CancelPull(object_id);
}

void ObjectManager::NotifyDirectoryObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  ray::Status status =
      object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);
}

ray::Status ObjectManager::SubscribeObjAdded(
    std::function<void(const object_manager::protocol::ObjectInfoT &)> callback) {
  store_notification_->SubscribeObjAdded(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  store_notification_->SubscribeObjDeleted(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::Pull(const ObjectID &object_id,
                                const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "Pull on " << self_node_id_ << " of object " << object_id;
  // Check if object is already local.
  if (local_objects_.count(object_id) != 0) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return ray::Status::OK();
  }
  if (pull_requests_.find(object_id) != pull_requests_.end()) {
    RAY_LOG(DEBUG) << object_id << " has inflight pull_requests, skipping.";
    return ray::Status::OK();
  }

  pull_requests_.emplace(object_id, PullRequest());
  // Subscribe to object notifications. A notification will be received every
  // time the set of client IDs for the object changes. Notifications will also
  // be received if the list of locations is empty. The set of client IDs has
  // no ordering guarantee between notifications.
  return object_directory_->SubscribeObjectLocations(
      object_directory_pull_callback_id_, object_id, owner_address,
      [this](const ObjectID &object_id, const std::unordered_set<NodeID> &client_ids,
             const std::string &spilled_url) {
        // Exit if the Pull request has already been fulfilled or canceled.
        auto it = pull_requests_.find(object_id);
        if (it == pull_requests_.end()) {
          return;
        }
        // Reset the list of clients that are now expected to have the object.
        // NOTE(swang): Since we are overwriting the previous list of clients,
        // we may end up sending a duplicate request to the same client as
        // before.
        it->second.client_locations =
            std::vector<NodeID>(client_ids.begin(), client_ids.end());
        if (!spilled_url.empty()) {
          // Try to restore the spilled object.
          restore_spilled_object_(object_id, spilled_url,
                                  [this, object_id](const ray::Status &status) {
                                    // Fall back to fetching from another object manager.
                                    if (!status.ok()) {
                                      TryPull(object_id);
                                    }
                                  });
        } else if (it->second.client_locations.empty()) {
          // The object locations are now empty, so we should wait for the next
          // notification about a new object location.  Cancel the timer until
          // the next Pull attempt since there are no more clients to try.
          if (it->second.retry_timer != nullptr) {
            it->second.retry_timer->cancel();
            it->second.timer_set = false;
          }
        } else {
          // New object locations were found, so begin trying to pull from a
          // client. This will be called every time a new client location
          // appears.
          TryPull(object_id);
        }
      });
}

void ObjectManager::TryPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  auto &node_vector = it->second.client_locations;

  // The timer should never fire if there are no expected client locations.
  if (node_vector.empty()) {
    return;
  }

  RAY_CHECK(local_objects_.count(object_id) == 0);
  // Make sure that there is at least one client which is not the local client.
  // TODO(rkn): It may actually be possible for this check to fail.
  if (node_vector.size() == 1 && node_vector[0] == self_node_id_) {
    RAY_LOG(WARNING) << "The object manager with ID " << self_node_id_
                     << " is trying to pull object " << object_id
                     << " but the object table suggests that this object manager "
                     << "already has the object. The object may have been evicted. It is "
                     << "most likely due to memory pressure, object pull has been "
                     << "requested before object location is updated.";
    it->second.timer_set = false;
    return;
  }

  // Choose a random client to pull the object from.
  // Generate a random index.
  std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
  int node_index = distribution(gen_);
  NodeID node_id = node_vector[node_index];
  // If the object manager somehow ended up choosing itself, choose a different
  // object manager.
  if (node_id == self_node_id_) {
    std::swap(node_vector[node_index], node_vector[node_vector.size() - 1]);
    node_vector.pop_back();
    RAY_LOG(WARNING)
        << "The object manager with ID " << self_node_id_ << " is trying to pull object "
        << object_id << " but the object table suggests that this object manager "
        << "already has the object. It is most likely due to memory pressure, object "
        << "pull has been requested before object location is updated.";
    node_id = node_vector[node_index % node_vector.size()];
    RAY_CHECK(node_id != self_node_id_);
  }

  RAY_LOG(DEBUG) << "Sending pull request from " << self_node_id_ << " to " << node_id
                 << " of object " << object_id;

  auto rpc_client = GetRpcClient(node_id);
  if (rpc_client) {
    // Try pulling from the client.
    rpc_service_.post([this, object_id, node_id, rpc_client]() {
      SendPullRequest(object_id, node_id, rpc_client);
    });
  } else {
    RAY_LOG(ERROR) << "Couldn't send pull request from " << self_node_id_ << " to "
                   << node_id << " of object " << object_id
                   << " , setup rpc connection failed.";
  }

  // If there are more clients to try, try them in succession, with a timeout
  // in between each try.
  if (!it->second.client_locations.empty()) {
    if (it->second.retry_timer == nullptr) {
      // Set the timer if we haven't already.
      it->second.retry_timer = std::unique_ptr<boost::asio::deadline_timer>(
          new boost::asio::deadline_timer(*main_service_));
    }

    // Wait for a timeout. If we receive the object or a caller Cancels the
    // Pull within the timeout, then nothing will happen. Otherwise, the timer
    // will fire and the next client in the list will be tried.
    boost::posix_time::milliseconds retry_timeout(config_.pull_timeout_ms);
    it->second.retry_timer->expires_from_now(retry_timeout);
    it->second.retry_timer->async_wait(
        [this, object_id](const boost::system::error_code &error) {
          if (!error) {
            // Try the Pull from the next client.
            TryPull(object_id);
          } else {
            // Check that the error was due to the timer being canceled.
            RAY_CHECK(error == boost::asio::error::operation_aborted);
          }
        });
    // Record that we set the timer until the next attempt.
    it->second.timer_set = true;
  } else {
    // The timer is not reset since there are no more clients to try. Go back
    // to waiting for more notifications. Once we receive a new object location
    // from the object directory, then the Pull will be retried.
    it->second.timer_set = false;
  }
};

void ObjectManager::SendPullRequest(
    const ObjectID &object_id, const NodeID &client_id,
    std::shared_ptr<rpc::ObjectManagerClient> rpc_client) {
  rpc::PullRequest pull_request;
  pull_request.set_object_id(object_id.Binary());
  pull_request.set_client_id(self_node_id_.Binary());

  rpc_client->Pull(pull_request, [object_id, client_id](const Status &status,
                                                        const rpc::PullReply &reply) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Send pull " << object_id << " request to client " << client_id
                       << " failed due to" << status.message();
    }
  });
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const NodeID &client_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  RAY_CHECK(iter != unfulfilled_push_requests_.end());
  size_t num_erased = iter->second.erase(client_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleSendFinished(const ObjectID &object_id, const NodeID &client_id,
                                       uint64_t chunk_index, double start_time,
                                       double end_time, ray::Status status) {
  RAY_LOG(DEBUG) << "HandleSendFinished on " << self_node_id_ << " to " << client_id
                 << " of object " << object_id << " chunk " << chunk_index
                 << ", status: " << status.ToString();
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
  }

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_send");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, client ID, chunk index, and status as a json list,
  // which will be parsed by the reader of the profile table.
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + client_id.Hex() +
                               "\"," + std::to_string(chunk_index) + ",\"" +
                               status.ToString() + "\"]");

  std::lock_guard<std::mutex> lock(profile_mutex_);
  profile_events_.push_back(profile_event);
}

void ObjectManager::HandleReceiveFinished(const ObjectID &object_id,
                                          const NodeID &client_id, uint64_t chunk_index,
                                          double start_time, double end_time,
                                          ray::Status status) {
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
  }

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_receive");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, client ID, chunk index, and status as a json list,
  // which will be parsed by the reader of the profile table.

  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + client_id.Hex() +
                               "\"," + std::to_string(chunk_index) + ",\"" +
                               status.ToString() + "\"]");

  std::lock_guard<std::mutex> lock(profile_mutex_);
  profile_events_.push_back(profile_event);
}

void PushManager::StartPush(const UniqueID &push_id, int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  RAY_LOG(ERROR) << "Start push for " << push_id << ", num chunks " << num_chunks;
  RAY_CHECK(num_chunks > 0);
  push_info_[push_id] = std::make_pair(num_chunks, send_chunk_fn);
  next_chunk_id_[push_id] = 0;
  chunks_remaining_ += num_chunks;
  ScheduleRemainingPushes();
  RAY_CHECK(push_info_.size() == next_chunk_id_.size());
}

void PushManager::OnChunkComplete() {
  chunks_in_flight_ -= 1;
  ScheduleRemainingPushes();
}

// TODO(ekl) currently chunks are sent in somewhat arbitrary order, we might want to
// prioritize chunk sends in FIFO or round robin according to push id.
void PushManager::ScheduleRemainingPushes() {
  auto it = push_info_.begin();
  while (it != push_info_.end() && chunks_in_flight_ < max_chunks_in_flight_) {
    auto push_id = it->first;
    auto max_chunks = it->second.first;
    auto send_chunk_fn = it->second.second;

    // Send the next chunk for this push.
    send_chunk_fn(next_chunk_id_[push_id]);
    chunks_in_flight_ += 1;
    chunks_remaining_ -= 1;
    RAY_LOG(ERROR) << "Sending chunk " << next_chunk_id_[push_id] << " of " << max_chunks
                   << " for push " << push_id << ", chunks in flight "
                   << NumChunksInFlight() << " / " << max_chunks_in_flight_
                   << " max, remaining chunks: " << NumChunksRemaining();

    // It is the last chunk and we don't need to track it any more.
    if (++next_chunk_id_[push_id] >= max_chunks) {
      next_chunk_id_.erase(push_id);
      push_info_.erase(it++);
      RAY_LOG(ERROR) << "Push for " << push_id
                     << " completed, remaining: " << NumPushesInFlight();
    } else {
      it++;
    }
  }
}

void ObjectManager::Push(const ObjectID &object_id, const NodeID &client_id) {
  RAY_LOG(DEBUG) << "Push on " << self_node_id_ << " to " << client_id << " of object "
                 << object_id;
  if (local_objects_.count(object_id) == 0) {
    // Avoid setting duplicated timer for the same object and client pair.
    auto &clients = unfulfilled_push_requests_[object_id];
    if (clients.count(client_id) == 0) {
      // If config_.push_timeout_ms < 0, we give an empty timer
      // and the task will be kept infinitely.
      auto timer = std::unique_ptr<boost::asio::deadline_timer>();
      if (config_.push_timeout_ms == 0) {
        // The Push request fails directly when config_.push_timeout_ms == 0.
        RAY_LOG(WARNING) << "Invalid Push request ObjectID " << object_id
                         << " due to direct timeout setting. (0 ms timeout)";
      } else if (config_.push_timeout_ms > 0) {
        // Put the task into a queue and wait for the notification of Object added.
        timer.reset(new boost::asio::deadline_timer(*main_service_));
        auto clean_push_period = boost::posix_time::milliseconds(config_.push_timeout_ms);
        timer->expires_from_now(clean_push_period);
        timer->async_wait(
            [this, object_id, client_id](const boost::system::error_code &error) {
              // Timer killing will receive the boost::asio::error::operation_aborted,
              // we only handle the timeout event.
              if (!error) {
                HandlePushTaskTimeout(object_id, client_id);
              }
            });
      }
      if (config_.push_timeout_ms != 0) {
        clients.emplace(client_id, std::move(timer));
      }
    }
    return;
  }

  // If we haven't pushed this object to this same object manager yet, then push
  // it. If we have, but it was a long time ago, then push it. If we have and it
  // was recent, then don't do it again.
  auto &recent_pushes = local_objects_[object_id].recent_pushes;
  auto it = recent_pushes.find(client_id);
  if (it == recent_pushes.end()) {
    // We haven't pushed this specific object to this specific object manager
    // yet (or if we have then the object must have been evicted and recreated
    // locally).
    recent_pushes[client_id] = absl::GetCurrentTimeNanos() / 1000000;
  } else {
    int64_t current_time = absl::GetCurrentTimeNanos() / 1000000;
    if (current_time - it->second <=
        RayConfig::instance().object_manager_repeated_push_delay_ms()) {
      // We pushed this object to the object manager recently, so don't do it
      // again.
      RAY_LOG(DEBUG) << "Object " << object_id << " recently pushed to " << client_id;
      return;
    } else {
      it->second = current_time;
    }
  }

  auto rpc_client = GetRpcClient(client_id);
  if (rpc_client) {
    const object_manager::protocol::ObjectInfoT &object_info =
        local_objects_[object_id].object_info;
    uint64_t data_size =
        static_cast<uint64_t>(object_info.data_size + object_info.metadata_size);
    uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);
    uint64_t num_chunks = buffer_pool_.GetNumChunks(data_size);

    rpc::Address owner_address;
    owner_address.set_raylet_id(object_info.owner_raylet_id);
    owner_address.set_ip_address(object_info.owner_ip_address);
    owner_address.set_port(object_info.owner_port);
    owner_address.set_worker_id(object_info.owner_worker_id);

    RAY_LOG(DEBUG) << "Sending object chunks of " << object_id << " to client "
                   << client_id << ", number of chunks: " << num_chunks
                   << ", total data size: " << data_size;

    UniqueID push_id = UniqueID::FromRandom();
    push_manager_->StartPush(push_id, num_chunks, [=](int64_t chunk_id) {
      SendObjectChunk(push_id, object_id, owner_address, client_id, data_size,
                      metadata_size, chunk_id, rpc_client,
                      [=](const Status &status) { push_manager_->OnChunkComplete(); });
    });
  } else {
    // Push is best effort, so do nothing here.
    RAY_LOG(ERROR)
        << "Failed to establish connection for Push with remote object manager.";
  }
}

void ObjectManager::SendObjectChunk(const UniqueID &push_id, const ObjectID &object_id,
                                    const rpc::Address &owner_address,
                                    const NodeID &client_id, uint64_t data_size,
                                    uint64_t metadata_size, uint64_t chunk_index,
                                    std::shared_ptr<rpc::ObjectManagerClient> rpc_client,
                                    std::function<void(const Status &)> on_complete) {
  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  rpc::PushRequest push_request;
  // Set request header
  push_request.set_push_id(push_id.Binary());
  push_request.set_object_id(object_id.Binary());
  push_request.mutable_owner_address()->CopyFrom(owner_address);
  push_request.set_client_id(self_node_id_.Binary());
  push_request.set_data_size(data_size);
  push_request.set_metadata_size(metadata_size);
  push_request.set_chunk_index(chunk_index);

  // Get data
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.GetChunk(object_id, data_size, metadata_size, chunk_index);
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;

  // Fail on status not okay. The object is local, and there is
  // no other anticipated error here.
  ray::Status status = chunk_status.second;
  if (!chunk_status.second.ok()) {
    RAY_LOG(WARNING) << "Attempting to push object " << object_id
                     << " which is not local. It may have been evicted.";
    on_complete(status);
    return;
  }

  push_request.set_data(chunk_info.data, chunk_info.buffer_length);

  // record the time cost between send chunk and receive reply
  rpc::ClientCallback<rpc::PushReply> callback =
      [this, start_time, object_id, client_id, chunk_index, push_id, data_size,
       owner_address, metadata_size, rpc_client,
       on_complete](const Status &status, const rpc::PushReply &reply) {
        // TODO: Just print warning here, should we try to resend this chunk?
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Send object " << object_id << " chunk to client "
                           << client_id << " failed due to" << status.message()
                           << ", chunk index: " << chunk_index;
        }
        double end_time = absl::GetCurrentTimeNanos() / 1e9;
        HandleSendFinished(object_id, client_id, chunk_index, start_time, end_time,
                           status);
        on_complete(status);
      };
  rpc_client->Push(push_request, callback);

  // Do this regardless of whether it failed or succeeded.
  buffer_pool_.ReleaseGetChunk(object_id, chunk_info.chunk_index);
}

void ObjectManager::CancelPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
      object_directory_pull_callback_id_, object_id));
  pull_requests_.erase(it);
}

ray::Status ObjectManager::Wait(
    const std::vector<ObjectID> &object_ids,
    const std::unordered_map<ObjectID, rpc::Address> &owner_addresses, int64_t timeout_ms,
    uint64_t num_required_objects, bool wait_local, const WaitCallback &callback) {
  UniqueID wait_id = UniqueID::FromRandom();
  RAY_LOG(DEBUG) << "Wait request " << wait_id << " on " << self_node_id_;
  RAY_RETURN_NOT_OK(AddWaitRequest(wait_id, object_ids, owner_addresses, timeout_ms,
                                   num_required_objects, wait_local, callback));
  RAY_RETURN_NOT_OK(LookupRemainingWaitObjects(wait_id));
  // LookupRemainingWaitObjects invokes SubscribeRemainingWaitObjects once lookup has
  // been performed on all remaining objects.
  return ray::Status::OK();
}

ray::Status ObjectManager::AddWaitRequest(
    const UniqueID &wait_id, const std::vector<ObjectID> &object_ids,
    const std::unordered_map<ObjectID, rpc::Address> &owner_addresses, int64_t timeout_ms,
    uint64_t num_required_objects, bool wait_local, const WaitCallback &callback) {
  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  RAY_CHECK(num_required_objects != 0);
  RAY_CHECK(num_required_objects <= object_ids.size())
      << num_required_objects << " " << object_ids.size();
  if (object_ids.size() == 0) {
    callback(std::vector<ObjectID>(), std::vector<ObjectID>());
  }

  // Initialize fields.
  active_wait_requests_.emplace(wait_id, WaitState(*main_service_, timeout_ms, callback));
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  wait_state.object_id_order = object_ids;
  wait_state.owner_addresses = owner_addresses;
  wait_state.timeout_ms = timeout_ms;
  wait_state.num_required_objects = num_required_objects;
  wait_state.wait_local = wait_local;
  for (const auto &object_id : object_ids) {
    if (local_objects_.count(object_id) > 0) {
      wait_state.found.insert(object_id);
    } else {
      wait_state.remaining.insert(object_id);
    }
  }

  return ray::Status::OK();
}

ray::Status ObjectManager::LookupRemainingWaitObjects(const UniqueID &wait_id) {
  auto &wait_state = active_wait_requests_.find(wait_id)->second;

  if (wait_state.remaining.empty()) {
    WaitComplete(wait_id);
  } else {
    // We invoke lookup calls immediately after checking which objects are local to
    // obtain current information about the location of remote objects. Thus,
    // we obtain information about all given objects, regardless of their location.
    // This is required to ensure we do not bias returning locally available objects
    // as ready whenever Wait is invoked with a mixture of local and remote objects.
    for (const auto &object_id : wait_state.remaining) {
      // Lookup remaining objects.
      wait_state.requested_objects.insert(object_id);
      RAY_RETURN_NOT_OK(object_directory_->LookupLocations(
          object_id, wait_state.owner_addresses[object_id],
          [this, wait_id](const ObjectID &lookup_object_id,
                          const std::unordered_set<NodeID> &client_ids,
                          const std::string &spilled_url) {
            auto &wait_state = active_wait_requests_.find(wait_id)->second;
            // Note that the object is guaranteed to be added to local_objects_ before
            // the notification is triggered.
            bool remote_object_ready = !client_ids.empty() || !spilled_url.empty();
            if (local_objects_.count(lookup_object_id) > 0 ||
                (!wait_state.wait_local && remote_object_ready)) {
              wait_state.remaining.erase(lookup_object_id);
              wait_state.found.insert(lookup_object_id);
            }
            RAY_LOG(DEBUG) << "Wait request " << wait_id << ": " << client_ids.size()
                           << " locations found for object " << lookup_object_id;
            wait_state.requested_objects.erase(lookup_object_id);
            if (wait_state.requested_objects.empty()) {
              SubscribeRemainingWaitObjects(wait_id);
            }
          }));
    }
  }
  return ray::Status::OK();
}

void ObjectManager::SubscribeRemainingWaitObjects(const UniqueID &wait_id) {
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  if (wait_state.found.size() >= wait_state.num_required_objects ||
      wait_state.timeout_ms == 0) {
    // Requirements already satisfied.
    WaitComplete(wait_id);
    return;
  }

  // There are objects remaining whose locations we don't know. Request their
  // locations from the object directory.
  for (const auto &object_id : wait_state.object_id_order) {
    if (wait_state.remaining.count(object_id) > 0) {
      RAY_LOG(DEBUG) << "Wait request " << wait_id << ": subscribing to object "
                     << object_id;
      wait_state.requested_objects.insert(object_id);
      // Subscribe to object notifications.
      RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
          wait_id, object_id, wait_state.owner_addresses[object_id],
          [this, wait_id](const ObjectID &subscribe_object_id,
                          const std::unordered_set<NodeID> &client_ids,
                          const std::string &spilled_url) {
            auto object_id_wait_state = active_wait_requests_.find(wait_id);
            if (object_id_wait_state == active_wait_requests_.end()) {
              // Depending on the timing of calls to the object directory, we
              // may get a subscription notification after the wait call has
              // already completed. If so, then don't process the
              // notification.
              return;
            }
            auto &wait_state = object_id_wait_state->second;
            // Note that the object is guaranteed to be added to local_objects_ before
            // the notification is triggered.
            bool remote_object_ready = !client_ids.empty() || !spilled_url.empty();
            if (local_objects_.count(subscribe_object_id) > 0 ||
                (!wait_state.wait_local && remote_object_ready)) {
              RAY_LOG(DEBUG) << "Wait request " << wait_id
                             << ": subscription notification received for object "
                             << subscribe_object_id;
              wait_state.remaining.erase(subscribe_object_id);
              wait_state.found.insert(subscribe_object_id);
              wait_state.requested_objects.erase(subscribe_object_id);
              RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
                  wait_id, subscribe_object_id));
              if (wait_state.found.size() >= wait_state.num_required_objects) {
                WaitComplete(wait_id);
              }
            }
          }));
    }

    // If a timeout was provided, then set a timer. If we don't find locations
    // for enough objects by the time the timer expires, then we will return
    // from the Wait.
    if (wait_state.timeout_ms != -1) {
      auto timeout = boost::posix_time::milliseconds(wait_state.timeout_ms);
      wait_state.timeout_timer->expires_from_now(timeout);
      wait_state.timeout_timer->async_wait(
          [this, wait_id](const boost::system::error_code &error_code) {
            if (error_code.value() != 0) {
              return;
            }
            if (active_wait_requests_.find(wait_id) == active_wait_requests_.end()) {
              // When a subscription callback is triggered first, WaitComplete will be
              // called. The timer may at the same time goes off and may be an
              // interruption will post WaitComplete to main_service_ the second time.
              // This check will avoid the duplicated call of this function.
              return;
            }
            WaitComplete(wait_id);
          });
    }
  }
}

void ObjectManager::WaitComplete(const UniqueID &wait_id) {
  auto iter = active_wait_requests_.find(wait_id);
  RAY_CHECK(iter != active_wait_requests_.end());
  auto &wait_state = iter->second;
  // If we complete with outstanding requests, then timeout_ms should be non-zero or -1
  // (infinite wait time).
  if (!wait_state.requested_objects.empty()) {
    RAY_CHECK(wait_state.timeout_ms > 0 || wait_state.timeout_ms == -1);
  }
  // Unsubscribe to any objects that weren't found in the time allotted.
  for (const auto &object_id : wait_state.requested_objects) {
    RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(wait_id, object_id));
  }
  // Cancel the timer. This is okay even if the timer hasn't been started.
  // The timer handler will be given a non-zero error code. The handler
  // will do nothing on non-zero error codes.
  wait_state.timeout_timer->cancel();
  // Order objects according to input order.
  std::vector<ObjectID> found;
  std::vector<ObjectID> remaining;
  for (const auto &item : wait_state.object_id_order) {
    if (found.size() < wait_state.num_required_objects &&
        wait_state.found.count(item) > 0) {
      found.push_back(item);
    } else {
      remaining.push_back(item);
    }
  }
  wait_state.callback(found, remaining);
  active_wait_requests_.erase(wait_id);
  RAY_LOG(DEBUG) << "Wait request " << wait_id << " finished: found " << found.size()
                 << " remaining " << remaining.size();
}

/// Implementation of ObjectManagerServiceHandler
void ObjectManager::HandlePush(const rpc::PushRequest &request, rpc::PushReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID client_id = NodeID::FromBinary(request.client_id());

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const rpc::Address &owner_address = request.owner_address();
  const std::string &data = request.data();

  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  auto status = ReceiveObjectChunk(client_id, object_id, owner_address, data_size,
                                   metadata_size, chunk_index, data);
  double end_time = absl::GetCurrentTimeNanos() / 1e9;

  HandleReceiveFinished(object_id, client_id, chunk_index, start_time, end_time, status);
  send_reply_callback(status, nullptr, nullptr);
}

ray::Status ObjectManager::ReceiveObjectChunk(const NodeID &client_id,
                                              const ObjectID &object_id,
                                              const rpc::Address &owner_address,
                                              uint64_t data_size, uint64_t metadata_size,
                                              uint64_t chunk_index,
                                              const std::string &data) {
  RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << self_node_id_ << " from " << client_id
                 << " of object " << object_id << " chunk index: " << chunk_index
                 << ", chunk data size: " << data.size()
                 << ", object size: " << data_size;

  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.CreateChunk(object_id, owner_address, data_size, metadata_size,
                               chunk_index);
  ray::Status status;
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;
  if (chunk_status.second.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    std::memcpy(chunk_info.data, data.data(), chunk_info.buffer_length);
    buffer_pool_.SealChunk(object_id, chunk_index);
  } else {
    RAY_LOG(WARNING) << "ReceiveObjectChunk index " << chunk_index << " of object "
                     << object_id << " failed: " << chunk_status.second.message();
    // TODO(hme): If the object isn't local, create a pull request for this chunk.
  }
  return status;
}

void ObjectManager::HandlePull(const rpc::PullRequest &request, rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID client_id = NodeID::FromBinary(request.client_id());
  RAY_LOG(DEBUG) << "Received pull request from client " << client_id << " for object ["
                 << object_id << "].";

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("receive_pull_request");
  profile_event.set_start_time(absl::GetCurrentTimeNanos() / 1e9);
  profile_event.set_end_time(profile_event.start_time());
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + client_id.Hex() +
                               "\"]");
  {
    std::lock_guard<std::mutex> lock(profile_mutex_);
    profile_events_.emplace_back(profile_event);
  }

  main_service_->post([this, object_id, client_id]() { Push(object_id, client_id); });
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::HandleFreeObjects(const rpc::FreeObjectsRequest &request,
                                      rpc::FreeObjectsReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &e : request.object_ids()) {
    object_ids.emplace_back(ObjectID::FromBinary(e));
  }
  FreeObjects(object_ids, /* local_only */ true);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids,
                                bool local_only) {
  buffer_pool_.FreeObjects(object_ids);
  if (!local_only) {
    const auto remote_connections = object_directory_->LookupAllRemoteConnections();
    std::vector<std::shared_ptr<rpc::ObjectManagerClient>> rpc_clients;
    for (const auto &connection_info : remote_connections) {
      auto rpc_client = GetRpcClient(connection_info.client_id);
      if (rpc_client != nullptr) {
        rpc_clients.push_back(rpc_client);
      }
    }
    rpc_service_.post([this, object_ids, rpc_clients]() {
      SpreadFreeObjectsRequest(object_ids, rpc_clients);
    });
  }
}

void ObjectManager::SpreadFreeObjectsRequest(
    const std::vector<ObjectID> &object_ids,
    const std::vector<std::shared_ptr<rpc::ObjectManagerClient>> &rpc_clients) {
  // This code path should be called from node manager.
  rpc::FreeObjectsRequest free_objects_request;
  for (const auto &e : object_ids) {
    free_objects_request.add_object_ids(e.Binary());
  }

  for (auto &rpc_client : rpc_clients) {
    rpc_client->FreeObjects(free_objects_request, [](const Status &status,
                                                     const rpc::FreeObjectsReply &reply) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Send free objects request failed due to" << status.message();
      }
    });
  }
}

std::shared_ptr<rpc::ObjectManagerClient> ObjectManager::GetRpcClient(
    const NodeID &client_id) {
  auto it = remote_object_manager_clients_.find(client_id);
  if (it == remote_object_manager_clients_.end()) {
    RemoteConnectionInfo connection_info(client_id);
    object_directory_->LookupRemoteConnectionInfo(connection_info);
    if (!connection_info.Connected()) {
      return nullptr;
    }
    auto object_manager_client = std::make_shared<rpc::ObjectManagerClient>(
        connection_info.ip, connection_info.port, client_call_manager_);

    RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
                   << ", port: " << connection_info.port
                   << ", local port: " << GetServerPort();

    it = remote_object_manager_clients_
             .emplace(client_id, std::move(object_manager_client))
             .first;
  }
  return it->second;
}

std::shared_ptr<rpc::ProfileTableData> ObjectManager::GetAndResetProfilingInfo() {
  auto profile_info = std::make_shared<rpc::ProfileTableData>();
  profile_info->set_component_type("object_manager");
  profile_info->set_component_id(self_node_id_.Binary());

  {
    std::lock_guard<std::mutex> lock(profile_mutex_);
    for (auto const &profile_event : profile_events_) {
      profile_info->add_profile_events()->CopyFrom(profile_event);
    }
    profile_events_.clear();
  }

  return profile_info;
}

std::string ObjectManager::DebugString() const {
  std::stringstream result;
  result << "ObjectManager:";
  result << "\n- num local objects: " << local_objects_.size();
  result << "\n- num active wait requests: " << active_wait_requests_.size();
  result << "\n- num unfulfilled push requests: " << unfulfilled_push_requests_.size();
  result << "\n- num pull requests: " << pull_requests_.size();
  result << "\n- num buffered profile events: " << profile_events_.size();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << store_notification_->DebugString();
  result << "\n" << buffer_pool_.DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() const {
  stats::ObjectStoreAvailableMemory().Record(config_.object_store_memory - used_memory_);
  stats::ObjectStoreUsedMemory().Record(used_memory_);
  stats::ObjectStoreLocalObjects().Record(local_objects_.size());
  stats::ObjectManagerPullRequests().Record(pull_requests_.size());
}

}  // namespace ray
