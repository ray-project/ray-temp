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

#include "ray/object_manager/ownership_based_object_directory.h"

namespace ray {

OwnershipBasedObjectDirectory::OwnershipBasedObjectDirectory(
    boost::asio::io_service &io_service, std::shared_ptr<gcs::GcsClient> &gcs_client)
    : ObjectDirectory(io_service, gcs_client),
      client_call_manager_(io_service) {}

namespace {

/// Filter out the removed clients from the object locations.
void FilterRemovedClients(std::shared_ptr<gcs::GcsClient> gcs_client,
                          std::unordered_set<ClientID> *node_ids) {
  for (auto it = node_ids->begin(); it != node_ids->end();) {
    if (gcs_client->Nodes().IsRemoved(*it)) {
      it = node_ids->erase(it);
    } else {
      it++;
    }
  }
}

}  // namespace

ray::Status OwnershipBasedObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  WorkerID worker_id = WorkerID::FromBinary(object_info.owner_worker_id);
  auto it = worker_rpc_clients_.find(worker_id);
  if (it == worker_rpc_clients_.end()) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(object_info.owner_raylet_id);
    owner_address.set_ip_address(object_info.owner_ip_address);
    owner_address.set_port(object_info.owner_port);
    owner_address.set_worker_id(object_info.owner_worker_id);
    auto client = std::unique_ptr<rpc::CoreWorkerClient>(
        new rpc::CoreWorkerClient(owner_address, client_call_manager_));
    it = worker_rpc_clients_
             .emplace(worker_id,
                      std::make_pair<std::unique_ptr<rpc::CoreWorkerClient>, size_t>(
                          std::move(client), 0))
             .first;
  }

  rpc::AddObjectLocationOwnerRequest request;
  request.set_intended_worker_id(object_info.owner_worker_id);
  request.set_object_id(object_id.Binary());
  request.set_client_id(client_id.Binary());

  worker_rpc_clients_[worker_id].second++;
  RAY_CHECK_OK(it->second.first->AddObjectLocationOwner(
      request, [this, worker_id, object_id](
                   Status status, const rpc::AddObjectLocationOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Worker " << worker_id << " failed to add the location for "
                         << object_id;
        }
        // Remove the cached worker client if there are no more pending requests.
        if (--worker_rpc_clients_[worker_id].second == 0) {
          worker_rpc_clients_.erase(worker_id);
        }
      }));
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::ReportObjectRemoved(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  WorkerID worker_id = WorkerID::FromBinary(object_info.owner_worker_id);
  auto it = worker_rpc_clients_.find(worker_id);
  if (it == worker_rpc_clients_.end()) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(object_info.owner_raylet_id);
    owner_address.set_ip_address(object_info.owner_ip_address);
    owner_address.set_port(object_info.owner_port);
    owner_address.set_worker_id(object_info.owner_worker_id);
    auto client = std::unique_ptr<rpc::CoreWorkerClient>(
        new rpc::CoreWorkerClient(owner_address, client_call_manager_));
    it = worker_rpc_clients_
             .emplace(worker_id,
                      std::make_pair<std::unique_ptr<rpc::CoreWorkerClient>, size_t>(
                          std::move(client), 0))
             .first;
  }

  rpc::RemoveObjectLocationOwnerRequest request;
  request.set_intended_worker_id(object_info.owner_worker_id);
  request.set_object_id(object_id.Binary());
  request.set_client_id(client_id.Binary());

  worker_rpc_clients_[worker_id].second++;
  RAY_CHECK_OK(it->second.first->RemoveObjectLocationOwner(
      request, [this, worker_id, object_id](
                   Status status, const rpc::RemoveObjectLocationOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Worker " << worker_id
                         << " failed to remove the location for " << object_id;
        }
        // Remove the cached worker client if there are no more pending requests.
        if (--worker_rpc_clients_[worker_id].second == 0) {
          worker_rpc_clients_.erase(worker_id);
        }
      }));
  return Status::OK();
};

void OwnershipBasedObjectDirectory::SubscriptionCallback(
    ObjectID object_id, WorkerID worker_id, Status status,
    const rpc::GetObjectLocationsOwnerReply &reply) {
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    // Remove the cached worker client if there are no more pending requests.
    if (--worker_rpc_clients_[worker_id].second == 0) {
      worker_rpc_clients_.erase(worker_id);
    }
    return;
  }

  std::unordered_set<ClientID> client_ids;
  for (auto const &client_id : reply.client_ids()) {
    client_ids.emplace(ClientID::FromBinary(client_id));
  }
  FilterRemovedClients(gcs_client_, &client_ids);
  if (client_ids != it->second.current_object_locations) {
    it->second.current_object_locations = std::move(client_ids);
    auto callbacks = it->second.callbacks;
    // Call all callbacks associated with the object id locations we have
    // received.  This notifies the client even if the list of locations is
    // empty, since this may indicate that the objects have been evicted from
    // all nodes.
    for (const auto &callback_pair : callbacks) {
      // It is safe to call the callback directly since this is already running
      // in the subscription callback stack.
      callback_pair.second(object_id, it->second.current_object_locations);
    }
  }

  auto worker_it = worker_rpc_clients_.find(worker_id);
  rpc::GetObjectLocationsOwnerRequest request;
  request.set_intended_worker_id(worker_id.Binary());
  request.set_object_id(object_id.Binary());
  RAY_CHECK_OK(worker_it->second.first->GetObjectLocationsOwner(
      request,
      std::bind(&OwnershipBasedObjectDirectory::SubscriptionCallback, this, object_id,
                worker_id, std::placeholders::_1, std::placeholders::_2)));
}

ray::Status OwnershipBasedObjectDirectory::SubscribeObjectLocations(
    const UniqueID &callback_id, const ObjectID &object_id,
    const rpc::Address &owner_address, const OnLocationsFound &callback) {
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    it = listeners_.emplace(object_id, LocationListenerState()).first;
    WorkerID worker_id = WorkerID::FromBinary(owner_address.worker_id());
    auto worker_it = worker_rpc_clients_.find(worker_id);
    if (worker_it == worker_rpc_clients_.end()) {
      auto client = std::unique_ptr<rpc::CoreWorkerClient>(
          new rpc::CoreWorkerClient(owner_address, client_call_manager_));
      worker_it =
          worker_rpc_clients_
              .emplace(worker_id,
                       std::make_pair<std::unique_ptr<rpc::CoreWorkerClient>, size_t>(
                           std::move(client), 0))
              .first;
    }
    worker_rpc_clients_[worker_id].second++;
    rpc::GetObjectLocationsOwnerRequest request;
    request.set_intended_worker_id(owner_address.worker_id());
    request.set_object_id(object_id.Binary());
    RAY_CHECK_OK(worker_it->second.first->GetObjectLocationsOwner(
        request,
        std::bind(&OwnershipBasedObjectDirectory::SubscriptionCallback, this, object_id,
                  worker_id, std::placeholders::_1, std::placeholders::_2)));
  }
  auto &listener_state = it->second;

  if (listener_state.callbacks.count(callback_id) > 0) {
    return Status::OK();
  }
  listener_state.callbacks.emplace(callback_id, callback);
  // If we previously received some notifications about the object's locations,
  // immediately notify the caller of the current known locations.
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::UnsubscribeObjectLocations(
    const UniqueID &callback_id, const ObjectID &object_id) {
  auto entry = listeners_.find(object_id);
  if (entry == listeners_.end()) {
    return Status::OK();
  }
  entry->second.callbacks.erase(callback_id);
  if (entry->second.callbacks.empty()) {
    listeners_.erase(entry);
  }
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::LookupLocations(
    const ObjectID &object_id, const rpc::Address &owner_address,
    const OnLocationsFound &callback) {
  WorkerID worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto it = worker_rpc_clients_.find(worker_id);
  if (it == worker_rpc_clients_.end()) {
    auto client = std::unique_ptr<rpc::CoreWorkerClient>(
        new rpc::CoreWorkerClient(owner_address, client_call_manager_));
    it = worker_rpc_clients_
             .emplace(worker_id,
                      std::make_pair<std::unique_ptr<rpc::CoreWorkerClient>, size_t>(
                          std::move(client), 0))
             .first;
  }

  rpc::GetObjectLocationsOwnerRequest request;
  request.set_intended_worker_id(owner_address.worker_id());
  request.set_object_id(object_id.Binary());

  worker_rpc_clients_[worker_id].second++;
  RAY_CHECK_OK(it->second.first->GetObjectLocationsOwner(
      request, [this, worker_id, object_id, callback](
                   Status status, const rpc::GetObjectLocationsOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Worker " << worker_id << " failed to get the location for "
                         << object_id;
        }
        std::unordered_set<ClientID> client_ids;
        for (auto const &client_id : reply.client_ids()) {
          client_ids.emplace(ClientID::FromBinary(client_id));
        }
        FilterRemovedClients(gcs_client_, &client_ids);
        callback(object_id, client_ids);
        // Remove the cached worker client if there are no more pending requests.
        if (--worker_rpc_clients_[worker_id].second == 0) {
          worker_rpc_clients_.erase(worker_id);
        }
      }));
  return Status::OK();
}

std::string OwnershipBasedObjectDirectory::DebugString() const { return ""; }

}  // namespace ray
