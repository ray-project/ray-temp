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

#include "ray/object_manager/plasma/create_request_queue.h"

#include <stdlib.h>

#include <memory>

#include "ray/object_manager/plasma/common.h"
#include "ray/util/asio_util.h"
#include "ray/util/util.h"

namespace plasma {

uint64_t CreateRequestQueue::AddRequest(const ObjectID &object_id,
                                        const std::shared_ptr<ClientInterface> &client,
                                        const CreateObjectCallback &create_callback) {
  auto req_id = next_req_id_++;
  fulfilled_requests_[req_id] = nullptr;
  queue_.emplace_back(new CreateRequest(object_id, req_id, client, create_callback));
  return req_id;
}

bool CreateRequestQueue::GetRequestResult(uint64_t req_id, PlasmaObject *result,
                                          PlasmaError *error) {
  auto it = fulfilled_requests_.find(req_id);
  if (it == fulfilled_requests_.end()) {
    RAY_LOG(ERROR)
        << "Object store client requested the result of a previous request to create an "
           "object, but the result has already been returned to the client. This client "
           "may hang because the creation request cannot be fulfilled.";
    *error = PlasmaError::UnexpectedError;
    return true;
  }

  if (!it->second) {
    return false;
  }

  *result = it->second->result;
  *error = it->second->error;
  fulfilled_requests_.erase(it);
  return true;
}

std::pair<PlasmaObject, PlasmaError> CreateRequestQueue::TryRequestImmediately(
    const ObjectID &object_id, const std::shared_ptr<ClientInterface> &client,
    const CreateObjectCallback &create_callback) {
  PlasmaObject result = {};

  if (!queue_.empty()) {
    // There are other requests queued. Return an out-of-memory error
    // immediately because this request cannot be served.
    return {result, PlasmaError::OutOfMemory};
  }

  auto req_id = AddRequest(object_id, client, create_callback);
  if (!ProcessRequests().ok()) {
    // If the request was not immediately fulfillable, finish it.
    RAY_CHECK(!queue_.empty());
    FinishRequest(queue_.begin());
  }
  PlasmaError error;
  RAY_CHECK(GetRequestResult(req_id, &result, &error));
  return {result, error};
}

Status CreateRequestQueue::ProcessRequest(std::unique_ptr<CreateRequest> &request) {
  // Return an OOM error to the client if we have hit the maximum number of
  // retries.
  bool evict_if_full = evict_if_full_;
  if (max_retries_ == 0) {
    // If we cannot retry, then always evict on the first attempt.
    evict_if_full = true;
  } else if (num_retries_ > 0) {
    // Always try to evict after the first attempt.
    evict_if_full = true;
  }

  request->error = request->create_callback(evict_if_full, &request->result);
  Status status;
  if (request->error != PlasmaError::OK) {
    status =
        Status::TransientObjectStoreFull("Object store full, queueing creation request");
  }

  return status;
}

Status CreateRequestQueue::ProcessRequests() {
  while (!queue_.empty()) {
    auto request_it = queue_.begin();
    auto status = ProcessRequest(*request_it);
    if (status.IsTransientObjectStoreFull() || status.IsObjectStoreFull()) {
      if (!spill_objects_callback_()) {
        RAY_LOG(ERROR) << "Cannot spill any more, raise OOM.";
        FinishRequest(request_it);
        status = Status::ObjectStoreFull("Object store full.");
      }
      return status;
    }
    FinishRequest(request_it);
  }
  // SANG-TODO Get memory usage callback here.
  return Status::OK();
}

void CreateRequestQueue::FinishRequest(
    std::list<std::unique_ptr<CreateRequest>>::iterator request_it) {
  // Fulfill the request.
  auto &request = *request_it;
  auto it = fulfilled_requests_.find(request->request_id);
  RAY_CHECK(it != fulfilled_requests_.end());
  RAY_CHECK(it->second == nullptr);
  it->second = std::move(request);
  queue_.erase(request_it);

  // Reset the number of retries since we are no longer trying this request.
  num_retries_ = 0;
}

void CreateRequestQueue::RemoveDisconnectedClientRequests(
    const std::shared_ptr<ClientInterface> &client) {
  for (auto it = queue_.begin(); it != queue_.end();) {
    if ((*it)->client == client) {
      fulfilled_requests_.erase((*it)->request_id);
      it = queue_.erase(it);
    } else {
      it++;
    }
  }

  for (auto it = fulfilled_requests_.begin(); it != fulfilled_requests_.end();) {
    if (it->second && it->second->client == client) {
      fulfilled_requests_.erase(it);
    }
    it++;
  }
}

void CreateRequestQueue::TriggerGlobalGCIfNeeded() {
  // Invoke only once per 10 seconds.
  if (trigger_global_gc_ && current_time_ms() - last_global_gc_ms_ > 10000) {
    trigger_global_gc_();
    last_global_gc_ms_ = current_time_ms();
  }
}

}  // namespace plasma
