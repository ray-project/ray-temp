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

#include <algorithm>
#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"

namespace ray {

class PushManager {
 public:
  /// Manages rate limiting and deduplication of outbound object pushes.

  /// Create a push manager.
  ///
  /// \param max_chunks_in_flight Max number of chunks allowed to be in flight
  ///                             from this PushManager (this raylet).
  PushManager(int64_t max_chunks_in_flight)
      : max_chunks_in_flight_(max_chunks_in_flight) {
    RAY_CHECK(max_chunks_in_flight_ > 0) << max_chunks_in_flight_;
  };

  /// Start pushing an object subject to max chunks in flight limit.
  ///
  /// Duplicate concurrent pushes to the same destination will be suppressed.
  ///
  /// \param dest_id The node to send to.
  /// \param obj_id The object to send.
  /// \param num_chunks The total number of chunks to send.
  /// \param send_chunk_fn This function will be called with args 0...{num_chunks-1}.
  ///                      The caller promises to call PushManager::OnChunkComplete()
  ///                      once a call to send_chunk_fn finishes.
  void StartPush(const NodeID &dest_id, const ObjectID &obj_id, int64_t num_chunks,
                 std::function<void(int64_t)> send_chunk_fn);

  /// Called every time a chunk completes to trigger additional sends.
  /// TODO(ekl) maybe we should cancel the entire push on error.
  void OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id);

  /// Return the number of chunks currently in flight. For testing only.
  int64_t NumChunksInFlight() const { return chunks_in_flight_; };

  /// Return the number of chunks remaining. For testing only.
  int64_t NumChunksRemaining() const {
    int total = 0;
    for (const auto &pair : chunks_remaining_) {
      total += pair.second;
    }
    return total;
  }

  /// Return the number of pushes currently in flight. For testing only.
  int64_t NumPushesInFlight() const { return push_info_.size(); };

  std::string DebugString() const {
    std::stringstream result;
    result << "PushManager:";
    result << "\n- num pushes in flight: " << NumPushesInFlight();
    result << "\n- num chunks in flight: " << NumChunksInFlight();
    result << "\n- num chunks remaining: " << NumChunksRemaining();
    result << "\n- max chunks allowed: " << max_chunks_in_flight_;
    return result.str();
  }

 private:
  /// Called on completion events to trigger additional pushes.
  void ScheduleRemainingPushes();

  /// Pair of (destination, object_id).
  typedef std::pair<NodeID, ObjectID> PushID;

  /// Info about the pushed object: (num_chunks_total, chunk_send_fn).
  typedef std::pair<int64_t, std::function<void(int64_t)>> PushInfo;

  /// Max number of chunks in flight allowed.
  const int64_t max_chunks_in_flight_;

  /// Running count of chunks in flight, used to limit progress of in_flight_pushes_.
  int64_t chunks_in_flight_ = 0;

  /// Tracks all pushes with chunk transfers in flight.
  absl::flat_hash_map<PushID, PushInfo> push_info_;

  /// Tracks progress of in flight pushes.
  absl::flat_hash_map<PushID, int64_t> next_chunk_id_;

  /// Tracks number of unfinished chunk sends.
  absl::flat_hash_map<PushID, int64_t> chunks_remaining_;
};

}  // namespace ray
