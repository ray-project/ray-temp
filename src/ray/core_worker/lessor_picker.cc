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

#include "ray/core_worker/lessor_picker.h"

namespace ray {

absl::optional<rpc::Address> LessorPicker::GetBestNodeForTask(
    const TaskSpecification &spec) {
  if (auto node_id = GetBestNodeIdForTask(spec)) {
    return node_addr_factory_(node_id.value());
  }
  return absl::nullopt;
}

absl::optional<NodeID> LessorPicker::GetBestNodeIdForTask(const TaskSpecification &spec) {
  return GetBestNodeIdForObjects(spec.GetDependencyIds());
}

/// Criteria for "best" node: The node with the most object bytes (from object_ids) local.
absl::optional<NodeID> LessorPicker::GetBestNodeIdForObjects(
    const std::vector<ObjectID> &object_ids) {
  // Number of object bytes (from object_ids) that a given node has local.
  absl::flat_hash_map<NodeID, uint64_t> bytes_local_table;
  uint64_t max_bytes = 0;
  absl::optional<NodeID> max_bytes_node;
  // Finds the node with the maximum number of object bytes local.
  for (const ObjectID &object_id : object_ids) {
    if (auto locality_data = locality_data_provider_->GetLocalityData(object_id)) {
      for (const NodeID &node_id : locality_data->nodes_containing_object) {
        auto &bytes = bytes_local_table[node_id];
        bytes += locality_data->object_size;
        // Update max, if needed.
        if (bytes > max_bytes) {
          max_bytes = bytes;
          max_bytes_node = node_id;
        }
      }
    } else {
      RAY_LOG(WARNING) << "No locality data available for object " << object_id
                       << ", won't be included in locality cost";
    }
  }
  return max_bytes_node;
}

}  // namespace ray
