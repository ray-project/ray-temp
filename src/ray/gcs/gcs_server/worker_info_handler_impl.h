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

#include "gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `WorkerInfoHandler`.
class DefaultWorkerInfoHandler : public rpc::WorkerInfoHandler {
 public:
  explicit DefaultWorkerInfoHandler(
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub)
      : gcs_table_storage_(gcs_table_storage), gcs_pub_sub_(gcs_pub_sub) {}

  void HandleReportWorkerFailure(const ReportWorkerFailureRequest &request,
                                 ReportWorkerFailureReply *reply,
                                 SendReplyCallback send_reply_callback) override;

  void HandleRegisterWorker(const RegisterWorkerRequest &request,
                            RegisterWorkerReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
};

}  // namespace rpc
}  // namespace ray
