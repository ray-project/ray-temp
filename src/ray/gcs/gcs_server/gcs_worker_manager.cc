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

#include "gcs_worker_manager.h"

namespace ray {
namespace gcs {

void GcsWorkerManager::HandleReportWorkerFailure(
    const rpc::ReportWorkerFailureRequest &request, rpc::ReportWorkerFailureReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const rpc::Address worker_address = request.worker_failure().worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  auto worker_failure_data = std::make_shared<WorkerTableData>();
  worker_failure_data->CopyFrom(request.worker_failure());
  worker_failure_data->set_is_worker_failure(true);
  const auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
  auto on_done = [this, worker_address, worker_id, worker_failure_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report worker failure, "
                     << worker_address.DebugString();
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(WORKER_CHANNEL, worker_id.Binary(),
                                         worker_failure_data->SerializeAsString(),
                                         nullptr));
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_table_storage_->WorkerTable().Put(worker_id, *worker_failure_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsWorkerManager::HandleRegisterWorker(const rpc::RegisterWorkerRequest &request,
                                            rpc::RegisterWorkerReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  auto worker_type = request.worker_type();
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  auto worker_info = MapFromProtobuf(request.worker_info());

  auto register_worker_data = std::make_shared<WorkerTableData>();
  register_worker_data->set_is_worker_failure(false);
  register_worker_data->set_worker_type(worker_type);
  register_worker_data->mutable_worker_address()->set_worker_id(worker_id.Binary());
  register_worker_data->mutable_worker_info()->insert(worker_info.begin(),
                                                      worker_info.end());

  auto on_done = [worker_id, reply, send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register worker " << worker_id;
    } else {
      RAY_LOG(DEBUG) << "Finished registering worker " << worker_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status =
      gcs_table_storage_->WorkerTable().Put(worker_id, *register_worker_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsWorkerManager::HandleGetWorkerInfo(const rpc::GetWorkerInfoRequest &request,
                                           rpc::GetWorkerInfoReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  WorkerID worker_id = WorkerID::FromBinary(request.worker_id());
  RAY_LOG(DEBUG) << "Getting worker info, worker id = " << worker_id;

  auto on_done = [worker_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<WorkerTableData> &result) {
    if (result) {
      reply->mutable_worker_table_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting worker info, worker id = " << worker_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->WorkerTable().Get(worker_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void GcsWorkerManager::HandleAddWorkerInfo(const rpc::AddWorkerInfoRequest &request,
                                           rpc::AddWorkerInfoReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  auto worker_data = std::make_shared<WorkerTableData>();
  worker_data->CopyFrom(request.worker_data());
  const auto worker_id = WorkerID::FromBinary(worker_data->worker_address().worker_id());
  auto on_done = [worker_data, reply, send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add worker information, "
                     << worker_data->DebugString();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->WorkerTable().Put(worker_id, *worker_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace gcs
}  // namespace ray
