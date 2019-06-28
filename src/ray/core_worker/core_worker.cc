#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type,
                       const ray::rpc::Language language, const std::string &store_socket,
                       const std::string &raylet_socket, const JobID &job_id)
    : worker_type_(worker_type),
      language_(language),
      store_socket_(store_socket),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      // TODO(qwang): JobId parameter can be removed once we embed jobId in driverId.
      raylet_client_(raylet_socket_,
                     ClientID::FromBinary(worker_context_.GetWorkerID().Binary()),
                     (worker_type_ == ray::WorkerType::WORKER),
                     worker_context_.GetCurrentJobID(), ToRayletTaskLanguage(language_)),
      task_interface_(*this),
      object_interface_(*this),
      task_execution_interface_(*this) {
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this needs to be changed
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  auto status = store_client_.Connect(store_socket_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Connecting plasma store failed when trying to construct"
                   << " core worker: " << status.message();
    throw std::runtime_error(status.message());
  }
}

}  // namespace ray
