#include "ray/core_worker/future_resolver.h"

namespace ray {

void FutureResolver::ResolveFutureAsync(const ObjectID &object_id, const TaskID &owner_id,
                                        const rpc::Address &owner_address) {
  absl::MutexLock lock(&mu_);
  auto it = owner_clients_.find(owner_id);
  if (it == owner_clients_.end()) {
    auto client = std::shared_ptr<rpc::CoreWorkerClientInterface>(
        client_factory_({owner_address.ip_address(), owner_address.port()}));
    owner_clients_.emplace(owner_id, std::move(client));
  }

  auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
  AttemptFutureResolution(object_id, owner_id, std::move(timer));
}

void FutureResolver::AttemptFutureResolution(
    const ObjectID &object_id, const TaskID &owner_id,
    std::shared_ptr<boost::asio::deadline_timer> timer) {
  auto &owner_client = owner_clients_[owner_id];
  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_id(owner_id.Binary());
  auto status = owner_client->GetObjectStatus(
      request,
      [this, object_id](const Status &status, const rpc::GetObjectStatusReply &reply) {
        if (!status.ok() || reply.status() != rpc::GetObjectStatusReply::PENDING) {
          // Either the owner is gone or the owner replied that the object has
          // been created. In both cases, we can now try to fetch the object via
          // plasma.
          RAY_CHECK_OK(in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                             object_id));
        }
      });
  if (!status.ok()) {
    RAY_CHECK_OK(
        in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  } else {
    timer->expires_from_now(
        boost::posix_time::milliseconds(wait_object_eviction_milliseconds_));
    timer->async_wait(
        [this, object_id, owner_id, timer](const boost::system::error_code &error) {
          if (error == boost::asio::error::operation_aborted) {
            return;  // deadline adjusted
          }
          absl::MutexLock lock(&mu_);
          AttemptFutureResolution(object_id, owner_id, std::move(timer));
        });
  }
}

}  // namespace ray
