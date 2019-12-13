#ifndef RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ActorInfoHandler`.
class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleGetActorInfo(const GetActorInfoRequest &request, GetActorInfoReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleRegisterActorInfo(const RegisterActorInfoRequest &request,
                               RegisterActorInfoReply *reply,
                               SendReplyCallback send_reply_callback) override;

  void HandleUpdateActorInfo(const UpdateActorInfoRequest &request,
                             UpdateActorInfoReply *reply,
                             SendReplyCallback send_reply_callback) override;

  uint64_t GetAsyncGetCount() { return metrics_[ASYNC_GET]; }

  uint64_t GetAsyncRegisterCount() { return metrics_[ASYNC_REGISTER]; }

  uint64_t GetAsyncUpdateCount() { return metrics_[ASYNC_UPDATE]; }

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
