#ifndef RAY_RPC_WORKER_SERVER_H
#define RAY_RPC_WORKER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/worker.grpc.pb.h"
#include "src/ray/protobuf/worker.pb.h"

namespace ray {

class CoreWorker;

namespace rpc {

/// The `GrpcServer` for `WorkerService`.
class WorkerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  WorkerGrpcService(boost::asio::io_service &main_service, CoreWorker &core_worker);

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override;

 private:
  /// The grpc async service object.
  WorkerService::AsyncService service_;

  /// The core worker that actually handles the requests.
  CoreWorker &core_worker_;
};

}  // namespace rpc
}  // namespace ray

#endif
