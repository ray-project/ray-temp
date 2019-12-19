#ifndef RAY_RPC_GCS_RPC_SERVER_H
#define RAY_RPC_GCS_RPC_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

#define JOB_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY)                           \
  std::unique_ptr<ServerCallFactory> HANDLER##_call_factory(                         \
      new ServerCallFactoryImpl<JobInfoGcsService, JobInfoHandler, HANDLER##Request, \
                                HANDLER##Reply>(                                     \
          service_, &JobInfoGcsService::AsyncService::Request##HANDLER,              \
          service_handler_, &JobInfoHandler::Handle##HANDLER, cq, main_service_));   \
  server_call_factories_and_concurrencies->emplace_back(                             \
      std::move(HANDLER##_call_factory), CONCURRENCY);

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY)                             \
  std::unique_ptr<ServerCallFactory> HANDLER##_call_factory(                             \
      new ServerCallFactoryImpl<ActorInfoGcsService, ActorInfoHandler, HANDLER##Request, \
                                HANDLER##Reply>(                                         \
          service_, &ActorInfoGcsService::AsyncService::Request##HANDLER,                \
          service_handler_, &ActorInfoHandler::Handle##HANDLER, cq, main_service_));     \
  server_call_factories_and_concurrencies->emplace_back(                                 \
      std::move(HANDLER##_call_factory), CONCURRENCY);

class JobInfoHandler {
 public:
  virtual ~JobInfoHandler() = default;

  virtual void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `JobInfoGcsService`.
class JobInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoGrpcService(boost::asio::io_service &io_service,
                              JobInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    JOB_INFO_SERVICE_RPC_HANDLER(AddJob, 1);
    JOB_INFO_SERVICE_RPC_HANDLER(MarkJobFinished, 1);
  }

 private:
  /// The grpc async service object.
  JobInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  JobInfoHandler &service_handler_;
};

class ActorInfoHandler {
 public:
  virtual ~ActorInfoHandler() = default;

  virtual void HandleGetActorInfo(const GetActorInfoRequest &request,
                                  GetActorInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRegisterActorInfo(const RegisterActorInfoRequest &request,
                                       RegisterActorInfoReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateActorInfo(const UpdateActorInfoRequest &request,
                                     UpdateActorInfoReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ActorInfoGcsService`.
class ActorInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ActorInfoGrpcService(boost::asio::io_service &io_service,
                                ActorInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetActorInfo, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(RegisterActorInfo, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(UpdateActorInfo, 1);
  }

 private:
  /// The grpc async service object.
  ActorInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ActorInfoHandler &service_handler_;
};

class NodeInfoHandler {
 public:
  virtual ~NodeInfoHandler() = default;

  virtual void HandleRegisterNodeInfo(const RegisterNodeInfoRequest &request,
                                      RegisterNodeInfoReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUnregisterNodeInfo(const UnregisterNodeInfoRequest &request,
                                        UnregisterNodeInfoReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllNodesInfo(const GetAllNodesInfoRequest &request,
                                     GetAllNodesInfoReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeInfoGcsService`.
class NodeInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeInfoGrpcService(boost::asio::io_service &io_service,
                               NodeInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    std::unique_ptr<ServerCallFactory> register_node_info_call_factory(
        new ServerCallFactoryImpl<NodeInfoGcsService, NodeInfoHandler,
                                  RegisterNodeInfoRequest, RegisterNodeInfoReply>(
            service_, &NodeInfoGcsService::AsyncService::RequestRegisterNodeInfo,
            service_handler_, &NodeInfoHandler::HandleRegisterNodeInfo, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(register_node_info_call_factory), 1);

    std::unique_ptr<ServerCallFactory> unregister_node_info_call_factory(
        new ServerCallFactoryImpl<NodeInfoGcsService, NodeInfoHandler,
                                  UnregisterNodeInfoRequest, UnregisterNodeInfoReply>(
            service_, &NodeInfoGcsService::AsyncService::RequestUnregisterNodeInfo,
            service_handler_, &NodeInfoHandler::HandleUnregisterNodeInfo, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(unregister_node_info_call_factory), 1);

    std::unique_ptr<ServerCallFactory> get_all_actors_info_call_factory(
        new ServerCallFactoryImpl<NodeInfoGcsService, NodeInfoHandler,
                                  GetAllNodesInfoRequest, GetAllNodesInfoReply>(
            service_, &NodeInfoGcsService::AsyncService::RequestGetAllNodesInfo,
            service_handler_, &NodeInfoHandler::HandleGetAllNodesInfo, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(get_all_actors_info_call_factory), 1);
  }

 private:
  /// The grpc async service object.
  NodeInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  NodeInfoHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_SERVER_H
