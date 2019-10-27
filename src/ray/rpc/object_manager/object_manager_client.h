#ifndef RAY_RPC_OBJECT_MANAGER_CLIENT_H
#define RAY_RPC_OBJECT_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/support/channel_arguments.h>

#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"
#include "src/ray/rpc/client_call.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class ObjectManagerClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  ObjectManagerClient(const std::string &address, const int port,
                      ClientCallManager &client_call_manager, int num_connections = 4)
      : client_call_manager_(client_call_manager), num_connections_(num_connections) {
   RAY_LOG(WARNING) << "Client creates channel with " << num_connections << " tcp connections";
        stubs_.reserve(num_connections);
    for (int i =0; i < num_connections_; i++) {
        grpc::ResourceQuota quota;
        quota.SetMaxThreads(num_connections_);
        grpc::ChannelArguments argument;
    std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials(), argument);
      stubs_.push_back(ObjectManagerService::NewStub(channel));
    }
  };

  /// Push object to remote object manager
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  void Push(const PushRequest &request, const ClientCallback<PushReply> &callback) {
    client_call_manager_.CreateCall<ObjectManagerService, PushRequest, PushReply>(
        *stubs_[rand() % num_connections_], &ObjectManagerService::Stub::PrepareAsyncPush, request, callback);
  }

  /// Pull object from remote object manager
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply from server
  void Pull(const PullRequest &request, const ClientCallback<PullReply> &callback) {
    client_call_manager_.CreateCall<ObjectManagerService, PullRequest, PullReply>(
        *stubs_[rand() % num_connections_], &ObjectManagerService::Stub::PrepareAsyncPull, request, callback);
  }

  /// Tell remote object manager to free objects
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  void FreeObjects(const FreeObjectsRequest &request,
                   const ClientCallback<FreeObjectsReply> &callback) {
    client_call_manager_
        .CreateCall<ObjectManagerService, FreeObjectsRequest, FreeObjectsReply>(
            *stubs_[rand() % num_connections_], &ObjectManagerService::Stub::PrepareAsyncFreeObjects, request,
            callback);
  }

 private:
  int num_connections_;

  /// The gRPC-generated stub.
  std::vector<std::unique_ptr<ObjectManagerService::Stub>> stubs_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_OBJECT_MANAGER_CLIENT_H
