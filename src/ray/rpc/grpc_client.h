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

#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

// This macro wraps the logic to call a specific RPC method of a service,
// to make it easier to implement a new RPC client.
#define INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client) \
  ({                                                                    \
    rpc_client->CallMethod<METHOD##Request, METHOD##Reply>(             \
        &SERVICE::Stub::PrepareAsync##METHOD, request, callback);       \
  })

// Define a void RPC client method.
#define VOID_RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS)               \
  void METHOD(const METHOD##Request &request,                                    \
              const ClientCallback<METHOD##Reply> &callback) SPECS {             \
    RAY_UNUSED(INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client)); \
  }

// Define a RPC client method that returns ray::Status.
#define RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS)               \
  ray::Status METHOD(const METHOD##Request &request,                        \
                     const ClientCallback<METHOD##Reply> &callback) SPECS { \
    return INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client); \
  }

template <class GrpcService>
class GrpcClient {
 public:
  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager)
      : client_call_manager_(call_manager) {
    grpc::ChannelArguments argument;
    // Disable http proxy since it disrupts local connections. TODO(ekl) we should make
    // this configurable, or selectively set it for known local connections only.
    argument.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument.SetMaxSendMessageSize(RayConfig::instance().max_grpc_message_size());
    argument.SetMaxReceiveMessageSize(RayConfig::instance().max_grpc_message_size());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument);
    stub_ = GrpcService::NewStub(channel);
  }

  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager,
             int num_threads)
      : client_call_manager_(call_manager) {
    grpc::ResourceQuota quota;
    quota.SetMaxThreads(num_threads);
    grpc::ChannelArguments argument;
    argument.SetResourceQuota(quota);
    argument.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument.SetMaxSendMessageSize(RayConfig::instance().max_grpc_message_size());
    argument.SetMaxReceiveMessageSize(RayConfig::instance().max_grpc_message_size());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument);
    stub_ = GrpcService::NewStub(channel);
  }

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return Status.
  template <class Request, class Reply>
  ray::Status CallMethod(
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {
    uint8_t retries = 0;
    grpc::Status status;
    for (uint8_t retries = 0; retries < 5; ++retries) {
      status = client_call_manager_
                   .CreateCall<GrpcService, Request, Reply>(
                       *stub_, prepare_async_function, request, callback)
                   ->GetStatus();
      // Retry requests that failed with a transient error.
      // https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
      if (status.error_code() == grpc::UNAVAILABLE) {
        // Exponential backoff.
        uint64_t delay = 2 ^ retries * 100;
        RAY_LOG(WARNING) << "RPC got status UNAVAILABLE, retrying after " << delay
                         << "ms...";
        usleep(delay * 1000);
      } else {
        break;
      }
    }
    return GrpcStatusToRayStatus(status);
  }

 private:
  ClientCallManager &client_call_manager_;
  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;
};

}  // namespace rpc
}  // namespace ray

#endif
