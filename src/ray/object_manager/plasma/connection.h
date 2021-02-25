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

#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/compat.h"

namespace plasma {

namespace flatbuf {
enum class MessageType : int64_t;
}

class Client;

using PlasmaStoreMessageHandler = std::function<ray::Status(
    std::shared_ptr<Client>, flatbuf::MessageType, const std::vector<uint8_t> &)>;

class ClientInterface {
 public:
  virtual ~ClientInterface() {}
};

/// Contains all information that is associated with a Plasma store client.
class Client : public ray::ClientConnection, public ClientInterface {
 public:
  static std::shared_ptr<Client> Create(PlasmaStoreMessageHandler message_handler,
                                        ray::local_stream_socket &&socket);

  ray::Status SendFd(MEMFD_TYPE fd);

  /// Object ids that are used by this client.
  std::unordered_set<ray::ObjectID> object_ids;

  std::string name = "anonymous_client";

 private:
  Client(ray::MessageHandler &message_handler, ray::local_stream_socket &&socket);
  /// File descriptors that are used by this client.
  std::unordered_set<MEMFD_TYPE> used_fds_;
};

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<Client> &client);

/// Contains all information that is associated with a Plasma store client.
class StoreConn : public ray::ServerConnection {
 public:
  StoreConn(ray::local_stream_socket &&socket);

  /// Receive a file descriptor for the store.
  ///
  /// \return A file descriptor.
  ray::Status RecvFd(MEMFD_TYPE *fd);
};

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<StoreConn> &store_conn);

}  // namespace plasma
