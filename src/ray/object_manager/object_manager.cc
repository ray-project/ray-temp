#include "ray/object_manager/object_manager.h"

namespace asio = boost::asio;

namespace object_manager_protocol = ray::object_manager::protocol;

namespace ray {

ObjectManager::ObjectManager(asio::io_service &main_service,
                             const ObjectManagerConfig &config,
                             std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    // TODO(hme): Eliminate knowledge of GCS.
    : client_id_(gcs_client->client_table().GetLocalClientId()),
      config_(config),
      object_directory_(new ObjectDirectory(gcs_client)),
      store_notification_(main_service, config_.store_socket_name),
      // release_delay of 2 * config_.max_sends is to ensure the pool does not release
      // an object prematurely whenever we reach the maximum number of sends.
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size,
                   /*release_delay=*/2 * config_.max_sends),
      send_work_(send_service_),
      receive_work_(receive_service_),
      connection_pool_() {
  RAY_CHECK(config_.max_sends > 0);
  RAY_CHECK(config_.max_receives > 0);
  RAY_CHECK(config_.max_push_retries > 0);
  main_service_ = &main_service;
  store_notification_.SubscribeObjAdded(
      [this](const ObjectInfoT &object_info) { NotifyDirectoryObjectAdd(object_info); });
  store_notification_.SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
}

ObjectManager::ObjectManager(asio::io_service &main_service,
                             const ObjectManagerConfig &config,
                             std::unique_ptr<ObjectDirectoryInterface> od)
    : config_(config),
      object_directory_(std::move(od)),
      store_notification_(main_service, config_.store_socket_name),
      // release_delay of 2 * config_.max_sends is to ensure the pool does not release
      // an object prematurely whenever we reach the maximum number of sends.
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size,
                   /*release_delay=*/2 * config_.max_sends),
      send_work_(send_service_),
      receive_work_(receive_service_),
      connection_pool_() {
  RAY_CHECK(config_.max_sends > 0);
  RAY_CHECK(config_.max_receives > 0);
  RAY_CHECK(config_.max_push_retries > 0);
  // TODO(hme) Client ID is never set with this constructor.
  main_service_ = &main_service;
  store_notification_.SubscribeObjAdded(
      [this](const ObjectInfoT &object_info) { NotifyDirectoryObjectAdd(object_info); });
  store_notification_.SubscribeObjDeleted(
      [this](const ObjectID &oid) { NotifyDirectoryObjectDeleted(oid); });
  StartIOService();
}

ObjectManager::~ObjectManager() { StopIOService(); }

void ObjectManager::RegisterGcs() { object_directory_->RegisterBackend(); }

void ObjectManager::StartIOService() {
  for (int i = 0; i < config_.max_sends; ++i) {
    send_threads_.emplace_back(std::thread(&ObjectManager::RunSendService, this));
  }
  for (int i = 0; i < config_.max_receives; ++i) {
    receive_threads_.emplace_back(std::thread(&ObjectManager::RunReceiveService, this));
  }
}

void ObjectManager::RunSendService() { send_service_.run(); }

void ObjectManager::RunReceiveService() { receive_service_.run(); }

void ObjectManager::StopIOService() {
  send_service_.stop();
  for (int i = 0; i < config_.max_sends; ++i) {
    send_threads_[i].join();
  }
  receive_service_.stop();
  for (int i = 0; i < config_.max_receives; ++i) {
    receive_threads_[i].join();
  }
}

void ObjectManager::NotifyDirectoryObjectAdd(const ObjectInfoT &object_info) {
  ObjectID object_id = ObjectID::from_binary(object_info.object_id);
  local_objects_[object_id] = object_info;
  ray::Status status =
      object_directory_->ReportObjectAdded(object_id, client_id_, object_info);
}

void ObjectManager::NotifyDirectoryObjectDeleted(const ObjectID &object_id) {
  local_objects_.erase(object_id);
  ray::Status status = object_directory_->ReportObjectRemoved(object_id, client_id_);
}

ray::Status ObjectManager::SubscribeObjAdded(
    std::function<void(const ObjectInfoT &)> callback) {
  store_notification_.SubscribeObjAdded(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  store_notification_.SubscribeObjDeleted(callback);
  return ray::Status::OK();
}

ray::Status ObjectManager::Pull(const ObjectID &object_id) {
  // Check if object is already local.
  if (local_objects_.count(object_id) != 0) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return ray::Status::OK();
  }
  ray::Status status_code = object_directory_->SubscribeObjectLocations(
      object_directory_pull_callback_id_, object_id,
      [this](const std::vector<ClientID> &client_ids, const ObjectID &object_id) {
        RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
            object_directory_pull_callback_id_, object_id));
        GetLocationsSuccess(client_ids, object_id);
      });
  return status_code;
}

void ObjectManager::GetLocationsSuccess(const std::vector<ray::ClientID> &client_ids,
                                        const ray::ObjectID &object_id) {
  if (local_objects_.count(object_id) == 0) {
    // Only pull objects that aren't local.
    RAY_CHECK(!client_ids.empty());
    ClientID client_id = client_ids.front();
    ray::Status status_code = Pull(object_id, client_id);
    RAY_CHECK_OK(status_code);
  }
}

ray::Status ObjectManager::Pull(const ObjectID &object_id, const ClientID &client_id) {
  // Check if object is already local.
  if (local_objects_.count(object_id) != 0) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return ray::Status::OK();
  }
  // Check if we're pulling from self.
  if (client_id == client_id_) {
    RAY_LOG(ERROR) << client_id_ << " attempted to pull an object from itself.";
    return ray::Status::Invalid("A node cannot pull an object from itself.");
  }
  return PullEstablishConnection(object_id, client_id);
};

ray::Status ObjectManager::PullEstablishConnection(const ObjectID &object_id,
                                                   const ClientID &client_id) {
  // Acquire a message connection and send pull request.
  ray::Status status;
  std::shared_ptr<SenderConnection> conn;
  // TODO(hme): There is no cap on the number of pull request connections.
  status = connection_pool_.GetSender(ConnectionPool::ConnectionType::MESSAGE, client_id,
                                      &conn);
  // Currently, acquiring a connection should not fail.
  // No status from GetSender is returned which can be
  // handled without failing.
  RAY_CHECK_OK(status);

  if (conn == nullptr) {
    status = object_directory_->GetInformation(
        client_id,
        [this, object_id, client_id](const RemoteConnectionInfo &connection_info) {
          std::shared_ptr<SenderConnection> async_conn = CreateSenderConnection(
              ConnectionPool::ConnectionType::MESSAGE, connection_info);
          connection_pool_.RegisterSender(ConnectionPool::ConnectionType::MESSAGE,
                                          client_id, async_conn);
          Status pull_send_status = PullSendRequest(object_id, async_conn);
          RAY_CHECK_OK(pull_send_status);
        },
        [](const Status &status) {
          RAY_LOG(ERROR) << "Failed to establish connection with remote object manager.";
          RAY_CHECK_OK(status);
        });
  } else {
    status = PullSendRequest(object_id, conn);
  }
  return status;
}

ray::Status ObjectManager::PullSendRequest(const ObjectID &object_id,
                                           std::shared_ptr<SenderConnection> &conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = object_manager_protocol::CreatePullRequestMessage(
      fbb, fbb.CreateString(client_id_.binary()), fbb.CreateString(object_id.binary()));
  fbb.Finish(message);
  RAY_CHECK_OK(conn->WriteMessage(object_manager_protocol::MessageType_PullRequest,
                                  fbb.GetSize(), fbb.GetBufferPointer()));
  RAY_CHECK_OK(
      connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::MESSAGE, conn));
  return ray::Status::OK();
}

ray::Status ObjectManager::Push(const ObjectID &object_id, const ClientID &client_id,
                                int retry) {
  if (local_objects_.count(object_id) == 0) {
    if (retry < 0) {
      retry = config_.max_push_retries;
    } else if (retry == 0) {
      RAY_LOG(ERROR) << "Invalid Push request ObjectID: " << object_id
                     << " after retrying " << config_.max_push_retries << " times.";
      return ray::Status::OK();
    }
    main_service_->post([this, object_id, client_id, retry]() {
      RAY_CHECK_OK(Push(object_id, client_id, retry - 1));
    });
    return ray::Status::OK();
  }

  // TODO(hme): Cache this data in ObjectDirectory.
  // Okay for now since the GCS client caches this data.
  Status status = object_directory_->GetInformation(
      client_id,
      [this, object_id, client_id](const RemoteConnectionInfo &info) {
        const ObjectInfoT &object_info = local_objects_[object_id];
        uint64_t data_size =
            static_cast<uint64_t>(object_info.data_size + object_info.metadata_size);
        uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);
        uint64_t num_chunks = buffer_pool_.GetNumChunks(data_size);
        for (uint64_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
          send_service_.post([this, client_id, object_id, data_size, metadata_size,
                              chunk_index, info]() {
            ExecuteSendObject(client_id, object_id, data_size, metadata_size, chunk_index,
                              info);
          });
        }
      },
      [](const Status &status) {
        // Push is best effort, so do nothing here.
      });
  return status;
}

void ObjectManager::ExecuteSendObject(const ClientID &client_id,
                                      const ObjectID &object_id, uint64_t data_size,
                                      uint64_t metadata_size, uint64_t chunk_index,
                                      const RemoteConnectionInfo &connection_info) {
  RAY_LOG(DEBUG) << "ExecuteSendObject " << client_id << " " << object_id << " "
                 << chunk_index;
  ray::Status status;
  std::shared_ptr<SenderConnection> conn;
  status = connection_pool_.GetSender(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                      &conn);
  if (conn == nullptr) {
    conn =
        CreateSenderConnection(ConnectionPool::ConnectionType::TRANSFER, connection_info);
    connection_pool_.RegisterSender(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                    conn);
  }
  status = SendObjectHeaders(object_id, data_size, metadata_size, chunk_index, conn);
  RAY_CHECK_OK(status);
}

ray::Status ObjectManager::SendObjectHeaders(const ObjectID &object_id,
                                             uint64_t data_size, uint64_t metadata_size,
                                             uint64_t chunk_index,
                                             std::shared_ptr<SenderConnection> &conn) {
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.GetChunk(object_id, data_size, metadata_size, chunk_index);
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;

  // Fail on status not okay. The object is local, and there is
  // no other anticipated error here.
  RAY_CHECK_OK(chunk_status.second);

  // Create buffer.
  flatbuffers::FlatBufferBuilder fbb;
  // TODO(hme): use to_flatbuf
  auto message = object_manager_protocol::CreatePushRequestMessage(
      fbb, fbb.CreateString(object_id.binary()), chunk_index, data_size, metadata_size);
  fbb.Finish(message);
  ray::Status status =
      conn->WriteMessage(object_manager_protocol::MessageType_PushRequest, fbb.GetSize(),
                         fbb.GetBufferPointer());
  RAY_CHECK_OK(status);
  return SendObjectData(object_id, chunk_info, conn);
}

ray::Status ObjectManager::SendObjectData(const ObjectID &object_id,
                                          const ObjectBufferPool::ChunkInfo &chunk_info,
                                          std::shared_ptr<SenderConnection> &conn) {
  boost::system::error_code ec;
  std::vector<asio::const_buffer> buffer;
  buffer.push_back(asio::buffer(chunk_info.data, chunk_info.buffer_length));
  conn->WriteBuffer(buffer, ec);

  ray::Status status = ray::Status::OK();
  if (ec.value() != 0) {
    // Push failed. Deal with partial objects on the receiving end.
    // TODO(hme): Try to invoke disconnect on sender connection, then remove it.
    status = ray::Status::IOError(ec.message());
  }

  // Do this regardless of whether it failed or succeeded.
  buffer_pool_.ReleaseGetChunk(object_id, chunk_info.chunk_index);
  RAY_CHECK_OK(
      connection_pool_.ReleaseSender(ConnectionPool::ConnectionType::TRANSFER, conn));
  RAY_LOG(DEBUG) << "SendCompleted " << client_id_ << " " << object_id << " "
                 << config_.max_sends;
  return status;
}

ray::Status ObjectManager::Cancel(const ObjectID &object_id) {
  ray::Status status = object_directory_->UnsubscribeObjectLocations(
      object_directory_pull_callback_id_, object_id);
  return status;
}

ray::Status ObjectManager::Wait(const std::vector<ObjectID> &object_ids, int64_t wait_ms,
                                uint64_t num_required_objects, bool wait_local,
                                const WaitCallback &callback) {
  UniqueID wait_id = UniqueID::from_random();

  if (wait_local) {
    return ray::Status::NotImplemented("Wait for local objects is not yet implemented.");
  }

  RAY_CHECK(wait_ms >= 0);
  RAY_CHECK(num_required_objects != 0);
  RAY_CHECK(num_required_objects <= object_ids.size());
  if (object_ids.size() == 0) {
    callback(std::vector<ObjectID>(), std::vector<ObjectID>());
  }

  // Initialize fields.
  active_wait_requests_.emplace(wait_id, WaitState(*main_service_, wait_ms, callback));
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  wait_state.object_id_order = object_ids;
  wait_state.num_required_objects = num_required_objects;
  for (auto &oid : object_ids) {
    if (local_objects_.count(oid) > 0) {
      wait_state.found.insert(oid);
    } else {
      wait_state.remaining.insert(oid);
    }
  }

  // TODO: Debias returning local objects first by doing a lookup on remaining
  // objects before subscribing.
  if (wait_state.found.size() >= wait_state.num_required_objects) {
    // Requirements already satisfied.
    WaitComplete(wait_id);
  } else {
    if (wait_ms == 0) {
      for (auto &oid : wait_state.remaining) {
        // Lookup remaining objects.
        wait_state.requested_objects.insert(oid);
        RAY_CHECK_OK(object_directory_->LookupLocations(
            oid, [this, wait_id](const std::vector<ClientID> &client_ids,
                                 const ObjectID &object_id) {
              auto &wait_state = active_wait_requests_.find(wait_id)->second;
              if (!client_ids.empty()) {
                wait_state.remaining.erase(object_id);
                wait_state.found.insert(object_id);
              }
              wait_state.requested_objects.erase(object_id);
              if (wait_state.requested_objects.empty()) {
                WaitComplete(wait_id);
              }
            }));
      }
    } else {
      for (auto &oid : wait_state.remaining) {
        // Subscribe to object notifications.
        wait_state.requested_objects.insert(oid);
        RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
            wait_id, oid, [this, wait_id](const std::vector<ClientID> &client_ids,
                                          const ObjectID &object_id) {
              auto &wait_state = active_wait_requests_.find(wait_id)->second;
              if (wait_state.remaining.count(object_id) != 0) {
                wait_state.remaining.erase(object_id);
                wait_state.found.insert(object_id);
              }
              wait_state.requested_objects.erase(object_id);
              RAY_CHECK_OK(
                  object_directory_->UnsubscribeObjectLocations(wait_id, object_id));
              if (wait_state.found.size() >= wait_state.num_required_objects) {
                WaitComplete(wait_id);
              }
            }));
      }
      // Set timeout.
      // TODO (hme): If we need to just wait for all objects independent of time
      // (i.e. infinite wait time), determine what the value of wait_ms should be and
      // skip this call. WaitComplete will be invoked when all objects have locations.
      wait_state.timeout_timer->async_wait(
          [this, wait_id](const boost::system::error_code &error_code) {
            if (error_code.value() != 0) {
              return;
            }
            WaitComplete(wait_id);
          });
    }
  }
  return ray::Status::OK();
}

void ObjectManager::WaitComplete(const UniqueID &wait_id) {
  auto &wait_state = active_wait_requests_.find(wait_id)->second;
  // If we complete with outstanding requests, then wait_ms should be non-zero.
  RAY_CHECK(!(wait_state.requested_objects.size() > 0) || wait_state.wait_ms > 0);
  // Unsubscribe to any objects that weren't found in the time allotted.
  for (auto &object_id : wait_state.requested_objects) {
    RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(wait_id, object_id));
  }
  // Cancel the timer. This is okay even if the timer hasn't been started.
  // The timer handler will be given a non-zero error code. The handler
  // will do nothing on non-zero error codes.
  wait_state.timeout_timer->cancel();
  // Wait semantics require marking at most num_required_objects as found.

  //  int64_t num_move = wait_state.found.size() - wait_state.num_required_objects;
  //  if (num_move > 0) {
  //    auto iter = wait_state.found.begin();
  //    while (num_move > 0) {
  //      num_move -= 1;
  //      wait_state.remaining.insert(*iter);
  //      iter = wait_state.found.erase(iter);
  //    }
  //  }
  //  std::vector<ObjectID> found_vec(wait_state.found.begin(), wait_state.found.end());
  //  std::vector<ObjectID> remaining_vec(wait_state.remaining.begin(),
  //  wait_state.remaining.end());

  std::vector<ObjectID> found_vec;
  std::vector<ObjectID> remaining_vec;

  for (auto item : wait_state.object_id_order) {
    if (found_vec.size() < wait_state.num_required_objects &&
        wait_state.found.count(item) > 0) {
      found_vec.push_back(item);
    } else {
      remaining_vec.push_back(item);
    }
  }
  wait_state.callback(found_vec, remaining_vec);
  active_wait_requests_.erase(wait_id);
}

std::shared_ptr<SenderConnection> ObjectManager::CreateSenderConnection(
    ConnectionPool::ConnectionType type, RemoteConnectionInfo info) {
  std::shared_ptr<SenderConnection> conn =
      SenderConnection::Create(*main_service_, info.client_id, info.ip, info.port);
  // Prepare client connection info buffer
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = (type == ConnectionPool::ConnectionType::TRANSFER);
  auto message = object_manager_protocol::CreateConnectClientMessage(
      fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Send synchronously.
  RAY_CHECK_OK(conn->WriteMessage(object_manager_protocol::MessageType_ConnectClient,
                                  fbb.GetSize(), fbb.GetBufferPointer()));
  // The connection is ready; return to caller.
  return conn;
}

void ObjectManager::ProcessNewClient(TcpClientConnection &conn) {
  conn.ProcessMessages();
}

void ObjectManager::ProcessClientMessage(std::shared_ptr<TcpClientConnection> &conn,
                                         int64_t message_type, const uint8_t *message) {
  switch (message_type) {
  case object_manager_protocol::MessageType_PushRequest: {
    ReceivePushRequest(conn, message);
    break;
  }
  case object_manager_protocol::MessageType_PullRequest: {
    ReceivePullRequest(conn, message);
    break;
  }
  case object_manager_protocol::MessageType_ConnectClient: {
    ConnectClient(conn, message);
    break;
  }
  case protocol::MessageType_DisconnectClient: {
    // TODO(hme): Disconnect without depending on the node manager protocol.
    DisconnectClient(conn, message);
    break;
  }
  default: { RAY_LOG(FATAL) << "invalid request " << message_type; }
  }
}

void ObjectManager::ConnectClient(std::shared_ptr<TcpClientConnection> &conn,
                                  const uint8_t *message) {
  // TODO: trash connection on failure.
  auto info =
      flatbuffers::GetRoot<object_manager_protocol::ConnectClientMessage>(message);
  ClientID client_id = ObjectID::from_binary(info->client_id()->str());
  bool is_transfer = info->is_transfer();
  conn->SetClientID(client_id);
  if (is_transfer) {
    connection_pool_.RegisterReceiver(ConnectionPool::ConnectionType::TRANSFER, client_id,
                                      conn);
  } else {
    connection_pool_.RegisterReceiver(ConnectionPool::ConnectionType::MESSAGE, client_id,
                                      conn);
  }
  conn->ProcessMessages();
}

void ObjectManager::DisconnectClient(std::shared_ptr<TcpClientConnection> &conn,
                                     const uint8_t *message) {
  connection_pool_.RemoveReceiver(conn);
}

void ObjectManager::ReceivePullRequest(std::shared_ptr<TcpClientConnection> &conn,
                                       const uint8_t *message) {
  // Serialize and push object to requesting client.
  auto pr = flatbuffers::GetRoot<object_manager_protocol::PullRequestMessage>(message);
  ObjectID object_id = ObjectID::from_binary(pr->object_id()->str());
  ClientID client_id = ClientID::from_binary(pr->client_id()->str());
  ray::Status push_status = Push(object_id, client_id);
  conn->ProcessMessages();
}

void ObjectManager::ReceivePushRequest(std::shared_ptr<TcpClientConnection> &conn,
                                       const uint8_t *message) {
  // Serialize.
  auto object_header =
      flatbuffers::GetRoot<object_manager_protocol::PushRequestMessage>(message);
  ObjectID object_id = ObjectID::from_binary(object_header->object_id()->str());
  uint64_t chunk_index = object_header->chunk_index();
  uint64_t data_size = object_header->data_size();
  uint64_t metadata_size = object_header->metadata_size();
  receive_service_.post([this, object_id, data_size, metadata_size, chunk_index, conn]() {
    ExecuteReceiveObject(conn->GetClientID(), object_id, data_size, metadata_size,
                         chunk_index, *conn);
  });
}

void ObjectManager::ExecuteReceiveObject(const ClientID &client_id,
                                         const ObjectID &object_id, uint64_t data_size,
                                         uint64_t metadata_size, uint64_t chunk_index,
                                         TcpClientConnection &conn) {
  RAY_LOG(DEBUG) << "ExecuteReceiveObject " << client_id << " " << object_id << " "
                 << chunk_index;

  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.CreateChunk(object_id, data_size, metadata_size, chunk_index);
  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;
  if (chunk_status.second.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    std::vector<boost::asio::mutable_buffer> buffer;
    buffer.push_back(asio::buffer(chunk_info.data, chunk_info.buffer_length));
    boost::system::error_code ec;
    conn.ReadBuffer(buffer, ec);
    if (ec.value() == 0) {
      buffer_pool_.SealChunk(object_id, chunk_index);
    } else {
      buffer_pool_.AbortCreateChunk(object_id, chunk_index);
      // TODO(hme): This chunk failed, so create a pull request for this chunk.
    }
  } else {
    RAY_LOG(ERROR) << "Create Chunk Failed index = " << chunk_index << ": "
                   << chunk_status.second.message();
    // Read object into empty buffer.
    uint64_t buffer_length = buffer_pool_.GetBufferLength(chunk_index, data_size);
    std::vector<uint8_t> mutable_vec;
    mutable_vec.resize(buffer_length);
    std::vector<boost::asio::mutable_buffer> buffer;
    buffer.push_back(asio::buffer(mutable_vec, buffer_length));
    boost::system::error_code ec;
    conn.ReadBuffer(buffer, ec);
    if (ec.value() != 0) {
      RAY_LOG(ERROR) << ec.message();
    }
    // TODO(hme): If the object isn't local, create a pull request for this chunk.
  }
  conn.ProcessMessages();
  RAY_LOG(DEBUG) << "ReceiveCompleted " << client_id_ << " " << object_id << " "
                 << "/" << config_.max_receives;
}

}  // namespace ray
