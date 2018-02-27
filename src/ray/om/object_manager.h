#ifndef RAY_OBJECTMANAGER_H
#define RAY_OBJECTMANAGER_H

#include <memory>
#include <cstdint>
#include <deque>
#include <map>
#include <thread>
#include <algorithm>

// #include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/plasma.h"
#include "plasma/events.h"
#include "plasma/client.h"

#include "ray/id.h"
#include "ray/status.h"

#include "ray/raylet/client_connection.h"

#include "ray/om/object_directory.h"
#include "ray/om/object_store_client.h"
#include "ray/om/format/om_generated.h"
// #include "plasma/plasma_common.h"
// #include "common_protocol.h"

namespace ray {

struct OMConfig {
  int num_retries = 5;
  std::string store_socket_name;
};

struct SendRequest {
  ObjectID object_id;
  ClientID client_id;
  int64_t object_size;
  uint8_t *data;
};

// TODO(hme): Move this to an appropriate location (client_connection?).
class SenderConnection : public boost::enable_shared_from_this<SenderConnection> {

 public:
  typedef boost::shared_ptr<SenderConnection> pointer;
  typedef std::unordered_map<ray::ObjectID, SendRequest, UniqueIDHasher> SendRequestsType;
  typedef std::deque<ray::ObjectID> SendQueueType;

  static pointer Create(boost::asio::io_service& io_service,
                        const ODRemoteConnectionInfo &info){
    return pointer(new SenderConnection(io_service, info));
  };

  explicit SenderConnection(boost::asio::io_service& io_service,
                            const ODRemoteConnectionInfo &info) :
      socket_(io_service),
      send_queue_()
  {
    boost::asio::ip::address addr = boost::asio::ip::address::from_string(info.ip);
    boost::asio::ip::tcp::endpoint endpoint(addr, info.port);
    socket_.connect(endpoint);
  };

  boost::asio::ip::tcp::socket &GetSocket(){
    return socket_;
  };

  bool IsObjectIdQueueEmpty(){
    return send_queue_.empty();
  }

  bool ObjectIdQueued(const ObjectID &object_id){
    return std::find(send_queue_.begin(),
                     send_queue_.end(),
                     object_id)!=send_queue_.end();
  }

  void QueueObjectId(const ObjectID &object_id){
    send_queue_.push_back(ObjectID(object_id));
  }

  ObjectID DeQueueObjectId(){
    ObjectID object_id = send_queue_.front();
    send_queue_.pop_front();
    return object_id;
  }

  void AddSendRequest(const ObjectID &object_id, SendRequest &send_request){
    send_requests_.emplace(object_id, send_request);
  }

  void RemoveSendRequest(const ObjectID &object_id){
    send_requests_.erase(object_id);
  }

  SendRequest &GetSendRequest(const ObjectID &object_id){
    return send_requests_[object_id];
  };

 private:
  boost::asio::ip::tcp::socket socket_;
  SendQueueType send_queue_;
  SendRequestsType send_requests_;

};

// TODO(hme): Implement connection cleanup.
class ObjectManager {

 public:

  // Callback signatures for Push and Pull. Please keep until we're certain
  // they will not be necessary (hme).
  using TransferCallback = std::function<void(ray::Status,
                                         const ray::ObjectID&,
                                         const ray::ClientID&)>;

  using WaitCallback = std::function<void(const ray::Status,
                                          uint64_t,
                                          const std::vector<ray::ObjectID>&)>;

  // Instantiates Ray implementation of ObjectDirectory.
  explicit ObjectManager(boost::asio::io_service &io_service,
                         OMConfig config);

  // Takes user-defined ObjectDirectoryInterface implementation.
  // When this constructor is used, the ObjectManager assumes ownership of
  // the given ObjectDirectory instance.
  explicit ObjectManager(boost::asio::io_service &io_service,
                         OMConfig config,
                         std::shared_ptr<ObjectDirectoryInterface> od);

  void SetClientID(const ClientID &client_id);
  ClientID GetClientID();

  // Subscribe to notifications of objects added to local store.
  // Upon subscribing, the callback will be invoked for all objects that
  // already exist in the local store.
  ray::Status SubscribeObjAdded(std::function<void(const ray::ObjectID&)> callback);

  // Subscribe to notifications of objects deleted from local store.
  ray::Status SubscribeObjDeleted(std::function<void(const ray::ObjectID&)> callback);

  // Push an object to DBClientID.
  ray::Status Push(const ObjectID &object_id,
                   const ClientID &dbclient_id);

  // Pull an object from DBClientID. Returns UniqueID associated with
  // an invocation of this method.
  ray::Status Pull(const ObjectID &object_id);

  // Discover DBClientID via ObjectDirectory, then pull object
  // from DBClientID associated with ObjectID.
  ray::Status Pull(const ObjectID &object_id,
                   const ClientID &client_id);

  ray::Status AddSock(TCPClientConnection::pointer conn);

  // Cancels all requests (Push/Pull) associated with the given ObjectID.
  ray::Status Cancel(const ObjectID &object_id);

  // Wait for timeout_ms before invoking the provided callback.
  // If num_ready_objects is satisfied before the timeout, then
  // invoke the callback.
  ray::Status Wait(const std::vector<ObjectID> &object_ids,
                   uint64_t timeout_ms,
                   int num_ready_objects,
                   const WaitCallback &callback);

  ray::Status Terminate();

 private:
  ClientID client_id_;
  OMConfig config;
  std::shared_ptr<ObjectDirectoryInterface> od;
  std::unique_ptr<ObjectStoreClient> store_client_;

  boost::asio::io_service io_service_;

  boost::asio::io_service::work work_;
  std::thread io_thread_;
  // boost::thread_group thread_group_;

  // TODO (hme): This needs to account for receives as well.
  int num_transfers_ = 0;
  // TODO (hme): Allow for concurrent sends.
  int max_transfers_ = 1;

  // Note that, currently, receives take place on the main thread,
  // and sends take place on a dedicated thread.
  std::unordered_map<ray::ClientID,
                     SenderConnection::pointer,
                     ray::UniqueIDHasher> message_send_connections_;
  std::unordered_map<ray::ClientID,
                     SenderConnection::pointer,
                     ray::UniqueIDHasher> transfer_send_connections_;

  std::unordered_map<ray::ClientID,
                     TCPClientConnection::pointer,
                     ray::UniqueIDHasher> message_receive_connections_;
  std::unordered_map<ray::ClientID,
                     TCPClientConnection::pointer,
                     ray::UniqueIDHasher> transfer_receive_connections_;

  void StartIOService();
  void IOServiceLoop();
  void StopIOService();

  ray::Status ExecutePull(const ObjectID &object_id,
                          SenderConnection::pointer client);

  ray::Status QueuePush(const ObjectID &object_id,
                        SenderConnection::pointer client);
  ray::Status ExecutePushQueue(SenderConnection::pointer client);
  ray::Status ExecutePushCompleted(const ObjectID &object_id,
                                   SenderConnection::pointer client);
  ray::Status ExecutePush(const ObjectID &object_id,
                          SenderConnection::pointer client);

  /// callback that gets called internally to OD on get location success.
  void GetLocationsSuccess(const std::vector<ODRemoteConnectionInfo>& v,
                           const ObjectID &object_id);

  /// callback that gets called internally to OD on get location failure.
   void GetLocationsFailed(ray::Status status,
                           const ObjectID &object_id);

  ray::Status GetMsgConnection(const ClientID &client_id,
                               std::function<void(SenderConnection::pointer)> callback);

  ray::Status CreateMsgConnection(const ODRemoteConnectionInfo &info,
                                  std::function<void(SenderConnection::pointer)> callback);

  ray::Status GetTransferConnection(const ClientID &client_id,
                                    std::function<void(SenderConnection::pointer)> callback);

  ray::Status CreateTransferConnection(const ODRemoteConnectionInfo &info,
                                       std::function<void(SenderConnection::pointer)> callback);

  ray::Status WaitPushReceive(TCPClientConnection::pointer conn);

  void HandlePushSend(SenderConnection::pointer conn,
                      const ObjectID &object_id,
                      const boost::system::error_code &header_ec);

  void HandlePushReceive(TCPClientConnection::pointer conn,
                         const boost::system::error_code& length_ec);

};

} // end namespace

#endif // RAY_OBJECTMANAGER_H
