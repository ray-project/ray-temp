#include "client_connection.h"

#include <stdio.h>
#include <boost/bind.hpp>

#include "ray/ray_config.h"
#include "ray/util/util.h"

namespace ray {

template <class T>
Status ReadBuffer(boost::asio::basic_stream_socket<T> &socket,
                  const boost::asio::mutable_buffer &buffer) {
  boost::system::error_code ec;
  // Loop until all bytes are read while handling interrupts.
  uint64_t bytes_remaining = boost::asio::buffer_size(buffer);
  uint64_t position = 0;
  while (bytes_remaining != 0) {
    size_t bytes_read =
        socket.read_some(boost::asio::buffer(buffer + position, bytes_remaining), ec);
    position += bytes_read;
    bytes_remaining -= bytes_read;
    if (ec.value() == EINTR) {
      continue;
    } else if (ec.value() != boost::system::errc::errc_t::success) {
      return boost_to_ray_status(ec);
    }
  }
  return Status::OK();
}

template <class T>
Status ReadBuffer(boost::asio::basic_stream_socket<T> &socket,
                  const std::vector<boost::asio::mutable_buffer> &buffer) {
  // Loop until all bytes are read while handling interrupts.
  for (const auto &b : buffer) {
    auto status = ReadBuffer<T>(socket, b);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

template <class T>
Status WriteBuffer(boost::asio::basic_stream_socket<T> &socket,
                   const std::vector<boost::asio::const_buffer> &buffer) {
  boost::system::error_code error;
  // Loop until all bytes are written while handling interrupts.
  // When profiling with pprof, unhandled interrupts were being sent by the profiler to
  // the raylet process, which was causing synchronous reads and writes to fail.
  for (const auto &b : buffer) {
    uint64_t bytes_remaining = boost::asio::buffer_size(b);
    uint64_t position = 0;
    while (bytes_remaining != 0) {
      size_t bytes_written =
          socket.write_some(boost::asio::buffer(b + position, bytes_remaining), error);
      position += bytes_written;
      bytes_remaining -= bytes_written;
      if (error.value() == EINTR) {
        continue;
      } else if (error.value() != boost::system::errc::errc_t::success) {
        return boost_to_ray_status(error);
      }
    }
  }
  return ray::Status::OK();
}

ray::Status TcpConnect(boost::asio::ip::tcp::socket &socket,
                       const std::string &ip_address_string, int port) {
  boost::asio::ip::address ip_address =
      boost::asio::ip::address::from_string(ip_address_string);
  boost::asio::ip::tcp::endpoint endpoint(ip_address, port);
  boost::system::error_code error;
  socket.connect(endpoint, error);
  const auto status = boost_to_ray_status(error);
  if (!status.ok()) {
    // Close the socket if the connect failed.
    boost::system::error_code close_error;
    socket.close(close_error);
  }
  return status;
}

ray::Status UnixSocketConnect(const std::string &socket_name,
                              boost::asio::io_service &main_service,
                              std::unique_ptr<stream_protocol::socket> &socket,
                              int num_retries, int64_t timeout_ms) {
  if (num_retries < 0) {
    num_retries = RayConfig::instance().num_connect_attempts();
  }
  if (timeout_ms < 0) {
    timeout_ms = RayConfig::instance().connect_timeout_milliseconds();
  }
  RAY_CHECK(!socket_name.empty());
  stream_protocol::endpoint endpoint(socket_name);
  boost::system::error_code ec;
  for (int i = 0; i < num_retries; i++) {
    socket.reset(new stream_protocol::socket(main_service));
    socket->connect(endpoint, ec);
    if (!ec) {
      return ray::Status::OK();
    }
    usleep(timeout_ms * 1000);
    if (i > 0) {
      RAY_LOG(ERROR) << "Retrying to connect to socket for pathname " << socket_name
                     << " (num_attempts = " << i << ", num_retries = " << num_retries
                     << ")";
    }
  }
  return boost_to_ray_status(ec);
}

template <class T>
std::shared_ptr<ServerConnection<T>> ServerConnection<T>::Create(
    boost::asio::basic_stream_socket<T> &&socket) {
  std::shared_ptr<ServerConnection<T>> self(new ServerConnection(std::move(socket)));
  return self;
}

template <class T>
ServerConnection<T>::ServerConnection(boost::asio::basic_stream_socket<T> &&socket)
    : socket_(std::move(socket)),
      async_write_max_messages_(1),
      async_write_queue_(),
      async_write_in_flight_(false) {}

template <class T>
ServerConnection<T>::~ServerConnection() {
  // If there are any pending messages, invoke their callbacks with an IOError status.
  for (const auto &write_buffer : async_write_queue_) {
    write_buffer->handler(Status::IOError("Connection closed."));
  }
}

template <class T>
Status ServerConnection<T>::WriteBuffer(
    const std::vector<boost::asio::const_buffer> &buffer) {
  return ray::WriteBuffer<T>(socket_, buffer);
}

template <class T>
Status ServerConnection<T>::ReadBuffer(
    const std::vector<boost::asio::mutable_buffer> &buffer) {
  return ray::ReadBuffer<T>(socket_, buffer);
}

template <class T>
Status ServerConnection<T>::ReadBuffer(boost::asio::mutable_buffer &buffer) {
  return ray::ReadBuffer<T>(socket_, buffer);
}

template <class T>
ray::Status ServerConnection<T>::ReadMessage(int64_t type,
                                             std::unique_ptr<uint8_t[]> &message) {
  int64_t read_version, read_type, read_length;
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_version, sizeof(read_version)));
  header.push_back(boost::asio::buffer(&read_type, sizeof(read_type)));
  header.push_back(boost::asio::buffer(&read_length, sizeof(read_length)));

  auto status = ReadBuffer(header);
  if (!status.ok()) {
    return status;
  }
  // If there was no error, make sure the protocol version matches.
  RAY_CHECK(read_version == RayConfig::instance().ray_protocol_version());
  if (read_type == static_cast<int64_t>(MessageType::DisconnectClient)) {
    return ray::Status::IOError("Disconnection message received.");
  }

  if (type != read_type) {
    return ray::Status::TypeError(
        std::string("Connection corrupted. ") + "Expected message type: " +
        std::to_string(type) + "; got message type: " +
        std::to_string(read_type) + ". Check logs or dmesg for previous errors.");
  }
  // Create read buffer.
  message.reset(new uint8_t[read_length]);
  auto buffer = boost::asio::mutable_buffer(message.get(), read_length);
  // Wait for the message to be read.
  return ReadBuffer(buffer);
}

template <class T>
ray::Status ServerConnection<T>::WriteMessage(int64_t type, int64_t length,
                                              const uint8_t *message) {
  sync_writes_ += 1;
  bytes_written_ += length;

  std::vector<boost::asio::const_buffer> message_buffers;
  auto write_version = RayConfig::instance().ray_protocol_version();
  message_buffers.push_back(boost::asio::buffer(&write_version, sizeof(write_version)));
  message_buffers.push_back(boost::asio::buffer(&type, sizeof(type)));
  message_buffers.push_back(boost::asio::buffer(&length, sizeof(length)));
  message_buffers.push_back(boost::asio::buffer(message, length));
  return WriteBuffer(message_buffers);
}

template <class T>
void ServerConnection<T>::WriteMessageAsync(
    int64_t type, int64_t length, const uint8_t *message,
    const std::function<void(const ray::Status &)> &handler) {
  async_writes_ += 1;
  bytes_written_ += length;

  auto write_buffer = std::unique_ptr<AsyncWriteBuffer>(new AsyncWriteBuffer());
  write_buffer->write_version = RayConfig::instance().ray_protocol_version();
  write_buffer->write_type = type;
  write_buffer->write_length = length;
  write_buffer->write_message.resize(length);
  write_buffer->write_message.assign(message, message + length);
  write_buffer->handler = handler;

  auto size = async_write_queue_.size();
  auto size_is_power_of_two = (size & (size - 1)) == 0;
  if (size > 1000 && size_is_power_of_two) {
    RAY_LOG(WARNING) << "ServerConnection has " << size << " buffered async writes";
  }

  async_write_queue_.push_back(std::move(write_buffer));

  if (!async_write_in_flight_) {
    DoAsyncWrites();
  }
}

template <class T>
void ServerConnection<T>::DoAsyncWrites() {
  // Make sure we were not writing to the socket.
  RAY_CHECK(!async_write_in_flight_);
  async_write_in_flight_ = true;

  // Do an async write of everything currently in the queue to the socket.
  std::vector<boost::asio::const_buffer> message_buffers;
  int num_messages = 0;
  for (const auto &write_buffer : async_write_queue_) {
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_version,
                                                  sizeof(write_buffer->write_version)));
    message_buffers.push_back(
        boost::asio::buffer(&write_buffer->write_type, sizeof(write_buffer->write_type)));
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_length,
                                                  sizeof(write_buffer->write_length)));
    message_buffers.push_back(boost::asio::buffer(write_buffer->write_message));
    num_messages++;
    if (num_messages >= async_write_max_messages_) {
      break;
    }
  }
  auto this_ptr = this->shared_from_this();
  boost::asio::async_write(
      ServerConnection<T>::socket_, message_buffers,
      [this, this_ptr, num_messages](const boost::system::error_code &error,
                                     size_t bytes_transferred) {
        ray::Status status = ray::Status::OK();
        if (error.value() != boost::system::errc::errc_t::success) {
          status = boost_to_ray_status(error);
        }
        // Call the handlers for the written messages.
        for (int i = 0; i < num_messages; i++) {
          auto write_buffer = std::move(async_write_queue_.front());
          write_buffer->handler(status);
          async_write_queue_.pop_front();
        }
        // We finished writing, so mark that we're no longer doing an async write.
        async_write_in_flight_ = false;
        // If there is more to write, try to write the rest.
        if (!async_write_queue_.empty()) {
          DoAsyncWrites();
        }
      });
}

template <class T>
const ClientID &ServerConnection<T>::GetClientId() const {
  return client_id_;
}

template <class T>
void ServerConnection<T>::SetClientID(const ClientID &client_id) {
  client_id_ = client_id;
}

template <class T>
ray::Status ServerConnection<T>::Close() {
  boost::system::error_code ec;
  socket_.close(ec);
  return boost_to_ray_status(ec);
}

template <class T>
std::shared_ptr<ClientConnection<T>> ClientConnection<T>::Create(
    ClientHandler<T> &client_handler, MessageHandler<T> &message_handler,
    boost::asio::basic_stream_socket<T> &&socket, const std::string &debug_label,
    int64_t error_message_type) {
  std::shared_ptr<ClientConnection<T>> self(new ClientConnection(
      message_handler, std::move(socket), debug_label, error_message_type));
  // Let our manager process our new connection.
  client_handler(*self);
  return self;
}

template <class T>
ClientConnection<T>::ClientConnection(MessageHandler<T> &message_handler,
                                      boost::asio::basic_stream_socket<T> &&socket,
                                      const std::string &debug_label,
                                      int64_t error_message_type)
    : ServerConnection<T>(std::move(socket)),
      message_handler_(message_handler),
      debug_label_(debug_label),
      error_message_type_(error_message_type) {}

template <class T>
void ClientConnection<T>::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_version_, sizeof(read_version_)));
  header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
  header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
  boost::asio::async_read(
      ServerConnection<T>::socket_, header,
      boost::bind(&ClientConnection<T>::ProcessMessageHeader,
                  shared_ClientConnection_from_this(), boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::ProcessMessageHeader(const boost::system::error_code &error) {
  if (error) {
    // If there was an error, disconnect the client.
    read_type_ = error_message_type_;
    read_length_ = 0;
    ProcessMessage(error);
    return;
  }

  // If there was no error, make sure the protocol version matches.
  RAY_CHECK(read_version_ == RayConfig::instance().ray_protocol_version());
  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  ServerConnection<T>::bytes_read_ += read_length_;
  // Wait for the message to be read.
  boost::asio::async_read(
      ServerConnection<T>::socket_, boost::asio::buffer(read_message_),
      boost::bind(&ClientConnection<T>::ProcessMessage,
                  shared_ClientConnection_from_this(), boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::ProcessMessage(const boost::system::error_code &error) {
  if (error) {
    read_type_ = error_message_type_;
  }

  int64_t start_ms = current_time_ms();
  message_handler_(shared_ClientConnection_from_this(), read_type_, read_message_.data());
  int64_t interval = current_time_ms() - start_ms;
  if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
    RAY_LOG(WARNING) << "[" << debug_label_ << "]ProcessMessage with type " << read_type_
                     << " took " << interval << " ms.";
  }
}

template <class T>
std::string ServerConnection<T>::DebugString() const {
  std::stringstream result;
  result << "\n- bytes read: " << bytes_read_;
  result << "\n- bytes written: " << bytes_written_;
  result << "\n- num async writes: " << async_writes_;
  result << "\n- num sync writes: " << sync_writes_;
  result << "\n- writing: " << async_write_in_flight_;
  int64_t num_bytes = 0;
  for (auto &buffer : async_write_queue_) {
    num_bytes += buffer->write_length;
  }
  result << "\n- pending async bytes: " << num_bytes;
  return result.str();
}

template class ServerConnection<boost::asio::local::stream_protocol>;
template class ServerConnection<boost::asio::ip::tcp>;
template class ClientConnection<boost::asio::local::stream_protocol>;
template class ClientConnection<boost::asio::ip::tcp>;

}  // namespace ray
