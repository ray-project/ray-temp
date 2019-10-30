#ifndef _STREAMING_QUEUE_TRANSPORT_H_
#define _STREAMING_QUEUE_TRANSPORT_H_

#include <condition_variable>
#include <mutex>
#include <queue>

#include "../streaming_serializable.h"
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "streaming_logging.h"
#include "queue/streaming_queue_generated.h"

namespace ray {
namespace streaming {

/// base class of all command messages
class Message {
 public:
  Message(const ActorID &actor_id, const ActorID &peer_actor_id, const ObjectID &queue_id,
          std::shared_ptr<LocalMemoryBuffer> buffer = nullptr)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        buffer_(buffer) {}
  Message() {}
  virtual ~Message() {}
  virtual ActorID ActorId() { return actor_id_; }
  virtual ActorID PeerActorId() { return peer_actor_id_; }
  virtual ObjectID QueueId() { return queue_id_; }
  virtual queue::flatbuf::MessageType Type() = 0;
  std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

  virtual std::unique_ptr<LocalMemoryBuffer> ToBytes();
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) = 0;

 protected:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  std::shared_ptr<LocalMemoryBuffer> buffer_;

 public:
  static const uint32_t MagicNum;
};

class DataMessage : public Message {
 public:
  DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id, ObjectID queue_id,
              uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> buffer, bool raw)
      : Message(actor_id, peer_actor_id, queue_id, buffer), seq_id_(seq_id), raw_(raw) {}
  virtual ~DataMessage() {}

  static std::shared_ptr<DataMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);

  uint64_t SeqId() { return seq_id_; }
  bool IsRaw() { return raw_; }
  queue::flatbuf::MessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  bool raw_;

  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueDataMsg;
};

class NotificationMessage : public Message {
 public:
  NotificationMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t seq_id)
      : Message(actor_id, peer_actor_id, queue_id), seq_id_(seq_id) {}

  virtual ~NotificationMessage() {}

  static std::shared_ptr<NotificationMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);

  uint64_t SeqId() { return seq_id_; }
  queue::flatbuf::MessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueNotificationMsg;
};

class CheckMessage : public Message {
 public:
  CheckMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
               const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~CheckMessage() {}

  static std::shared_ptr<CheckMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);

  queue::flatbuf::MessageType Type() { return type_; }

 private:
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueCheckMsg;
};

class CheckRspMessage : public Message {
 public:
  CheckRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                  const ObjectID &queue_id, queue::flatbuf::StreamingQueueError err_code)
      : Message(actor_id, peer_actor_id, queue_id), err_code_(err_code) {}
  virtual ~CheckRspMessage() {}

  static std::shared_ptr<CheckRspMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  queue::flatbuf::MessageType Type() { return type_; }
  queue::flatbuf::StreamingQueueError Error() { return err_code_; }

 private:
  queue::flatbuf::StreamingQueueError err_code_;
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueCheckRspMsg;
};

class PullRequestMessage : public Message {
 public:
  PullRequestMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                     const ObjectID &queue_id, uint64_t seq_id, bool async)
      : Message(actor_id, peer_actor_id, queue_id), seq_id_(seq_id), async_(async) {}
  virtual ~PullRequestMessage() {}

  static std::shared_ptr<PullRequestMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);

  uint64_t SeqId() { return seq_id_; }
  bool IsAsync() { return async_; }
  queue::flatbuf::MessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  bool async_;
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueuePullRequestMsg;
};

class PullResponseMessage : public Message {
 public:
  PullResponseMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id,
                      queue::flatbuf::StreamingQueueError err_code)
      : Message(actor_id, peer_actor_id, queue_id), err_code_(err_code) {}
  virtual ~PullResponseMessage() {}

  static std::shared_ptr<PullResponseMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);

  queue::flatbuf::MessageType Type() { return type_; }
  queue::flatbuf::StreamingQueueError Error() { return err_code_; }

 private:
  queue::flatbuf::StreamingQueueError err_code_;
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueuePullResponseMsg;
};

class PullDataMessage : public Message {
 public:
  PullDataMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                  ObjectID queue_id, uint64_t first_seq_id, uint64_t seq_id,
                  uint64_t last_seq_id, std::shared_ptr<LocalMemoryBuffer> buffer, bool raw)
      : Message(actor_id, peer_actor_id, queue_id, buffer),
        first_seq_id_(first_seq_id),
        last_seq_id_(last_seq_id),
        seq_id_(seq_id),
        raw_(raw) {}
  virtual ~PullDataMessage() {}

  static std::shared_ptr<PullDataMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);

  uint64_t SeqId() { return seq_id_; }
  uint64_t FirstSeqId() { return first_seq_id_; }
  uint64_t LastSeqId() { return last_seq_id_; }
  bool IsRaw() { return raw_; }
  queue::flatbuf::MessageType Type() { return type_; }

 private:
  uint64_t first_seq_id_;
  uint64_t last_seq_id_;
  uint64_t seq_id_;
  bool raw_;

  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueuePullDataMsg;
};

class ResubscribeMessage : public Message {
 public:
  ResubscribeMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                     const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~ResubscribeMessage() {}

  static std::shared_ptr<ResubscribeMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  queue::flatbuf::MessageType Type() { return type_; }

 private:
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueResubscribeMsg;
};

class GetLastMsgIdMessage : public Message {
 public:
  GetLastMsgIdMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~GetLastMsgIdMessage() {}

  static std::shared_ptr<GetLastMsgIdMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  queue::flatbuf::MessageType Type() { return type_; }

 private:
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueGetLastMsgId;
};

class GetLastMsgIdRspMessage : public Message {
 public:
  GetLastMsgIdRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                         const ObjectID &queue_id, uint64_t seq_id, uint64_t msg_id,
                         queue::flatbuf::StreamingQueueError error)
      : Message(actor_id, peer_actor_id, queue_id),
        seq_id_(seq_id),
        msg_id_(msg_id),
        err_code_(error) {}
  virtual ~GetLastMsgIdRspMessage() {}

  static std::shared_ptr<GetLastMsgIdRspMessage> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  queue::flatbuf::MessageType Type() { return type_; }
  queue::flatbuf::StreamingQueueError Error() { return err_code_; }

  uint64_t SeqId() { return seq_id_; }
  uint64_t MsgId() { return msg_id_; }

 private:
  uint64_t seq_id_;
  uint64_t msg_id_;
  queue::flatbuf::StreamingQueueError err_code_;
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp;
};

class TestInitMsg : public Message {
 public:
  TestInitMsg(const queue::flatbuf::StreamingQueueTestRole role,
              const ActorID &actor_id, const ActorID &peer_actor_id,
              const std::string actor_handle_serialized, 
              const std::vector<ObjectID> &queue_ids, const std::vector<ObjectID> &rescale_queue_ids,
              std::string test_suite_name, std::string test_name,
              uint64_t param)
      : Message(actor_id, peer_actor_id, queue_ids[0]),
  actor_handle_serialized_(actor_handle_serialized), queue_ids_(queue_ids), rescale_queue_ids_(rescale_queue_ids),
  role_(role), test_suite_name_(test_suite_name), test_name_(test_name), param_(param) {}
  virtual ~TestInitMsg() {}

  static std::shared_ptr<TestInitMsg> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  queue::flatbuf::MessageType Type() { return type_; }
  std::string ActorHandleSerialized() { return actor_handle_serialized_; }
  queue::flatbuf::StreamingQueueTestRole Role() { return role_; }
  std::vector<ObjectID> QueueIds() { return queue_ids_; }
  std::vector<ObjectID> RescaleQueueIds() { return rescale_queue_ids_; }
  std::string TestSuiteName() { return test_suite_name_; }
  std::string TestName() { return test_name_;}
  uint64_t Param() { return param_; }

  std::string ToString() {
    std::ostringstream os;
    os << "actor_handle_serialized: " << actor_handle_serialized_;
    os << " queue_ids:[";
    for (auto &qid : queue_ids_) {
      os << qid << ",";
    }
    os << "], rescale_queue_ids:[";
    for (auto &qid : rescale_queue_ids_) {
      os << qid << ",";
    }
    os << "],";
    os << " role:" << queue::flatbuf::EnumNameStreamingQueueTestRole(role_);
    os << " suite_name: " << test_suite_name_;
    os << " test_name: " << test_name_;
    os << " param: " << param_;
    return os.str();
  }
 private:
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueTestInitMsg;
  std::string actor_handle_serialized_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
  queue::flatbuf::StreamingQueueTestRole role_;
  std::string test_suite_name_;
  std::string test_name_;
  uint64_t param_;
};

class TestCheckStatusRspMsg : public Message {
 public:
  TestCheckStatusRspMsg(const std::string test_name, bool status)
      : test_name_(test_name), status_(status) {}
  virtual ~TestCheckStatusRspMsg() {}

  static std::shared_ptr<TestCheckStatusRspMsg> FromBytes(uint8_t *bytes);
  virtual void ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder);
  queue::flatbuf::MessageType Type() { return type_; }
  std::string TestName() { return test_name_; }
  bool Status() { return status_; }

 private:
  const queue::flatbuf::MessageType type_ =
      queue::flatbuf::MessageType::StreamingQueueTestCheckStatusRspMsg;
  std::string test_name_;
  bool status_;
};

/// duplex
class Transport {
 public:
  virtual void WriterSend(std::unique_ptr<LocalMemoryBuffer> buffer) = 0;
  virtual void ReaderSend(std::unique_ptr<LocalMemoryBuffer> buffer) = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> WriterRecv() = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> ReaderRecv() = 0;
  virtual void Send(std::unique_ptr<LocalMemoryBuffer> buffer) = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms) = 0;
  virtual std::shared_ptr<LocalMemoryBuffer> Recv() = 0;
  virtual void Destroy() = 0;
};

template <class T>
class MemoryQueue {
 public:
  MemoryQueue(size_t size) : capacity_(size), destroyed_(false) {}

  void Push(T t) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_.size() >= capacity_) {
      writeable_cv_.wait(lock);
    }
    queue_.push(std::move(t));
    readable_cv_.notify_one();
  }

  T Pop() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_.empty() && !destroyed_) {
      readable_cv_.wait(lock);
    }

    if (destroyed_) {
      return nullptr;
    }
    T t = std::move(queue_.front());
    queue_.pop();
    writeable_cv_.notify_one();

    return t;
  }

  T Front() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_.empty()) {
      readable_cv_.wait(lock);
    }
    STREAMING_LOG(INFO) << "memory_queue_ size:" << queue_.size();
    T t = std::move(queue_.front());
    uint8_t *bytes2 = t.Data();
    uint8_t *p_cur = bytes2;
    uint32_t *magic_num = (uint32_t *)p_cur;
    STREAMING_LOG(INFO) << *magic_num << " " << Message::MagicNum;
    STREAMING_CHECK(*magic_num == Message::MagicNum);
    return t;
  }

  void Destroy() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    destroyed_ = true;
    readable_cv_.notify_all();
    writeable_cv_.notify_all();
  }

 private:
  std::mutex queue_mutex_;
  std::condition_variable readable_cv_;
  std::condition_variable writeable_cv_;
  std::queue<T> queue_;
  size_t capacity_;
  std::atomic<bool> destroyed_;
};

class SimpleMemoryTransport : public Transport {
 public:
  SimpleMemoryTransport(size_t size = 10240) : memory_queue_(size) {}
  virtual ~SimpleMemoryTransport() = default;

  void WriterSend(std::unique_ptr<LocalMemoryBuffer> buffer) { Send(std::move(buffer)); }

  void ReaderSend(std::unique_ptr<LocalMemoryBuffer> buffer) { Send(std::move(buffer)); }

  std::shared_ptr<LocalMemoryBuffer> WriterRecv() { return Recv(); }

  std::shared_ptr<LocalMemoryBuffer> ReaderRecv() { return Recv(); }

  void Send(std::unique_ptr<LocalMemoryBuffer> buffer) {
    memory_queue_.Push(std::move(buffer));
  }

  std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) {
    return nullptr;
  }

  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms) {
    return nullptr;
  }

  std::shared_ptr<LocalMemoryBuffer> Recv() {
    std::shared_ptr<LocalMemoryBuffer> buffer = memory_queue_.Pop();
    return buffer;
  }

  void Destroy() { memory_queue_.Destroy(); }

 private:
  MemoryQueue<std::unique_ptr<LocalMemoryBuffer>> memory_queue_;
};

class MemoryTransport : public Transport {
 public:
  MemoryTransport(size_t size) : write_memory_queue_(size), read_memory_queue_(size) {}
  virtual ~MemoryTransport() = default;

  virtual void WriterSend(std::unique_ptr<LocalMemoryBuffer> buffer) {
    write_memory_queue_.Push(std::move(buffer));

    //    std::unique_ptr<LocalMemoryBuffer> buffer2 = memory_queue_.Front();
    //    uint8_t* bytes2 = buffer2->Data();
    //    STREAMING_LOG(INFO) << "pointer in Send: " << static_cast<const void *>(bytes2);
    //    uint8_t* p_cur = bytes2;
    //    uint32_t* magic_num = (uint32_t*)p_cur;
    //    STREAMING_LOG(INFO) << *magic_num << " " << Message::MagicNum;
    //    STREAMING_CHECK(*magic_num == Message::MagicNum);
  }

  virtual std::shared_ptr<LocalMemoryBuffer> WriterRecv() {
    // LocalMemoryBuffer buffer(nullptr, 0);
    std::shared_ptr<LocalMemoryBuffer> buffer = read_memory_queue_.Pop();
    //    uint8_t* bytes2 = buffer.Data();
    //    uint8_t* p_cur = bytes2;
    //    uint32_t* magic_num = (uint32_t*)p_cur;
    //    STREAMING_LOG(INFO) << *magic_num << " " << Message::MagicNum;
    //    STREAMING_CHECK(*magic_num == Message::MagicNum);
    return buffer;
  }

  virtual void ReaderSend(std::unique_ptr<LocalMemoryBuffer> buffer) {
    read_memory_queue_.Push(std::move(buffer));
  }

  virtual std::shared_ptr<LocalMemoryBuffer> ReaderRecv() {
    std::shared_ptr<LocalMemoryBuffer> buffer = write_memory_queue_.Pop();
    return buffer;
  }

  void Send(std::unique_ptr<LocalMemoryBuffer> buffer) {}

  std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) {
    return nullptr;
  }

  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms) {
    return nullptr;
  }

  std::shared_ptr<LocalMemoryBuffer> Recv() { return nullptr; }

  void Destroy() {
    write_memory_queue_.Destroy();
    read_memory_queue_.Destroy();
  }

 private:
  MemoryQueue<std::unique_ptr<LocalMemoryBuffer>> write_memory_queue_;
  MemoryQueue<std::unique_ptr<LocalMemoryBuffer>> read_memory_queue_;
};

class DirectCallTransport : public Transport {
 public:
  DirectCallTransport(CoreWorker* worker,
                      ActorHandle* actor_handle, RayFunction &async_func,
                      RayFunction &sync_func)
      : core_worker_(worker),
        peer_actor_handle_(actor_handle),
        async_func_(async_func),
        sync_func_(sync_func) {
    STREAMING_LOG(DEBUG) << "DirectCallTransport constructor:";
    for (auto item : async_func_.GetFunctionDescriptor()) {
      STREAMING_LOG(DEBUG) << "async_func: " << item;
    }
    for (auto item : sync_func_.GetFunctionDescriptor()) {
      STREAMING_LOG(DEBUG) << "sync_func: " << item;
    }
  }
  virtual ~DirectCallTransport() = default;

  virtual void WriterSend(std::unique_ptr<LocalMemoryBuffer> buffer) {
    Send(std::move(buffer));
  }

  virtual void ReaderSend(std::unique_ptr<LocalMemoryBuffer> buffer) {
    Send(std::move(buffer));
  }

  virtual std::shared_ptr<LocalMemoryBuffer> WriterRecv() { return Recv(); }

  virtual std::shared_ptr<LocalMemoryBuffer> ReaderRecv() { return Recv(); }

  virtual void Destroy() {}

  virtual void Send(std::unique_ptr<LocalMemoryBuffer> buffer);
  virtual std::shared_ptr<LocalMemoryBuffer> SendForResult(
      std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms);
  std::shared_ptr<LocalMemoryBuffer> SendForResultWithRetry(
      std::unique_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms);
  virtual std::shared_ptr<LocalMemoryBuffer> Recv();

 private:
  CoreWorker* core_worker_;
  ActorHandle* peer_actor_handle_;
  RayFunction async_func_;
  RayFunction sync_func_;
};

enum TransportType { MEMORY, DIRECTCALL };

class TransportFactory {
 public:
  static std::shared_ptr<Transport> GetTransport(TransportType type) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (instance_ != nullptr) {
      STREAMING_LOG(INFO) << "Transport instance exist.";
      return instance_;
    }

    switch (type) {
    case MEMORY:
      STREAMING_LOG(INFO) << "Create MemoryTransport";
      instance_ = std::make_shared<MemoryTransport>(10240);
      break;
    case DIRECTCALL:
      // instance_ = std::make_shared<DirectCallTransport>();
      break;
    default:
      STREAMING_CHECK(false) << "invalid transport type";
    }

    return instance_;
  }

 private:
  static std::shared_ptr<Transport> instance_;
  static std::mutex mutex_;
};

}  // namespace streaming
}  // namespace ray
#endif
