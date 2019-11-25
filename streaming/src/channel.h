#ifndef RAY_CHANNEL_H
#define RAY_CHANNEL_H

#include "config/streaming_config.h"
#include "queue/queue_interface.h"
#include "ring_buffer.h"
#include "status.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

struct StreamingQueueInfo {
  uint64_t first_seq_id = 0;
  uint64_t last_seq_id = 0;
  uint64_t target_seq_id = 0;
  uint64_t consumed_seq_id = 0;
};

struct ProducerChannelInfo {
  ObjectID channel_id;
  StreamingRingBufferPtr writer_ring_buffer;
  uint64_t current_message_id;
  uint64_t current_seq_id;
  uint64_t message_last_commit_id;
  StreamingQueueInfo queue_info;
  uint32_t queue_size;
  int64_t message_pass_by_ts;

  // for Direct Call
  uint64_t actor_handle;
  ActorID actor_id;
};

struct ConsumerChannelInfo {
  ObjectID channel_id;
  uint64_t current_message_id;
  uint64_t current_seq_id;
  uint64_t barrier_id;
  uint64_t partial_barrier_id;

  StreamingQueueInfo queue_info;

  uint64_t last_queue_item_delay;
  uint64_t last_queue_item_latency;
  uint64_t last_queue_target_diff;
  uint64_t get_queue_item_times;

  // for Direct Call
  uint64_t actor_handle;
  ActorID actor_id;
};

class ProducerChannel {
 public:
  explicit ProducerChannel(std::shared_ptr<Config> &transfer_config,
                           ProducerChannelInfo &p_channel_info);
  virtual ~ProducerChannel() = default;
  virtual StreamingStatus CreateTransferChannel() = 0;
  virtual StreamingStatus DestroyTransferChannel() = 0;
  virtual StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size) = 0;
  virtual StreamingStatus NotifyChannelConsumed(uint64_t channel_offset) = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
  ProducerChannelInfo &channel_info;
};

class ConsumerChannel {
 public:
  explicit ConsumerChannel(std::shared_ptr<Config> &transfer_config,
                           ConsumerChannelInfo &c_channel_info);
  virtual ~ConsumerChannel() = default;
  virtual StreamingStatus CreateTransferChannel() = 0;
  virtual StreamingStatus DestroyTransferChannel() = 0;
  virtual StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                                 uint32_t &data_size,
                                                 uint32_t timeout) = 0;
  virtual StreamingStatus NotifyChannelConsumed(uint64_t offset_id) = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
  ConsumerChannelInfo &channel_info;
};

class StreamingQueueProducer : public ProducerChannel {
 public:
  explicit StreamingQueueProducer(std::shared_ptr<Config> &transfer_config,
                                  ProducerChannelInfo &p_channel_info);
  ~StreamingQueueProducer() override;
  StreamingStatus CreateTransferChannel() override;
  StreamingStatus DestroyTransferChannel() override;
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;

 private:
  StreamingStatus CreateQueue();

 private:
  std::shared_ptr<StreamingQueueWriter> queue_writer_;
};

class StreamingQueueConsumer : public ConsumerChannel {
 public:
  explicit StreamingQueueConsumer(std::shared_ptr<Config> &transfer_config,
                                  ConsumerChannelInfo &c_channel_info);
  ~StreamingQueueConsumer() override;
  StreamingStatus CreateTransferChannel() override;
  StreamingStatus DestroyTransferChannel() override;
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  StreamingStatus ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                         uint32_t &data_size, uint32_t timeout) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;

 private:
  std::shared_ptr<StreamingQueueReader> queue_reader_;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_CHANNEL_H
