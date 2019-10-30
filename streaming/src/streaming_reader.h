#ifndef RAY_STREAMING_READER_H
#define RAY_STREAMING_READER_H

#include <cstdlib>
#include <functional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "streaming.h"
#include "streaming_message_bundle.h"
#include "streaming_message_merger.h"
#include "streaming_transfer.h"

namespace ray {
namespace streaming {

struct StreamingReaderBundle {
  // it's immutable data point
  uint8_t *data = nullptr;
  uint32_t data_size;
  ObjectID from;
  uint64_t seq_id;
  StreamingMessageBundleMetaPtr meta;
};

struct StreamingReaderMsgPtrComparator {
  StreamingReaderMsgPtrComparator(){};
  bool operator()(const std::shared_ptr<StreamingReaderBundle> &a,
                  const std::shared_ptr<StreamingReaderBundle> &b);
};

class StreamingReader : public StreamingCommon {
 private:
  std::shared_ptr<ConsumerTransfer> transfer_;
  std::vector<ObjectID> input_queue_ids_;

  std::vector<ObjectID> unready_queue_ids_;

  std::unique_ptr<StreamingMessageMerger<std::shared_ptr<StreamingReaderBundle>,
                                         StreamingReaderMsgPtrComparator>>
      reader_merger_;

  std::shared_ptr<StreamingReaderBundle> last_fetched_queue_item_;

  int64_t timer_interval_;
  int64_t last_bundle_ts_;
  int64_t last_message_ts_;
  int64_t last_message_latency_;
  int64_t last_bundle_unit_;
  
  ObjectID last_read_q_id_;

  static const uint32_t kReadItemTimeout;

 protected:
  std::unordered_map<ObjectID, ConsumerChannelInfo> channel_info_map_;
  std::shared_ptr<Config> transfer_config_;

 public:
  /*!
   * init Streaming reader
   * @param store_path
   * @param input_ids
   * @param plasma_queue_seq_ids
   * @param raylet_client
   */
  void Init(const std::string &store_path, const std::vector<ObjectID> &input_ids,
            const std::vector<uint64_t> &plasma_queue_seq_ids,
            const std::vector<uint64_t> &streaming_msg_ids, int64_t timer_interval);

  void Init(const std::string &store_path, const std::vector<ObjectID> &input_ids,
            int64_t timer_interval);
  /*!
   * get latest message from input queues
   * @param timeout_ms
   * @param offset_seq_id, return offsets in plasma seq_id of each input queue, in
   * unordered_map
   * @param offset_msg_id, return offsets in streaming msg_id of each input queue, in
   * unordered_map
   * @param msg, return the latest message
   */
  StreamingStatus GetBundle(const uint32_t timeout_ms,
                            std::shared_ptr<StreamingReaderBundle> &message);

  /*!
   * get offset infomation
   * @param offset_seq_id, return offsets in plasma seq_id of each input queue, in
   * unordered_map
   * @param offset_msg_id, return offsets in streaming msg_id of each input queue, in
   * unordered_map
   */
  void GetOffsetInfo(std::unordered_map<ObjectID, ConsumerChannelInfo> *&offset_map);

    /*!
   * notify input queues to clear data before the offset.
   * used when checkpoint is done.
   * @param qid
   * @param offset
   */
  void NotifyConsumedItem(ConsumerChannelInfo &channel_info, uint64_t offset);
  
  void Stop();

  virtual ~StreamingReader();

 private:
  StreamingStatus InitChannel();

  StreamingStatus InitChannelMerger();

  StreamingStatus StashNextMessage(std::shared_ptr<StreamingReaderBundle> &message);

  StreamingStatus GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                        std::shared_ptr<StreamingReaderBundle> &message);

  StreamingStatus GetMergedMessageBundle(std::shared_ptr<StreamingReaderBundle> &message,
                                         bool &is_valid_break);
};

class StreamingReaderDirectCall : public StreamingReader {
 public:
  StreamingReaderDirectCall(CoreWorker *core_worker,
                            const std::vector<ObjectID> &queue_ids,
                            const std::vector<uint64_t> &actor_handles,
                            RayFunction async_func, RayFunction sync_func)
      : core_worker_(core_worker) {
    transfer_config_->Set(ConfigEnum::CORE_WORKER,
                          reinterpret_cast<uint64_t>(core_worker_));
    transfer_config_->Set(ConfigEnum::ASYNC_FUNCTION, async_func);
    transfer_config_->Set(ConfigEnum::SYNC_FUNCTION, sync_func);
    for (size_t i = 0; i < queue_ids.size(); ++i) {
      auto &q_id = queue_ids[i];
      channel_info_map_[q_id].actor_handle = actor_handles[i];
    }
  }

  virtual ~StreamingReaderDirectCall() { core_worker_ = nullptr; }

 private:
  CoreWorker *core_worker_;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_READER_H
