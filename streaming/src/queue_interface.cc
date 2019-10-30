#include "ray/util/util.h"

#include "queue_interface.h"
#include "streaming.h"

namespace ray {

inline ray::Status ConvertStatus(const arrow::Status &status) {
  if (status.ok()) {
    return Status::OK();
  }
  return ray::Status(static_cast<ray::StatusCode>(status.code()), status.message());
}

/***
 * code below is interface implementation of streaming queue
 ***/

StreamingQueueWriter::StreamingQueueWriter(CoreWorker *core_worker,
                                           RayFunction &async_func,
                                           RayFunction &sync_func)
    : core_worker_(core_worker), async_func_(async_func), sync_func_(sync_func) {
  actor_id_ = core_worker_->GetWorkerContext().GetCurrentActorID();
  queue_manager_ = ray::streaming::QueueManager::GetInstance(actor_id_);
  queue_writer_ = std::make_shared<ray::streaming::QueueWriter>(queue_manager_);
}

Status StreamingQueueWriter::CreateQueue(const ObjectID &queue_id, int64_t data_size,
                                         uint64_t actor_handle, /*StreamingQueue only*/
                                         bool reconstruct_queue, bool clear) {
  STREAMING_LOG(INFO) << "CreateQueue qid: " << queue_id << " data_size: " << data_size
                      << " actor_handle: " << actor_handle
                      << " reconstruct_queue: " << reconstruct_queue
                      << " clear: " << clear;
  if (queue_manager_->IsUpQueueExist(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueWriter::CreateQueue duplicate!!!";
    return Status::OK();
  }
  ActorHandle *actor_handle_ptr = reinterpret_cast<ActorHandle *>(actor_handle);
  queue_manager_->AddOutTransport(
      queue_id, std::make_shared<ray::streaming::DirectCallTransport>(
                    core_worker_, actor_handle_ptr, async_func_, sync_func_));
  STREAMING_LOG(INFO) << "StreamingQueueWriter::CreateQueue"
                      << (queue_writer_ == nullptr);
  queue_writer_->CreateQueue(queue_id, actor_id_, actor_handle_ptr->GetActorID(), data_size,
                             clear);

  STREAMING_LOG(INFO) << "StreamingQueueWriter::CreateQueue done";
  return Status::OK();
}

bool StreamingQueueWriter::IsQueueFoundInLocal(const ObjectID &queue_id,
                                               const int64_t timeout_ms) {
  return queue_writer_->IsQueueExist(queue_id);
}

Status StreamingQueueWriter::SetQueueEvictionLimit(const ObjectID &queue_id,
                                                   uint64_t eviction_limit) {
  queue_writer_->SetQueueEvictionLimit(queue_id, eviction_limit);
  return Status::OK();
}

void StreamingQueueWriter::PullQueueToLocal(const ObjectID &queue_id) {
  RAY_CHECK(false) << "PullQueueToLocal not implemented";
}

void StreamingQueueWriter::WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                               int64_t timeout_ms,
                                               std::vector<ObjectID> &failed_queues) {
  queue_writer_->WaitQueues(queue_ids, timeout_ms, failed_queues);
}

void StreamingQueueWriter::GetMinConsumedSeqID(const ObjectID &queue_id,
                                               uint64_t &min_consumed_id) {
  // RAY_LOG(DEGBU) << "Only can be used for debug & monitor";

  min_consumed_id = queue_writer_->GetMinConsumedSeqID(queue_id);
}

// TODO:
void StreamingQueueWriter::GetUnconsumedBytes(const ObjectID &queue_id,
                                              uint32_t &unconsumed_bytes) {}

Status StreamingQueueWriter::PushQueueItem(const ObjectID &queue_id, uint64_t seq_id,
                                           uint8_t *data, uint32_t data_size,
                                           uint64_t timestamp) {
  return queue_writer_->PushSync(queue_id, seq_id, data, data_size, timestamp);
}

bool StreamingQueueWriter::NotifyResubscribe(const ObjectID &queue_id) {
  // queue_writer_->NotifyResubscribe(queue_id);
  RAY_LOG(INFO) << "Not implemented NotifyResubscribe";
  return true;
}

void StreamingQueueWriter::CleanupSubscription(const ObjectID &queue_id) {
  RAY_LOG(INFO) << "Not implemented CleanupSubscription";
}

void StreamingQueueWriter::GetLastQueueItem(const ObjectID &queue_id,
                                            std::shared_ptr<uint8_t> &data,
                                            uint32_t &data_size, uint64_t &sequence_id) {
  sequence_id = queue_writer_->GetLastQueueItem(queue_id);
}

uint64_t StreamingQueueWriter::GetLastMsgId(const ObjectID &queue_id,
                                            uint64_t &last_queue_seq_id) {
  uint64_t last_queue_msg_id;
  queue_writer_->GetPeerLastMsgId(queue_id, last_queue_msg_id, last_queue_seq_id);
  return last_queue_msg_id;
}

Status StreamingQueueWriter::DeleteQueue(const ObjectID &queue_id) {
  return Status::OK();
}

StreamingQueueReader::StreamingQueueReader(CoreWorker *core_worker,
                                           RayFunction &async_func,
                                           RayFunction &sync_func)
    : core_worker_(core_worker), async_func_(async_func), sync_func_(sync_func) {
  actor_id_ = core_worker_->GetWorkerContext().GetCurrentActorID();
  queue_manager_ = ray::streaming::QueueManager::GetInstance(actor_id_);
  queue_reader_ = std::make_shared<ray::streaming::QueueReader>(queue_manager_);
}

/// Create queue and pull queue (if needed), synchronously.
bool StreamingQueueReader::GetQueue(const ObjectID &queue_id, int64_t timeout_ms,
                                    uint64_t start_seq_id, uint64_t actor_handle) {
  STREAMING_LOG(INFO) << "GetQueue qid: " << queue_id << " start_seq_id: " << start_seq_id
                      << " actor_handle: " << actor_handle;
  if (queue_manager_->IsDownQueueExist(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueReader::GetQueue duplicate!!!";
    return true;
  }
  ActorHandle *actor_handle_ptr = reinterpret_cast<ActorHandle *>(actor_handle);
  queue_manager_->AddOutTransport(
      queue_id, std::make_shared<ray::streaming::DirectCallTransport>(
                    core_worker_, actor_handle_ptr, async_func_, sync_func_));
  queue_manager_->UpdateUpActor(queue_id, actor_handle_ptr->GetActorID());
  queue_manager_->UpdateDownActor(queue_id, actor_id_);
  return queue_reader_->CreateQueue(queue_id, actor_id_, actor_handle_ptr->GetActorID(),
                                    start_seq_id);
}

Status StreamingQueueReader::GetQueueItem(const ObjectID &object_id, uint8_t *&data,
                                          uint32_t &data_size, uint64_t &seq_id,
                                          uint64_t timeout_ms) {
  queue_reader_->GetQueueItem(object_id, data, data_size, seq_id, timeout_ms);
  return Status::OK();
}

void StreamingQueueReader::NotifyConsumedItem(const ObjectID &queue_id, uint64_t seq_id) {
  queue_reader_->NotifyConsumedItem(queue_id, seq_id);
}

void StreamingQueueReader::GetLastSeqID(const ObjectID &queue_id, uint64_t &last_seq_id) {
  last_seq_id = queue_reader_->GetLastSeqID(queue_id);
}

void StreamingQueueReader::WaitQueuesInCluster(const std::vector<ObjectID> &queue_ids,
                                               int64_t timeout_ms,
                                               std::vector<ObjectID> &failed_queues) {
  // We don't need to wait queue in reader side under StreamingQueue
  // queue_reader_->WaitQueues(queue_ids, timeout_ms, failed_queues);
  failed_queues.clear();
}

Status StreamingQueueReader::DeleteQueue(const ObjectID &queue_id) {
  return Status::OK();
}
}  // namespace ray
