#ifndef LOCAL_SCHEDULER_CC
#define LOCAL_SCHEDULER_CC

#include "local_scheduler.h"

#include "common.h"
#include "common_protocol.h"
#include "format/nm_generated.h"
#include "Task.h"
#include "TaskSpecification.h"


using namespace std;
namespace ray {

LocalScheduler::LocalScheduler(
                       const std::string &socket_name,
                       const ResourceSet &resource_config,
                       ObjectManager &object_manager)
      : local_resources_(resource_config),
        worker_pool_(WorkerPool(0)),
        local_queues_(LsQueue()),
        sched_policy_(local_queues_),
        reconstruction_policy_(),
        task_dependency_manager_(object_manager, reconstruction_policy_) {
  //// TODO(atumanov): need to add the self-knowledge of DBClientID, using nill().
  //cluster_resource_map_[DBClientID::nil()] = local_resources_;
}


void LocalScheduler::ProcessClientMessage(shared_ptr<ClientConnection> client, int64_t message_type, const uint8_t *message_data) {
  LOG_INFO("Message of type %" PRId64, message_type);

  switch (message_type) {
  case MessageType_RegisterClientRequest: {
    auto message = flatbuffers::GetRoot<RegisterClientRequest>(message_data);
    if (message->is_worker()) {
      // Create a new worker from the registration request.
      Worker worker(message->worker_pid());
      // Add the new worker to the pool.
      client->SetWorker(std::move(worker));
    }

    // Build the reply to the worker's registration request. TODO(swang): This
    // is legacy code and should be removed once actor creation tasks are
    // implemented.
    flatbuffers::FlatBufferBuilder fbb;
    auto reply =
        CreateRegisterClientReply(fbb, fbb.CreateVector(std::vector<int>()));
    fbb.Finish(reply);
    // Reply to the worker's registration request, then listen for more
    // messages.
    client->WriteMessage(MessageType_RegisterClientReply, fbb.GetSize(), fbb.GetBufferPointer());
  } break;
  case MessageType_GetTask: {
    worker_pool_.AddWorker(client);
  } break;
  case MessageType_SubmitTask: {
    // Read the task submitted by the client.
    auto message = flatbuffers::GetRoot<SubmitTaskRequest>(message_data);
    TaskExecutionSpecification task_execution_spec(from_flatbuf(*message->execution_dependencies()));
    TaskSpecification task_spec(*message->task_spec());
    Task task(task_execution_spec, task_spec);
    // Submit the task to the local scheduler.
    submitTask(task);
  } break;
  case MessageType_DisconnectClient: {
    // Remove the dead worker from the pool and stop listening for messages.
    worker_pool_.RemoveWorker(client);
  } break;
  default:
    CHECK(0);
  }
}

void LocalScheduler::submitTask(Task& task) {
  local_queues_.QueueReadyTasks(std::vector<Task>({task}));

  // Ask policy for scheduling decision.
  // TODO(alexey): Give the policy all cluster resources instead of just the
  // local one.
  std::unordered_map<DBClientID, LsResources, UniqueIDHasher> cluster_resource_map;
  cluster_resource_map[DBClientID::nil()] = local_resources_;
  const auto &sched_policy_decision = sched_policy_.Schedule(cluster_resource_map);
  // Extract decision for this local scheduler.
  std::unordered_set<TaskID, UniqueIDHasher> task_ids;
  for (auto &task_schedule : sched_policy_decision) {
    if (task_schedule.second.is_nil()) {
      task_ids.insert(task_schedule.first);
    }
  }

  // Assign the tasks to a worker.
  std::vector<Task> tasks = local_queues_.RemoveTasks(task_ids);
  for (auto &task : tasks) {
    assignTask(task);
  }
}

void LocalScheduler::assignTask(Task& task) {
  if (worker_pool_.PoolSize() == 0) {
    // TODO(swang): Start a new worker and queue this task for future
    // assignment.
    return;
  }

  shared_ptr<ClientConnection> worker = worker_pool_.PopWorker();
  LOG_INFO("Assigning task to worker with pid %d", worker->GetWorker().Pid());

  // TODO(swang): Acquire resources for the task.
  //local_resources_.Acquire(task.GetTaskSpecification().GetRequiredResources());

  // Send the task to the worker.
  flatbuffers::FlatBufferBuilder fbb;
  const TaskSpecification &spec = task.GetTaskSpecification();
  auto message =
      CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                         fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  worker->WriteMessage(MessageType_ExecuteTask, fbb.GetSize(), fbb.GetBufferPointer());
  // Set the worker's assigned task.
  Worker worker_information = worker->GetWorker();
  worker->SetWorker(Worker(worker_information.Pid(), spec.TaskId()));
  // Mark the task as running.
  local_queues_.QueueRunningTasks(std::vector<Task>({task}));
}

} // end namespace ray

#endif  // LOCAL_SCHEDULER_CC
