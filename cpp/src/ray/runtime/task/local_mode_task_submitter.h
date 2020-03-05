#pragma once

#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <mutex>
#include <queue>

#include "invocation_spec.h"
#include "task_executer.h"
#include "task_spec.h"
#include "task_submitter.h"
#include <ray/api/task_type.h>

namespace ray {

class LocalModeTaskSubmitter : public TaskSubmitter {
 public:
  LocalModeTaskSubmitter();

  std::unique_ptr<UniqueId> submitTask(const InvocationSpec &invocation);

  std::unique_ptr<UniqueId> createActor(remote_function_ptr_holder &fptr,
                                        std::shared_ptr<msgpack::sbuffer> args);

  std::unique_ptr<UniqueId> submitActorTask(const InvocationSpec &invocation);

 private:
  std::queue<TaskSpec> _tasks;

  std::unordered_map<UniqueId, std::unique_ptr<ActorContext>> _actorContexts;

  std::mutex _actorContextsMutex;

  std::unique_ptr<boost::asio::thread_pool> _pool;

  std::unique_ptr<UniqueId> submit(const InvocationSpec &invocation, TaskType type);

  std::list<std::unique_ptr<UniqueId>> buildReturnIds(const UniqueId &taskId,
                                                      int returnCount);
};
}  // namespace ray