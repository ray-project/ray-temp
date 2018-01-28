#include "ray/gcs/tables.h"

#include "ray/gcs/client.h"

#include "task.h"
#include "common_protocol.h"

namespace ray {

namespace gcs {

std::shared_ptr<TaskTableDataT> MakeTaskTableData(const TaskExecutionSpec &execution_spec, const DBClientID& local_scheduler_id, SchedulingState scheduling_state) {
  auto data = std::make_shared<TaskTableDataT>();
  data->scheduling_state = scheduling_state;
  data->task_info = std::string(execution_spec.Spec(), execution_spec.SpecSize());
  data->scheduler_id = local_scheduler_id.binary();

  flatbuffers::FlatBufferBuilder fbb;
  auto execution_dependencies = CreateTaskExecutionDependencies(
      fbb, to_flatbuf(fbb, execution_spec.ExecutionDependencies()));
  fbb.Finish(execution_dependencies);

  data->execution_dependencies = std::string((const char *) fbb.GetBufferPointer(), fbb.GetSize());

  return data;
}

// TODO(pcm): This is a helper method that should go away once we get rid of
// the Task* datastructure and replace it with TaskTableDataT.
Status TaskTableAdd(AsyncGcsClient* gcs_client, Task* task) {
  TaskExecutionSpec &execution_spec = *Task_task_execution_spec(task);
  TaskSpec* spec = execution_spec.Spec();
  auto data = MakeTaskTableData(execution_spec, Task_local_scheduler(task), static_cast<SchedulingState>(Task_state(task)));
  return gcs_client->task_table().Add(ray::JobID::nil(), TaskSpec_task_id(spec), data,
                                                        [](gcs::AsyncGcsClient *client,
                                                           const TaskID &id,
                                                           std::shared_ptr<TaskTableDataT> data) {});
}

}  // namespace gcs

}  // namespace ray
