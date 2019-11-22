#define BOOST_BIND_NO_PLACEHOLDERS
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "src/ray/util/test_util.h"

using namespace std::placeholders;

namespace ray {

const std::string c_get_pid_string("GET_MOCK_WORKER_PID");

/// A mock C++ worker used by core_worker_test.cc to verify the task submission/execution
/// interfaces in both single node and cross-nodes scenarios. As the raylet client can
/// only
/// be called by a real worker process, core_worker_test.cc has to use this program binary
/// to start the actual worker process, in the test, the task submission interfaces are
/// called
/// in core_worker_test, and task execution interfaces are called in this file, see that
/// test
/// for more details on how this class is used.
class MockWorker {
 public:
  MockWorker(const std::string &store_socket, const std::string &raylet_socket,
             int node_manager_port, const gcs::GcsClientOptions &gcs_options)
      : worker_(WorkerType::WORKER, Language::PYTHON, store_socket, raylet_socket,
                JobID::FromInt(1), gcs_options, /*log_dir=*/"",
                /*node_id_address=*/"127.0.0.1", node_manager_port,
                std::bind(&MockWorker::ExecuteTask, this, _1, _2, _3, _4, _5, _6, _7)) {}

  void StartExecutingTasks() { worker_.StartExecutingTasks(); }

 private:
  Status ExecuteTask(TaskType task_type, const RayFunction &ray_function,
                     const std::unordered_map<std::string, double> &required_resources,
                     const std::vector<std::shared_ptr<RayObject>> &args,
                     const std::vector<ObjectID> &arg_reference_ids,
                     const std::vector<ObjectID> &return_ids,
                     std::vector<std::shared_ptr<RayObject>> *results) {
    // Note that this doesn't include dummy object id.
    RAY_CHECK(return_ids.size() >= 0);

    if (args.size() == 1 && args[0]->GetData()->Size() == c_get_pid_string.size() &&
        memcmp(args[0]->GetData()->Data(), c_get_pid_string.data(),
               c_get_pid_string.size()) == 0) {
      // Save the pid of current process to the return object.
      std::string pid_string = std::to_string(static_cast<int>(getpid()));
      auto data =
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(pid_string.data()));
      auto memory_buffer =
          std::make_shared<LocalMemoryBuffer>(data, pid_string.size(), true);
      results->push_back(std::make_shared<RayObject>(memory_buffer, nullptr));
      return Status::OK();
    }

    // Merge all the content from input args.
    std::vector<uint8_t> buffer;
    for (const auto &arg : args) {
      auto &data = arg->GetData();
      buffer.insert(buffer.end(), data->Data(), data->Data() + data->Size());
    }
    if (buffer.size() >= 8) {
      auto int_arr = reinterpret_cast<int64_t *>(buffer.data());
      if (int_arr[0] == SHOULD_CHECK_MESSAGE_ORDER) {
        auto seq_no = int_arr[1];
        if (seq_no > 0) {
          RAY_CHECK(seq_no == prev_seq_no_ + 1) << seq_no << " vs " << prev_seq_no_;
        }
        prev_seq_no_ = seq_no;
      }
    }
    auto memory_buffer =
        std::make_shared<LocalMemoryBuffer>(buffer.data(), buffer.size(), true);

    // Write the merged content to each of return ids.
    for (size_t i = 0; i < return_ids.size(); i++) {
      results->push_back(std::make_shared<RayObject>(memory_buffer, nullptr));
    }

    return Status::OK();
  }

  CoreWorker worker_;
  int64_t prev_seq_no_ = 0;
};

}  // namespace ray

int main(int argc, char **argv) {
  RAY_CHECK(argc == 4);
  auto store_socket = std::string(argv[1]);
  auto raylet_socket = std::string(argv[2]);
  auto node_manager_port = std::stoi(std::string(argv[3]));

  ray::gcs::GcsClientOptions gcs_options("127.0.0.1", 6379, "");
  ray::MockWorker worker(store_socket, raylet_socket, node_manager_port, gcs_options);
  worker.StartExecutingTasks();
  return 0;
}
