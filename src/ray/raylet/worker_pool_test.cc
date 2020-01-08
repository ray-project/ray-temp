#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/constants.h"
#include "ray/raylet/node_manager.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

int NUM_WORKERS_PER_PROCESS = 3;
int MAXIMUM_STARTUP_CONCURRENCY = 5;

std::vector<Language> LANGUAGES = {Language::PYTHON, Language::JAVA};

class WorkerPoolMock : public WorkerPool {
 public:
  WorkerPoolMock()
      : WorkerPoolMock(
            {{Language::PYTHON,
              {"dummy_py_worker_command", "--foo=RAY_WORKER_NUM_WORKERS_PLACEHOLDER"}},
             {Language::JAVA,
              {"dummy_java_worker_command",
               "--foo=RAY_WORKER_NUM_WORKERS_PLACEHOLDER"}}}) {}

  explicit WorkerPoolMock(const WorkerCommandMap &worker_commands)
      : WorkerPool(0, MAXIMUM_STARTUP_CONCURRENCY, nullptr, worker_commands),
        last_worker_process_() {
    for (auto &entry : states_by_lang_) {
      entry.second.num_workers_per_process = NUM_WORKERS_PER_PROCESS;
    }
  }

  ~WorkerPoolMock() {
    // Avoid killing real processes
    states_by_lang_.clear();
  }

  void StartWorkerProcess(const Language &language,
                          const std::vector<std::string> &dynamic_options = {}) {
    WorkerPool::StartWorkerProcess(language, dynamic_options);
  }

  WorkerProcessHandle StartProcess(
      const std::vector<std::string> &worker_command_args) override {
    // A non-null process handle that points to an invalid process object
    // is used to represent a dummy process for testing.
    last_worker_process_ = std::make_shared<WorkerProcess>();
    worker_commands_by_proc_[last_worker_process_] = worker_command_args;
    return last_worker_process_;
  }

  void WarnAboutSize() override {}

  WorkerProcessHandle LastStartedWorkerProcess() const { return last_worker_process_; }

  const std::vector<std::string> &GetWorkerCommand(WorkerProcessHandle proc) {
    return worker_commands_by_proc_[proc];
  }

  int NumWorkersStarting() const {
    int total = 0;
    for (auto &state_entry : states_by_lang_) {
      for (auto &process_entry : state_entry.second.starting_worker_processes) {
        total += process_entry.second;
      }
    }
    return total;
  }

  int NumWorkerProcessesStarting() const {
    int total = 0;
    for (auto &entry : states_by_lang_) {
      total += entry.second.starting_worker_processes.size();
    }
    return total;
  }

 private:
  WorkerProcessHandle last_worker_process_;
  // The worker commands by process.
  std::unordered_map<WorkerProcessHandle, std::vector<std::string>>
      worker_commands_by_proc_;
};

class WorkerPoolTest : public ::testing::Test {
 public:
  WorkerPoolTest()
      : worker_pool_(),
        io_service_(),
        error_message_type_(1),
        client_call_manager_(io_service_) {}

  std::shared_ptr<Worker> CreateWorker(WorkerProcessHandle proc,
                                       const Language &language = Language::PYTHON) {
    std::function<void(LocalClientConnection &)> client_handler =
        [this](LocalClientConnection &client) { HandleNewClient(client); };
    std::function<void(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *)>
        message_handler = [this](std::shared_ptr<LocalClientConnection> client,
                                 int64_t message_type, const uint8_t *message) {
          HandleMessage(client, message_type, message);
        };
    local_stream_protocol::socket socket(io_service_);
    auto client =
        LocalClientConnection::Create(client_handler, message_handler, std::move(socket),
                                      "worker", {}, error_message_type_);
    return std::shared_ptr<Worker>(new Worker(WorkerID::FromRandom(), proc, language, -1,
                                              client, client_call_manager_));
  }

  void SetWorkerCommands(const WorkerCommandMap &worker_commands) {
    WorkerPoolMock worker_pool(worker_commands);
    this->worker_pool_ = std::move(worker_pool);
  }

 protected:
  WorkerPoolMock worker_pool_;
  boost::asio::io_service io_service_;
  int64_t error_message_type_;
  rpc::ClientCallManager client_call_manager_;

 private:
  void HandleNewClient(LocalClientConnection &){};
  void HandleMessage(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *){};
};

static inline TaskSpecification ExampleTaskSpec(
    const ActorID actor_id = ActorID::Nil(), const Language &language = Language::PYTHON,
    const ActorID actor_creation_id = ActorID::Nil(),
    const std::vector<std::string> &dynamic_worker_options = {}) {
  rpc::TaskSpec message;
  message.set_language(language);
  if (!actor_id.IsNil()) {
    message.set_type(TaskType::ACTOR_TASK);
    message.mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  } else if (!actor_creation_id.IsNil()) {
    message.set_type(TaskType::ACTOR_CREATION_TASK);
    message.mutable_actor_creation_task_spec()->set_actor_id(actor_creation_id.Binary());
    for (const auto &option : dynamic_worker_options) {
      message.mutable_actor_creation_task_spec()->add_dynamic_worker_options(option);
    }
  } else {
    message.set_type(TaskType::NORMAL_TASK);
  }
  return TaskSpecification(std::move(message));
}

TEST_F(WorkerPoolTest, CompareWorkerProcessObjects) {
  typedef WorkerProcessHandle T;
  T a = std::make_shared<T::element_type>(), b = std::make_shared<T::element_type>();
  T empty = T();
  ASSERT_TRUE(std::less<T>()(a, b) ^ std::less<T>()(b, a));
  ASSERT_TRUE(std::equal_to<T>()(a, a));
  ASSERT_TRUE(!std::equal_to<T>()(a, b));
  ASSERT_TRUE(!std::equal_to<T>()(b, a));
  ASSERT_TRUE(std::less<T>()(empty, a));
  ASSERT_TRUE(!std::less<T>()(a, empty));
  ASSERT_TRUE(!std::equal_to<T>()(empty, a));
  ASSERT_TRUE(!std::equal_to<T>()(a, empty));
}

TEST_F(WorkerPoolTest, HandleWorkerRegistration) {
  worker_pool_.StartWorkerProcess(Language::PYTHON);
  WorkerProcessHandle proc = worker_pool_.LastStartedWorkerProcess();
  std::vector<std::shared_ptr<Worker>> workers;
  for (int i = 0; i < NUM_WORKERS_PER_PROCESS; i++) {
    workers.push_back(CreateWorker(proc));
  }
  for (const auto &worker : workers) {
    // Check that there's still a starting worker process
    // before all workers have been registered
    ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(), 1);
    // Check that we cannot lookup the worker before it's registered.
    ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), nullptr);
    RAY_CHECK_OK(worker_pool_.RegisterWorker(worker));
    // Check that we can lookup the worker after it's registered.
    ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), worker);
  }
  // Check that there's no starting worker process
  ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(), 0);
  for (const auto &worker : workers) {
    worker_pool_.DisconnectWorker(worker);
    // Check that we cannot lookup the worker after it's disconnected.
    ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), nullptr);
  }
}

TEST_F(WorkerPoolTest, StartupWorkerProcessCount) {
  std::string num_workers_arg =
      std::string("--foo=") + std::to_string(NUM_WORKERS_PER_PROCESS);
  std::vector<std::vector<std::string>> worker_commands = {
      {{"dummy_py_worker_command", num_workers_arg},
       {"dummy_java_worker_command", num_workers_arg}}};
  int desired_initial_worker_process_count_per_language = 100;
  int expected_worker_process_count =
      static_cast<int>(std::ceil(static_cast<double>(MAXIMUM_STARTUP_CONCURRENCY) /
                                 NUM_WORKERS_PER_PROCESS * LANGUAGES.size()));
  ASSERT_TRUE(expected_worker_process_count <
              static_cast<int>(desired_initial_worker_process_count_per_language *
                               LANGUAGES.size()));
  WorkerProcessHandle last_started_worker_process;
  for (int i = 0; i < desired_initial_worker_process_count_per_language; i++) {
    for (size_t j = 0; j < LANGUAGES.size(); j++) {
      worker_pool_.StartWorkerProcess(LANGUAGES[j]);
      ASSERT_TRUE(worker_pool_.NumWorkerProcessesStarting() <=
                  expected_worker_process_count);
      if (last_started_worker_process != worker_pool_.LastStartedWorkerProcess()) {
        last_started_worker_process = worker_pool_.LastStartedWorkerProcess();
        const auto &real_command =
            worker_pool_.GetWorkerCommand(worker_pool_.LastStartedWorkerProcess());
        ASSERT_EQ(real_command, worker_commands[j]);
      } else {
        ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(),
                  expected_worker_process_count);
        ASSERT_TRUE(static_cast<int>(i * LANGUAGES.size() + j) >=
                    expected_worker_process_count);
      }
    }
  }
  // Check number of starting workers
  ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(), expected_worker_process_count);
}

TEST_F(WorkerPoolTest, InitialWorkerProcessCount) {
  worker_pool_.Start(1);
  // Here we try to start only 1 worker for each worker language. But since each worker
  // process contains exactly NUM_WORKERS_PER_PROCESS (3) workers here, it's expected to
  // see 3 workers for each worker language, instead of 1.
  ASSERT_NE(worker_pool_.NumWorkersStarting(), 1 * LANGUAGES.size());
  ASSERT_EQ(worker_pool_.NumWorkersStarting(),
            NUM_WORKERS_PER_PROCESS * LANGUAGES.size());
  ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(), LANGUAGES.size());
}

TEST_F(WorkerPoolTest, HandleWorkerPushPop) {
  // Try to pop a worker from the empty pool and make sure we don't get one.
  std::shared_ptr<Worker> popped_worker;
  const auto task_spec = ExampleTaskSpec();
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_EQ(popped_worker, nullptr);

  // Create some workers.
  std::unordered_set<std::shared_ptr<Worker>> workers;
  workers.insert(CreateWorker(std::make_shared<WorkerProcess>()));
  workers.insert(CreateWorker(std::make_shared<WorkerProcess>()));
  // Add the workers to the pool.
  for (auto &worker : workers) {
    worker_pool_.PushWorker(worker);
  }

  // Pop two workers and make sure they're one of the workers we created.
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_EQ(popped_worker, nullptr);
}

TEST_F(WorkerPoolTest, PopActorWorker) {
  // Create a worker.
  auto worker = CreateWorker(std::make_shared<WorkerProcess>());
  // Add the worker to the pool.
  worker_pool_.PushWorker(worker);

  // Assign an actor ID to the worker.
  const auto task_spec = ExampleTaskSpec();
  auto actor = worker_pool_.PopWorker(task_spec);
  const auto job_id = JobID::FromInt(1);
  auto actor_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);
  actor->AssignActorId(actor_id);
  worker_pool_.PushWorker(actor);

  // Check that there are no more non-actor workers.
  ASSERT_EQ(worker_pool_.PopWorker(task_spec), nullptr);
  // Check that we can pop the actor worker.
  const auto actor_task_spec = ExampleTaskSpec(actor_id);
  actor = worker_pool_.PopWorker(actor_task_spec);
  ASSERT_EQ(actor, worker);
  ASSERT_EQ(actor->GetActorId(), actor_id);
}

TEST_F(WorkerPoolTest, PopWorkersOfMultipleLanguages) {
  // Create a Python Worker, and add it to the pool
  auto py_worker = CreateWorker(std::make_shared<WorkerProcess>(), Language::PYTHON);
  worker_pool_.PushWorker(py_worker);
  // Check that no worker will be popped if the given task is a Java task
  const auto java_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::JAVA);
  ASSERT_EQ(worker_pool_.PopWorker(java_task_spec), nullptr);
  // Check that the worker can be popped if the given task is a Python task
  const auto py_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON);
  ASSERT_NE(worker_pool_.PopWorker(py_task_spec), nullptr);

  // Create a Java Worker, and add it to the pool
  auto java_worker = CreateWorker(std::make_shared<WorkerProcess>(), Language::JAVA);
  worker_pool_.PushWorker(java_worker);
  // Check that the worker will be popped now for Java task
  ASSERT_NE(worker_pool_.PopWorker(java_task_spec), nullptr);
}

TEST_F(WorkerPoolTest, StartWorkerWithDynamicOptionsCommand) {
  const std::vector<std::string> java_worker_command = {
      "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER_0", "dummy_java_worker_command",
      "--foo=RAY_WORKER_NUM_WORKERS_PLACEHOLDER",
      "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER_1"};
  SetWorkerCommands({{Language::PYTHON, {"dummy_py_worker_command"}},
                     {Language::JAVA, java_worker_command}});

  const auto job_id = JobID::FromInt(1);
  TaskSpecification task_spec = ExampleTaskSpec(
      ActorID::Nil(), Language::JAVA,
      ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1), {"test_op_0", "test_op_1"});
  worker_pool_.StartWorkerProcess(Language::JAVA, task_spec.DynamicWorkerOptions());
  const auto real_command =
      worker_pool_.GetWorkerCommand(worker_pool_.LastStartedWorkerProcess());
  ASSERT_EQ(real_command,
            std::vector<std::string>(
                {"test_op_0", "dummy_java_worker_command", "--foo=1", "test_op_1"}));
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
