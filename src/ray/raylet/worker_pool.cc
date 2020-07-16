// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/worker_pool.h"

#include <algorithm>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "ray/common/constants.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace {

// A helper function to get a worker from a list.
std::shared_ptr<ray::raylet::Worker> GetWorker(
    const std::unordered_set<std::shared_ptr<ray::raylet::Worker>> &worker_pool,
    const std::shared_ptr<ray::ClientConnection> &connection) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if ((*it)->Connection() == connection) {
      return (*it);
    }
  }
  return nullptr;
}

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
bool RemoveWorker(std::unordered_set<std::shared_ptr<ray::raylet::Worker>> &worker_pool,
                  const std::shared_ptr<ray::raylet::Worker> &worker) {
  return worker_pool.erase(worker) > 0;
}

}  // namespace

namespace ray {

namespace raylet {

WorkerPool::WorkerPool(
    boost::asio::io_service &io_service, uint32_t default_num_initial_workers,
    int maximum_startup_concurrency, int min_worker_port, int max_worker_port,
    std::shared_ptr<gcs::GcsClient> gcs_client, const WorkerCommandMap &worker_commands,
    const std::unordered_map<std::string, std::string> &raylet_config,
    std::function<void()> starting_worker_timeout_callback,
    std::function<boost::optional<rpc::JobConfig>(const JobID &)> job_config_getter)
    : io_service_(&io_service),
      adaptive_num_initial_workers_(default_num_initial_workers),
      maximum_startup_concurrency_(maximum_startup_concurrency),
      gcs_client_(std::move(gcs_client)),
      raylet_config_(raylet_config),
      starting_worker_timeout_callback_(starting_worker_timeout_callback),
      job_config_getter_(job_config_getter) {
  RAY_CHECK(maximum_startup_concurrency > 0);
#ifndef _WIN32
  // Ignore SIGCHLD signals. If we don't do this, then worker processes will
  // become zombies instead of dying gracefully.
  signal(SIGCHLD, SIG_IGN);
#endif
  for (const auto &entry : worker_commands) {
    // Initialize the pool state for this language.
    auto &state = states_by_lang_[entry.first];
    switch (entry.first) {
    case Language::PYTHON:
      state.num_workers_per_process =
          RayConfig::instance().num_workers_per_process_python();
      break;
    case Language::JAVA:
      state.num_workers_per_process =
          RayConfig::instance().num_workers_per_process_java();
      break;
    default:
      RAY_LOG(FATAL) << "The number of workers per process for "
                     << Language_Name(entry.first) << " worker is not set.";
    }
    state.multiple_for_warning =
        std::max(state.num_workers_per_process, maximum_startup_concurrency);
    // Set worker command for this language.
    state.worker_command = entry.second;
    RAY_CHECK(!state.worker_command.empty()) << "Worker command must not be empty.";
  }
  // Initialize free ports list with all ports in the specified range.
  if (min_worker_port != 0) {
    if (max_worker_port == 0) {
      max_worker_port = 65535;  // Maximum valid port number.
    }
    RAY_CHECK(min_worker_port > 0 && min_worker_port <= 65535);
    RAY_CHECK(max_worker_port >= min_worker_port && max_worker_port <= 65535);
    free_ports_ = std::unique_ptr<std::queue<int>>(new std::queue<int>());
    for (int port = min_worker_port; port <= max_worker_port; port++) {
      free_ports_->push(port);
    }
  }
}

WorkerPool::~WorkerPool() {
  std::unordered_set<Process> procs_to_kill;
  for (const auto &entry : states_by_lang_) {
    // Kill all registered workers. NOTE(swang): This assumes that the registered
    // workers were started by the pool.
    for (const auto &worker : entry.second.registered_workers) {
      procs_to_kill.insert(worker->GetProcess());
    }
    // Kill all the workers that have been started but not registered.
    for (const auto &starting_worker : entry.second.starting_worker_processes) {
      procs_to_kill.insert(starting_worker.first);
    }
  }
  for (Process proc : procs_to_kill) {
    proc.Kill();
    // NOTE: Avoid calling Wait() here. It fails with ECHILD, as SIGCHLD is disabled.
  }
}

uint32_t WorkerPool::Size(const Language &language) const {
  const auto state = states_by_lang_.find(language);
  if (state == states_by_lang_.end()) {
    return 0;
  } else {
    return static_cast<uint32_t>(state->second.idle.size() +
                                 state->second.idle_actor.size());
  }
}

Process WorkerPool::StartWorkerProcess(const Language &language, const JobID &job_id,
                                       std::vector<std::string> dynamic_options) {
  auto job_config = job_config_getter_(job_id);
  if (!job_config) {
    RAY_LOG(INFO) << "Job config of job " << job_id << " are not local yet.";
    return Process();
  }

  auto &state = GetStateForLanguage(language);
  // If we are already starting up too many workers, then return without starting
  // more.
  int starting_workers = 0;
  for (auto &entry : state.starting_worker_processes) {
    starting_workers += entry.second;
  }
  if (starting_workers >= maximum_startup_concurrency_) {
    // Workers have been started, but not registered. Force start disabled -- returning.
    RAY_LOG(DEBUG) << "Worker not started, " << starting_workers
                   << " workers of language type " << static_cast<int>(language)
                   << " pending registration";
    return Process();
  }
  // Either there are no workers pending registration or the worker start is being forced.
  RAY_LOG(DEBUG) << "Starting new worker process, current pool has "
                 << state.idle_actor.size() << " actor workers, and " << state.idle.size()
                 << " non-actor workers";

  int workers_to_start;
  if (dynamic_options.empty() && language == Language::JAVA) {
    workers_to_start = job_config->num_java_workers_per_process();
  } else {
    workers_to_start = 1;
  }

  if (!job_config->jvm_options().empty()) {
    // Note that we push the item to the front of the vector to make
    // sure this is the freshest option than others.
    dynamic_options.insert(dynamic_options.begin(), job_config->jvm_options().begin(),
                           job_config->jvm_options().end());
  }

  // Extract pointers from the worker command to pass into execvp.
  std::vector<std::string> worker_command_args;
  size_t dynamic_option_index = 0;
  bool worker_raylet_config_placeholder_found = false;
  for (auto const &token : state.worker_command) {
    const auto option_placeholder =
        kWorkerDynamicOptionPlaceholderPrefix + std::to_string(dynamic_option_index);

    if (token == option_placeholder) {
      if (!dynamic_options.empty()) {
        RAY_CHECK(dynamic_option_index < dynamic_options.size());
        auto options = ParseCommandLine(dynamic_options[dynamic_option_index]);
        worker_command_args.insert(worker_command_args.end(), options.begin(),
                                   options.end());
        ++dynamic_option_index;
      }
      continue;
    }

    if (token == kWorkerRayletConfigPlaceholder) {
      worker_raylet_config_placeholder_found = true;
      switch (language) {
      case Language::JAVA:
        for (auto &entry : raylet_config_) {
          if (entry.first == "num_workers_per_process_java") {
            continue;
          }
          std::string arg;
          arg.append("-Dray.raylet.config.");
          arg.append(entry.first);
          arg.append("=");
          arg.append(entry.second);
          worker_command_args.push_back(arg);
        }
        // The value of `num_workers_per_process_java` may change depends on whether
        // dynamic options is empty, so we can't use the value in `RayConfig`. We always
        // overwrite the value here.
        worker_command_args.push_back(
            "-Dray.raylet.config.num_workers_per_process_java=" +
            std::to_string(workers_to_start));
        worker_command_args.push_back("-Dray.job.num-java-workers-per-process=" +
                                      std::to_string(workers_to_start));
        break;
      default:
        RAY_LOG(FATAL)
            << "Raylet config placeholder is not supported for worker language "
            << language;
      }
      continue;
    }

    worker_command_args.push_back(token);
  }

  // Currently only Java worker process supports multi-worker.
  if (language == Language::JAVA) {
    RAY_CHECK(worker_raylet_config_placeholder_found)
        << "The " << kWorkerRayletConfigPlaceholder
        << " placeholder is not found in worker command.";
  }

  // TODO(kfstorm): Set up environment variables in a later PR.
  Process proc = StartProcess(worker_command_args);
  // If the pid is reused between processes, the old process must have exited.
  // So it's safe to bind the pid with another job ID.
  RAY_LOG(INFO) << "Worker process " << proc.GetId() << " is bound to job " << job_id;
  state.worker_pids_to_assigned_jobs[proc.GetId()] = job_id;
  RAY_LOG(DEBUG) << "Started worker process of " << workers_to_start
                 << " worker(s) with pid " << proc.GetId();
  MonitorStartingWorkerProcess(proc, language);
  state.starting_worker_processes.emplace(proc, workers_to_start);
  return proc;
}

void WorkerPool::MonitorStartingWorkerProcess(const Process &proc,
                                              const Language &language) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      *io_service_, boost::posix_time::seconds(
                        RayConfig::instance().worker_register_timeout_seconds()));
  // Capture timer in lambda to copy it once, so that it can avoid destructing timer.
  timer->async_wait(
      [timer, language, proc, this](const boost::system::error_code e) -> void {
        // check the error code.
        auto &state = this->GetStateForLanguage(language);
        // Since this process times out to start, remove it from starting_worker_processes
        // to avoid the zombie worker.
        auto it = state.starting_worker_processes.find(proc);
        if (it != state.starting_worker_processes.end()) {
          RAY_LOG(INFO) << "Some workers of the worker process(" << proc.GetId()
                        << ") have not registered to raylet within timeout.";
          state.starting_worker_processes.erase(it);
          starting_worker_timeout_callback_();
        }
      });
}

Process WorkerPool::StartProcess(const std::vector<std::string> &worker_command_args) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::stringstream stream;
    stream << "Starting worker process with command:";
    for (const auto &arg : worker_command_args) {
      stream << " " << arg;
    }
    RAY_LOG(DEBUG) << stream.str();
  }

  // Launch the process to create the worker.
  std::error_code ec;
  std::vector<const char *> argv;
  for (const std::string &arg : worker_command_args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(NULL);
  Process child(argv.data(), io_service_, ec);
  if (!child.IsValid() || ec) {
    // The worker failed to start. This is a fatal error.
    RAY_LOG(FATAL) << "Failed to start worker with return value " << ec << ": "
                   << ec.message();
  }
  return child;
}

Status WorkerPool::GetNextFreePort(int *port) {
  if (!free_ports_) {
    *port = 0;
    return Status::OK();
  }

  // Try up to the current number of ports.
  int current_size = free_ports_->size();
  for (int i = 0; i < current_size; i++) {
    *port = free_ports_->front();
    free_ports_->pop();
    if (CheckFree(*port)) {
      return Status::OK();
    }
    // Return to pool to check later.
    free_ports_->push(*port);
  }
  *port = -1;
  return Status::Invalid(
      "No available ports. Please specify a wider port range using --min-worker-port and "
      "--max-worker-port.");
}

void WorkerPool::MarkPortAsFree(int port) {
  if (free_ports_) {
    RAY_CHECK(port != 0) << "";
    free_ports_->push(port);
  }
}

Status WorkerPool::RegisterWorker(const std::shared_ptr<Worker> &worker, pid_t pid,
                                  int *port) {
  RAY_CHECK(worker);
  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto it = state.starting_worker_processes.find(Process::FromPid(pid));
  if (it == state.starting_worker_processes.end()) {
    RAY_LOG(WARNING) << "Received a register request from an unknown worker " << pid;
    return Status::Invalid("Unknown worker");
  }
  RAY_RETURN_NOT_OK(GetNextFreePort(port));
  RAY_LOG(DEBUG) << "Registering worker with pid " << pid << ", port: " << *port;
  worker->SetAssignedPort(*port);
  worker->SetProcess(it->first);
  it->second--;
  if (it->second == 0) {
    state.starting_worker_processes.erase(it);
  }

  RAY_CHECK(worker->GetProcess().GetId() == pid);
  state.registered_workers.insert(worker);

  auto dedicated_workers_it = state.worker_pids_to_assigned_jobs.find(pid);
  RAY_CHECK(dedicated_workers_it != state.worker_pids_to_assigned_jobs.end());
  worker->AssignJobId(dedicated_workers_it->second);
  // We don't call state.worker_pids_to_assigned_jobs.erase(dedicated_workers_it) here
  // because we allow multi-workers per worker process.

  return Status::OK();
}

Status WorkerPool::RegisterDriver(const std::shared_ptr<Worker> &driver,
                                  const JobID &job_id, int *port) {
  RAY_CHECK(!driver->GetAssignedTaskId().IsNil());
  RAY_RETURN_NOT_OK(GetNextFreePort(port));
  driver->SetAssignedPort(*port);
  auto &state = GetStateForLanguage(driver->GetLanguage());
  state.registered_drivers.insert(std::move(driver));
  driver->AssignJobId(job_id);

  return Status::OK();
}

void WorkerPool::StartInitialWorkersForJob(const JobID &job_id) {
  auto job_config = job_config_getter_(job_id);
  RAY_CHECK(job_config);
  for (auto &entry : states_by_lang_) {
    auto &state = entry.second;
    uint32_t num_initial_workers = adaptive_num_initial_workers_;
    int32_t config_value;
    switch (entry.first) {
    case Language::PYTHON:
      config_value = job_config->num_initial_python_workers();
      break;
    case Language::JAVA:
      config_value = job_config->num_initial_java_workers();
      break;
    default:
      config_value = 0;
      break;
    }
    if (config_value >= 0) {
      num_initial_workers = static_cast<uint32_t>(config_value);
    }
    int num_worker_processes = static_cast<int>(std::ceil(
        static_cast<double>(num_initial_workers) / state.num_workers_per_process));
    for (int i = 0; i < num_worker_processes; i++) {
      StartWorkerProcess(entry.first, job_id);
    }
  }
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredWorker(
    const std::shared_ptr<ClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto worker = GetWorker(entry.second.registered_workers, connection);
    if (worker != nullptr) {
      return worker;
    }
  }
  return nullptr;
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredDriver(
    const std::shared_ptr<ClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto driver = GetWorker(entry.second.registered_drivers, connection);
    if (driver != nullptr) {
      return driver;
    }
  }
  return nullptr;
}

void WorkerPool::PushWorker(const std::shared_ptr<Worker> &worker) {
  // Since the worker is now idle, unset its assigned task ID.
  RAY_CHECK(worker->GetAssignedTaskId().IsNil())
      << "Idle workers cannot have an assigned task ID";
  auto &state = GetStateForLanguage(worker->GetLanguage());

  auto it = state.dedicated_workers_to_tasks.find(worker->GetProcess());
  if (it != state.dedicated_workers_to_tasks.end()) {
    // The worker is used for the actor creation task with dynamic options.
    // Put it into idle dedicated worker pool.
    const auto task_id = it->second;
    state.idle_dedicated_workers[task_id] = worker;
  } else {
    // The worker is not used for the actor creation task without dynamic options.
    // Put the worker to the corresponding idle pool.
    if (worker->GetActorId().IsNil()) {
      state.idle.insert(worker);
    } else {
      state.idle_actor[worker->GetActorId()] = worker;
    }
  }
}

std::shared_ptr<Worker> WorkerPool::PopWorker(const TaskSpecification &task_spec) {
  auto &state = GetStateForLanguage(task_spec.GetLanguage());

  std::shared_ptr<Worker> worker = nullptr;
  Process proc;
  if (task_spec.IsActorCreationTask() && !task_spec.DynamicWorkerOptions().empty()) {
    // Code path of actor creation task with dynamic worker options.
    // Try to pop it from idle dedicated pool.
    auto it = state.idle_dedicated_workers.find(task_spec.TaskId());
    if (it != state.idle_dedicated_workers.end()) {
      // There is an idle dedicated worker for this task.
      worker = std::move(it->second);
      state.idle_dedicated_workers.erase(it);
      // Because we found a worker that can perform this task,
      // we can remove it from dedicated_workers_to_tasks.
      state.dedicated_workers_to_tasks.erase(worker->GetProcess());
      state.tasks_to_dedicated_workers.erase(task_spec.TaskId());
    } else if (!HasPendingWorkerForTask(task_spec.GetLanguage(), task_spec.TaskId())) {
      // We are not pending a registration from a worker for this task,
      // so start a new worker process for this task.
      proc = StartWorkerProcess(task_spec.GetLanguage(), task_spec.JobId(),
                                task_spec.DynamicWorkerOptions());
      if (proc.IsValid()) {
        state.dedicated_workers_to_tasks[proc] = task_spec.TaskId();
        state.tasks_to_dedicated_workers[task_spec.TaskId()] = proc;
      }
    }
  } else if (!task_spec.IsActorTask()) {
    // Code path of normal task or actor creation task without dynamic worker options.
    // Find an available worker which is already assigned to this job.
    for (auto it = state.idle.begin(); it != state.idle.end(); it++) {
      if ((*it)->GetAssignedJobId() != task_spec.JobId()) {
        continue;
      }
      worker = std::move(*it);
      state.idle.erase(it);
      break;
    }
    if (worker == nullptr) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker process.
      proc = StartWorkerProcess(task_spec.GetLanguage(), task_spec.JobId());
    }
  } else {
    // Code path of actor task.
    const auto &actor_id = task_spec.ActorId();
    auto actor_entry = state.idle_actor.find(actor_id);
    if (actor_entry != state.idle_actor.end()) {
      worker = std::move(actor_entry->second);
      state.idle_actor.erase(actor_entry);
    }
  }

  if (worker == nullptr && proc.IsValid()) {
    WarnAboutSize();
  }

  if (worker) {
    RAY_CHECK(worker->GetAssignedJobId() == task_spec.JobId());
  }
  return worker;
}

bool WorkerPool::DisconnectWorker(const std::shared_ptr<Worker> &worker) {
  auto &state = GetStateForLanguage(worker->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_workers, worker));

  stats::CurrentWorker().Record(
      0, {{stats::LanguageKey, Language_Name(worker->GetLanguage())},
          {stats::WorkerPidKey, std::to_string(worker->GetProcess().GetId())}});

  MarkPortAsFree(worker->AssignedPort());
  return RemoveWorker(state.idle, worker);
}

void WorkerPool::DisconnectDriver(const std::shared_ptr<Worker> &driver) {
  auto &state = GetStateForLanguage(driver->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_drivers, driver));
  stats::CurrentDriver().Record(
      0, {{stats::LanguageKey, Language_Name(driver->GetLanguage())},
          {stats::WorkerPidKey, std::to_string(driver->GetProcess().GetId())}});
  MarkPortAsFree(driver->AssignedPort());
}

inline WorkerPool::State &WorkerPool::GetStateForLanguage(const Language &language) {
  auto state = states_by_lang_.find(language);
  RAY_CHECK(state != states_by_lang_.end()) << "Required Language isn't supported.";
  return state->second;
}

std::vector<std::shared_ptr<Worker>> WorkerPool::GetWorkersRunningTasksForJob(
    const JobID &job_id) const {
  std::vector<std::shared_ptr<Worker>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (worker->GetAssignedJobId() == job_id) {
        workers.push_back(worker);
      }
    }
  }

  return workers;
}

const std::vector<std::shared_ptr<Worker>> WorkerPool::GetAllRegisteredWorkers() const {
  std::vector<std::shared_ptr<Worker>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (worker->IsRegistered()) {
        workers.push_back(worker);
      }
    }
  }

  return workers;
}

const std::vector<std::shared_ptr<Worker>> WorkerPool::GetAllRegisteredDrivers() const {
  std::vector<std::shared_ptr<Worker>> drivers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &driver : entry.second.registered_drivers) {
      if (driver->IsRegistered()) {
        drivers.push_back(driver);
      }
    }
  }

  return drivers;
}

void WorkerPool::WarnAboutSize() {
  for (const auto &entry : states_by_lang_) {
    auto state = entry.second;
    int64_t num_workers_started_or_registered = 0;
    num_workers_started_or_registered +=
        static_cast<int64_t>(state.registered_workers.size());
    for (const auto &starting_process : state.starting_worker_processes) {
      num_workers_started_or_registered += starting_process.second;
    }
    int64_t multiple = num_workers_started_or_registered / state.multiple_for_warning;
    std::stringstream warning_message;
    if (multiple >= 3 && multiple > state.last_warning_multiple) {
      // Push an error message to the user if the worker pool tells us that it is
      // getting too big.
      state.last_warning_multiple = multiple;
      warning_message << "WARNING: " << num_workers_started_or_registered << " "
                      << Language_Name(entry.first)
                      << " workers have been started. This could be a result of using "
                      << "a large number of actors, or it could be a consequence of "
                      << "using nested tasks "
                      << "(see https://github.com/ray-project/ray/issues/3644) for "
                      << "some a discussion of workarounds.";
      auto error_data_ptr = gcs::CreateErrorTableData(
          "worker_pool_large", warning_message.str(), current_time_ms());
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

bool WorkerPool::HasPendingWorkerForTask(const Language &language,
                                         const TaskID &task_id) {
  auto &state = GetStateForLanguage(language);
  auto it = state.tasks_to_dedicated_workers.find(task_id);
  return it != state.tasks_to_dedicated_workers.end();
}

std::string WorkerPool::DebugString() const {
  std::stringstream result;
  result << "WorkerPool:";
  for (const auto &entry : states_by_lang_) {
    result << "\n- num " << Language_Name(entry.first)
           << " workers: " << entry.second.registered_workers.size();
    result << "\n- num " << Language_Name(entry.first)
           << " drivers: " << entry.second.registered_drivers.size();
  }
  return result.str();
}

void WorkerPool::RecordMetrics() const {
  for (const auto &entry : states_by_lang_) {
    // Record worker.
    for (auto worker : entry.second.registered_workers) {
      stats::CurrentWorker().Record(
          worker->GetProcess().GetId(),
          {{stats::LanguageKey, Language_Name(worker->GetLanguage())},
           {stats::WorkerPidKey, std::to_string(worker->GetProcess().GetId())}});
    }

    // Record driver.
    for (auto driver : entry.second.registered_drivers) {
      stats::CurrentDriver().Record(
          driver->GetProcess().GetId(),
          {{stats::LanguageKey, Language_Name(driver->GetLanguage())},
           {stats::WorkerPidKey, std::to_string(driver->GetProcess().GetId())}});
    }
  }
}

}  // namespace raylet

}  // namespace ray
