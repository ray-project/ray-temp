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

#include "ray/util/process.h"

#ifdef _WIN32
#include <process.h>
#else
#include <signal.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/wait.h>
#endif
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <string>
#include <vector>

#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

class ProcessFD {
  pid_t pid_;
  intptr_t fd_;

 public:
  ~ProcessFD();
  ProcessFD();
  ProcessFD(pid_t pid, intptr_t fd = -1);
  ProcessFD(const ProcessFD &other);
  ProcessFD(ProcessFD &&other);
  ProcessFD &operator=(const ProcessFD &other);
  ProcessFD &operator=(ProcessFD &&other);
  intptr_t CloneFD() const;
  void CloseFD();
  intptr_t GetFD() const;
  pid_t GetId() const;

  // Fork + exec combo. Returns -1 for the PID on failure.
  static ProcessFD spawnvp(const char *argv[], std::error_code &ec, bool decouple) {
    ec = std::error_code();
    intptr_t fd;
    pid_t pid;
#ifdef _WIN32
    (void)decouple;  // Windows doesn't require anything particular for decoupling.
    std::vector<std::string> args;
    for (size_t i = 0; argv[i]; ++i) {
      args.push_back(argv[i]);
    }
    std::string cmds[] = {std::string(), CreateCommandLine(args)};
    if (GetFileName(args.at(0)).find('.') == std::string::npos) {
      // Some executables might be missing an extension.
      // Append a single "." to prevent automatic appending of extensions by the system.
      std::vector<std::string> args_direct_call = args;
      args_direct_call[0] += ".";
      cmds[0] = CreateCommandLine(args_direct_call);
    }
    bool succeeded = false;
    PROCESS_INFORMATION pi = {};
    for (int attempt = 0; attempt < sizeof(cmds) / sizeof(*cmds); ++attempt) {
      std::string &cmd = cmds[attempt];
      if (!cmd.empty()) {
        (void)cmd.c_str();  // We'll need this to be null-terminated (but mutable) below
        TCHAR *cmdline = &*cmd.begin();
        STARTUPINFO si = {sizeof(si)};
        if (CreateProcessA(NULL, cmdline, NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi)) {
          succeeded = true;
          break;
        }
      }
    }
    if (succeeded) {
      CloseHandle(pi.hThread);
      fd = reinterpret_cast<intptr_t>(pi.hProcess);
      pid = pi.dwProcessId;
    } else {
      ec = std::error_code(GetLastError(), std::system_category());
      fd = -1;
      pid = -1;
    }
#else
    // TODO(mehrdadn): Use clone() on Linux or posix_spawnp() on Mac to avoid duplicating
    // file descriptors into the child process, as that can be problematic.
    int pipefds[2];  // Create pipe to get PID & track lifetime
    if (pipe(pipefds) == -1) {
      pipefds[0] = pipefds[1] = -1;
    }
    pid = pipefds[1] != -1 ? fork() : -1;
    if (pid <= 0 && pipefds[0] != -1) {
      close(pipefds[0]);  // not the parent, so close the read end of the pipe
      pipefds[0] = -1;
    }
    if (pid != 0 && pipefds[1] != -1) {
      close(pipefds[1]);  // not the child, so close the write end of the pipe
      pipefds[1] = -1;
    }
    if (pid == 0) {
      // Child process case. Reset the SIGCHLD handler.
      signal(SIGCHLD, SIG_DFL);
      // If process needs to be decoupled, double-fork to avoid zombies.
      if (pid_t pid2 = decouple ? fork() : 0) {
        _exit(pid2 == -1 ? errno : 0);  // Parent of grandchild; must exit
      }
      // This is the spawned process. Any intermediate parent is now dead.
      pid_t my_pid = getpid();
      if (write(pipefds[1], &my_pid, sizeof(my_pid)) == sizeof(my_pid)) {
        execvp(argv[0], const_cast<char *const *>(argv));
      }
      _exit(errno);  // fork() succeeded and exec() failed, so abort the child
    }
    if (pid > 0) {
      // Parent process case
      if (decouple) {
        int s;
        (void)waitpid(pid, &s, 0);  // can't do much if this fails, so ignore return value
      }
      int r = read(pipefds[0], &pid, sizeof(pid));
      (void)r;  // can't do much if this fails, so ignore return value
    }
    if (decouple) {
      fd = pipefds[0];  // grandchild, but we can use this to track its lifetime
    } else {
      fd = -1;  // direct child, so we can use the PID to track its lifetime
      if (pipefds[0] != -1) {
        close(pipefds[0]);
        pipefds[0] = -1;
      }
    }
    if (pid == -1) {
      ec = std::error_code(errno, std::system_category());
    }
#endif
    return ProcessFD(pid, fd);
  }
};

ProcessFD::~ProcessFD() {
  if (fd_ != -1) {
    bool success;
#ifdef _WIN32
    success = !!CloseHandle(reinterpret_cast<HANDLE>(fd_));
#else
    success = close(static_cast<int>(fd_)) == 0;
#endif
    RAY_CHECK(success) << "error " << errno << " closing process " << pid_ << " FD";
  }
}

ProcessFD::ProcessFD() : pid_(-1), fd_(-1) {}

ProcessFD::ProcessFD(pid_t pid, intptr_t fd) : pid_(pid), fd_(fd) {
  if (pid != -1) {
    bool process_does_not_exist = false;
    std::error_code error;
#ifdef _WIN32
    if (fd == -1) {
      BOOL inheritable = FALSE;
      DWORD permissions = MAXIMUM_ALLOWED;
      HANDLE handle = OpenProcess(permissions, inheritable, static_cast<DWORD>(pid));
      if (handle) {
        fd_ = reinterpret_cast<intptr_t>(handle);
      } else {
        DWORD error_code = GetLastError();
        error = std::error_code(error_code, std::system_category());
        if (error_code == ERROR_INVALID_PARAMETER) {
          process_does_not_exist = true;
        }
      }
    } else {
      RAY_CHECK(pid == GetProcessId(reinterpret_cast<HANDLE>(fd)));
    }
#else
    if (kill(pid, 0) == -1 && errno == ESRCH) {
      process_does_not_exist = true;
    }
#endif
    // Don't verify anything if the PID is too high, since that's used for testing
    if (pid < PID_MAX_LIMIT) {
      if (process_does_not_exist) {
        // NOTE: This indicates a race condition where a process died and its process
        // table entry was removed before the ProcessFD could be instantiated. For
        // processes owned by this process, we should make this impossible by keeping
        // the SIGCHLD signal. For processes not owned by this process, we need to come up
        // with a strategy to create this class in a way that avoids race conditions.
        RAY_LOG(ERROR) << "Process " << pid << " does not exist.";
      }
      if (error) {
        // TODO(mehrdadn): Should this be fatal, or perhaps returned as an error code?
        // Failures might occur due to reasons such as permission issues.
        RAY_LOG(ERROR) << "error " << error << " opening process " << pid << ": "
                       << error.message();
      }
    }
  }
}

ProcessFD::ProcessFD(const ProcessFD &other) : ProcessFD(other.pid_, other.CloneFD()) {}

ProcessFD::ProcessFD(ProcessFD &&other) : ProcessFD() { *this = std::move(other); }

ProcessFD &ProcessFD::operator=(const ProcessFD &other) {
  if (this != &other) {
    // Construct a copy, then call the move constructor
    *this = static_cast<ProcessFD>(other);
  }
  return *this;
}

ProcessFD &ProcessFD::operator=(ProcessFD &&other) {
  if (this != &other) {
    // We use swap() to make sure the argument is actually moved from
    using std::swap;
    swap(pid_, other.pid_);
    swap(fd_, other.fd_);
  }
  return *this;
}

intptr_t ProcessFD::CloneFD() const {
  intptr_t fd;
  if (fd_ != -1) {
#ifdef _WIN32
    HANDLE handle;
    BOOL inheritable = FALSE;
    fd = DuplicateHandle(GetCurrentProcess(), reinterpret_cast<HANDLE>(fd_),
                         GetCurrentProcess(), &handle, 0, inheritable,
                         DUPLICATE_SAME_ACCESS)
             ? reinterpret_cast<intptr_t>(handle)
             : -1;
#else
    fd = dup(static_cast<int>(fd_));
#endif
    RAY_DCHECK(fd != -1);
  } else {
    fd = -1;
  }
  return fd;
}

void ProcessFD::CloseFD() { fd_ = -1; }

intptr_t ProcessFD::GetFD() const { return fd_; }

pid_t ProcessFD::GetId() const { return pid_; }

Process::~Process() {}

Process::Process() {}

Process::Process(const Process &) = default;

Process::Process(Process &&) = default;

Process &Process::operator=(Process other) {
  p_ = std::move(other.p_);
  return *this;
}

Process::Process(pid_t pid) { p_ = std::make_shared<ProcessFD>(pid); }

Process::Process(const char *argv[], void *io_service, std::error_code &ec,
                 bool decouple) {
  (void)io_service;
  ProcessFD procfd = ProcessFD::spawnvp(argv, ec, decouple);
  if (!ec) {
    p_ = std::make_shared<ProcessFD>(std::move(procfd));
  }
}

std::error_code Process::Call(const std::vector<std::string> &args) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), NULL, ec, true);
  if (!ec) {
    int return_code = proc.Wait();
    if (return_code != 0) {
      ec = std::error_code(return_code, std::system_category());
    }
  }
  return ec;
}

Process Process::CreateNewDummy() {
  pid_t pid = -1;
  Process result(pid);
  return result;
}

Process Process::FromPid(pid_t pid) {
  RAY_DCHECK(pid >= 0);
  Process result(pid);
  return result;
}

const void *Process::Get() const { return p_ ? &*p_ : NULL; }

pid_t Process::GetId() const { return p_ ? p_->GetId() : -1; }

bool Process::IsNull() const { return !p_; }

bool Process::IsValid() const { return GetId() != -1; }

std::pair<Process, std::error_code> Process::Spawn(const std::vector<std::string> &args,
                                                   bool decouple,
                                                   const std::string &pid_file) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code error;
  Process proc(&*argv.begin(), NULL, error, decouple);
  if (!error && !pid_file.empty()) {
    std::ofstream file(pid_file, std::ios_base::out | std::ios_base::trunc);
    file << proc.GetId() << std::endl;
    RAY_CHECK(file.good());
  }
  return std::make_pair(std::move(proc), error);
}

int Process::Wait() const {
  int status;
  if (p_) {
    pid_t pid = p_->GetId();
    if (pid >= 0) {
      std::error_code error;
      intptr_t fd = p_->GetFD();
#ifdef _WIN32
      HANDLE handle = fd != -1 ? reinterpret_cast<HANDLE>(fd) : NULL;
      DWORD exit_code = STILL_ACTIVE;
      if (WaitForSingleObject(handle, INFINITE) == WAIT_OBJECT_0 &&
          GetExitCodeProcess(handle, &exit_code)) {
        status = static_cast<int>(exit_code);
      } else {
        error = std::error_code(GetLastError(), std::system_category());
        status = -1;
      }
#else
      if (fd != -1) {
        unsigned char buf[1 << 8];
        ptrdiff_t r;
        while ((r = read(fd, buf, sizeof(buf))) > 0) {
          // Keep reading until socket terminates
        }
        status = r == -1 ? -1 : 0;
      } else if (waitpid(pid, &status, 0) == -1) {
        error = std::error_code(errno, std::system_category());
      }
#endif
      if (error) {
        RAY_LOG(ERROR) << "Failed to wait for process " << pid << " with error " << error
                       << ": " << error.message();
      }
    } else {
      // (Dummy process case)
      status = 0;
    }
  } else {
    // (Null process case)
    status = -1;
  }
  return status;
}

void Process::Kill() {
  if (p_) {
    pid_t pid = p_->GetId();
    if (pid >= 0) {
      std::error_code error;
      intptr_t fd = p_->GetFD();
#ifdef _WIN32
      HANDLE handle = fd != -1 ? reinterpret_cast<HANDLE>(fd) : NULL;
      if (!::TerminateProcess(handle, ERROR_PROCESS_ABORTED)) {
        error = std::error_code(GetLastError(), std::system_category());
      }
#else
      (void)fd;
      if (kill(pid, SIGKILL) != 0) {
        error = std::error_code(errno, std::system_category());
      }
#endif
      if (error) {
        RAY_LOG(ERROR) << "Failed to kill process " << pid << " with error " << error
                       << ": " << error.message();
      }
    } else {
      // (Dummy process case)
      // Theoretically we could keep around an exit code here for Wait() to return,
      // but we might as well pretend this fake process had already finished running.
      // So don't bother doing anything.
    }
  } else {
    // (Null process case)
  }
}

}  // namespace ray

namespace std {

bool equal_to<ray::Process>::operator()(const ray::Process &x,
                                        const ray::Process &y) const {
  return !x.IsNull()
             ? !y.IsNull()
                   ? x.IsValid()
                         ? y.IsValid() ? equal_to<pid_t>()(x.GetId(), y.GetId()) : false
                         : y.IsValid() ? false
                                       : equal_to<void const *>()(x.Get(), y.Get())
                   : false
             : y.IsNull();
}

size_t hash<ray::Process>::operator()(const ray::Process &value) const {
  return !value.IsNull() ? value.IsValid() ? hash<pid_t>()(value.GetId())
                                           : hash<void const *>()(value.Get())
                         : size_t();
}

}  // namespace std
