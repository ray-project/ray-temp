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
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Psapi.h>  // EnumProcesses
#include <Windows.h>
#include <Winternl.h>
#include <process.h>
#else
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <string.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <string>
#include <vector>

#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/util.h"

#ifdef __APPLE__
extern char **environ;

// macOS dosn't come with execvpe.
// https://stackoverflow.com/questions/7789750/execve-with-path-search
int execvpe(const char *program, char *const argv[], char *const envp[]) {
  char **saved = environ;
  int rc;
  environ = const_cast<char **>(envp);
  rc = execvp(program, argv);
  environ = saved;
  return rc;
}
#endif

namespace ray {

bool EnvironmentVariableLess::operator()(char a, char b) const {
  // TODO(mehrdadn): This is only used on Windows due to current lack of Unicode support.
  // It should be changed when Process adds Unicode support on Windows.
  return std::less<char>()(tolower(a), tolower(b));
}

bool EnvironmentVariableLess::operator()(const std::string &a,
                                         const std::string &b) const {
  bool result;
#ifdef _WIN32
  result = std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(), *this);
#else
  result = a < b;
#endif
  return result;
}

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
  static ProcessFD spawnvpe(const char *argv[], std::error_code &ec, bool decouple,
                            const ProcessEnvironment &env, const std::string &cwd) {
    ec = std::error_code();
    intptr_t fd;
    pid_t pid;
    ProcessEnvironment new_env;
    for (char *const *e = environ; *e; ++e) {
      RAY_CHECK(*e && **e != '\0') << "environment variable name is absent";
      const char *key_end = strchr(*e + 1 /* +1 is needed for Windows */, '=');
      RAY_CHECK(key_end) << "environment variable value is absent: " << e;
      new_env[std::string(*e, static_cast<size_t>(key_end - *e))] = key_end + 1;
    }
    for (const auto &item : env) {
      new_env[item.first] = item.second;
    }
    std::string new_env_block;
    for (const auto &item : new_env) {
      new_env_block += item.first + '=' + item.second + '\0';
    }
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
        LPCSTR lpCurrentDirectory = cwd.empty() ? NULL : cwd.c_str();
        RAY_UNUSED(
            new_env_block.c_str());  // Ensure there's a final terminator for Windows
        char *const envp = &new_env_block[0];
        if (CreateProcessA(NULL, cmdline, NULL, NULL, FALSE, 0, envp, lpCurrentDirectory,
                           &si, &pi)) {
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
    std::vector<char *> new_env_ptrs;
    for (size_t i = 0; i < new_env_block.size(); i += strlen(&new_env_block[i]) + 1) {
      new_env_ptrs.push_back(&new_env_block[i]);
    }
    new_env_ptrs.push_back(static_cast<char *>(NULL));
    char **envp = &new_env_ptrs[0];

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
      // Change cwd for child process.
      int r = chdir(cwd.c_str());
      if (r != 0) {
        ec = std::error_code(errno, std::system_category());
      }
      if (write(pipefds[1], &my_pid, sizeof(my_pid)) == sizeof(my_pid)) {
        execvpe(argv[0], const_cast<char *const *>(argv),
                const_cast<char *const *>(envp));
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
    // Use pipe to track process lifetime. (The pipe closes when process terminates.)
    fd = pipefds[0];
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

Process::Process(const char *argv[], void *io_service, std::error_code &ec, bool decouple,
                 const ProcessEnvironment &env, const std::string &cwd) {
  (void)io_service;
  ProcessFD procfd = ProcessFD::spawnvpe(argv, ec, decouple, env, cwd);
  if (!ec) {
    p_ = std::make_shared<ProcessFD>(std::move(procfd));
  }
}

std::error_code Process::Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), NULL, ec, true, env);
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
                                                   const std::string &pid_file,
                                                   const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code error;
  Process proc(&*argv.begin(), NULL, error, decouple, env);
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
      // There are 3 possible cases:
      // - The process is a child whose death we await via waitpid().
      //   This is the usual case, when we have a child whose SIGCHLD we handle.
      // - The process shares a pipe with us whose closure we use to detect its death.
      //   This is used to track a non-owned process, like a grandchild.
      // - The process has no relationship with us, in which case we simply fail,
      //   since we have no need for this (and there's no good way to do it).
      // Why don't we just poll the PID? Because it's better not to:
      // - It would be prone to a race condition (we won't know when the PID is recycled).
      // - It would incur high latency and/or high CPU usage for the caller.
      if (fd != -1) {
        // We have a pipe, so wait for its other end to close, to detect process death.
        unsigned char buf[1 << 8];
        ptrdiff_t r;
        while ((r = read(fd, buf, sizeof(buf))) > 0) {
          // Keep reading until socket terminates
        }
        status = r == -1 ? -1 : 0;
      } else if (waitpid(pid, &status, 0) == -1) {
        // Just the normal waitpid() case.
        // (We can only do this once, only if we own the process. It fails otherwise.)
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
        if (error.value() == ERROR_ACCESS_DENIED) {
          // This can occur in some situations if the process is already terminating.
          DWORD exit_code;
          if (GetExitCodeProcess(handle, &exit_code) && exit_code != STILL_ACTIVE) {
            // The process is already terminating, so consider the killing successful.
            error = std::error_code();
          }
        }
      }
#else
      pollfd pfd = {static_cast<int>(fd), POLLHUP};
      if (fd != -1 && poll(&pfd, 1, 0) == 1 && (pfd.revents & POLLHUP)) {
        // The process has already died; don't attempt to kill its PID again.
      } else if (kill(pid, SIGKILL) != 0) {
        error = std::error_code(errno, std::system_category());
      }
      if (error.value() == ESRCH) {
        // The process died before our kill().
        // This is probably due to using FromPid().Kill() on a non-owned process.
        // We got lucky here, because we could've killed a recycled PID.
        // To avoid this, do not kill a process that is not owned by us.
        // Instead, let its parent receive its SIGCHLD normally and call waitpid() on it.
        // (Exception: Tests might occasionally trigger this, but that should be benign.)
      }
#endif
      if (error) {
        RAY_LOG(DEBUG) << "Failed to kill process " << pid << " with error " << error
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

/*
BSD 3-Clause License

Copyright (c) 2009, Jay Loden, Dave Daeschler, Giampaolo Rodola'
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

 * Neither the name of the psutil authors nor the names of its contributors
   may be used to endorse or promote products derived from this software without
   specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from https://github.com/giampaolo/psutil
*/

#ifdef _WIN32
DWORD *_get_pids(DWORD *numberOfReturnedPIDs) {
  // Win32 SDK says the only way to know if our process array
  // wasn't large enough is to check the returned size and make
  // sure that it doesn't match the size of the array.
  // If it does we allocate a larger array and try again

  // Stores the actual array
  DWORD *procArray = NULL;
  DWORD procArrayByteSz;
  int procArraySz = 0;

  // Stores the byte size of the returned array from enumprocesses
  DWORD enumReturnSz = 0;

  do {
    procArraySz += 1024;
    if (procArray != NULL) free(procArray);
    procArrayByteSz = procArraySz * sizeof(DWORD);
    procArray = malloc(procArrayByteSz);
    if (procArray == NULL) {
      return NULL;
    }
    if (!EnumProcesses(procArray, procArrayByteSz, &enumReturnSz)) {
      free(procArray);
      return NULL;
    }
  } while (enumReturnSz == procArraySz * sizeof(DWORD));

  // The number of elements is the returned size / size of each element
  *numberOfReturnedPIDs = enumReturnSz / sizeof(DWORD);

  return procArray;
}

// Return 1 if PID exists, 0 if not, -1 on error.
int _pid_in_pids(DWORD pid) {
  DWORD *proclist = NULL;
  DWORD numberOfReturnedPIDs;
  DWORD i;

  proclist = _get_pids(&numberOfReturnedPIDs);
  if (proclist == NULL) {
    return -1;
  }
  for (i = 0; i < numberOfReturnedPIDs; i++) {
    if (proclist[i] == pid) {
      free(proclist);
      return 1;
    }
  }
  free(proclist);
  return 0;
}

// Given a process handle checks whether it's actually running. If it
// does return the handle, else return NULL with Python exception set.
// This is needed because OpenProcess API sucks.
HANDLE
_check_phandle(HANDLE hProcess, DWORD pid) {
  DWORD exitCode;

  if (hProcess == NULL) {
    return NULL;
  }

  if (check_exit_code == 0) return hProcess;

  if (GetExitCodeProcess(hProcess, &exitCode)) {
    // XXX - maybe STILL_ACTIVE is not fully reliable as per:
    // http://stackoverflow.com/questions/1591342/#comment47830782_1591379
    if (exitCode == STILL_ACTIVE) {
      return hProcess;
    }
    if (_pid_in_pids(pid) == 1) {
      return hProcess;
    }
    CloseHandle(hProcess);
    return NULL;
  }

  if (GetLastError() == ERROR_ACCESS_DENIED) {
    return hProcess;
  }
  CloseHandle(hProcess);
  return NULL;
}
#endif

/*
 * Check if PID exists.
 */
bool Process::IsAlive(pid_t pid) {
#ifdef _WIN32
  HANDLE hProcess;

  // Special case for PID 0 System Idle Process
  if (pid == 0) return true;
  if (pid < 0) return false;

  hProcess =
      OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, static_cast<DWORD>(pid));

  // Access denied means there's a process to deny access to.
  if ((hProcess == NULL) && (GetLastError() == ERROR_ACCESS_DENIED)) return true;

  hProcess = _check_phandle(hProcess, pid);
  if (hProcess != NULL) {
    CloseHandle(hProcess);
    return true;
  }

  CloseHandle(hProcess);
  return false;
#else
  int ret;

  // No negative PID exists, plus -1 is an alias for sending signal
  // too all processes except system ones. Not what we want.
  if (pid < 0) return false;

  // As per "man 2 kill" PID 0 is an alias for sending the signal to
  // every process in the process group of the calling process.
  // Not what we want. Some platforms have PID 0, some do not.
  // We decide that at runtime.
  if (pid == 0) {
    return false;
  }

  ret = kill(pid, 0);
  if (ret == 0)
    return true;
  else {
    if (errno == ESRCH) {
      // ESRCH == No such process
      return false;
    } else if (errno == EPERM) {
      // EPERM clearly indicates there's a process to deny
      // access to.
      return true;
    } else {
      // According to "man 2 kill" possible error values are
      // (EINVAL, EPERM, ESRCH) therefore we should never get
      // here. If we do let's be explicit in considering this
      // an error.
      return false;
    }
  }
#endif
}

#ifdef _WIN32
#ifndef STATUS_BUFFER_OVERFLOW
#define STATUS_BUFFER_OVERFLOW ((NTSTATUS)0x80000005L)
#endif
typedef LONG NTSTATUS;
typedef NTSTATUS WINAPI NtQueryInformationProcess_t(HANDLE ProcessHandle,
                                                    ULONG ProcessInformationClass,
                                                    PVOID ProcessInformation,
                                                    ULONG ProcessInformationLength,
                                                    ULONG *ReturnLength);

static std::atomic<NtQueryInformationProcess_t *> NtQueryInformationProcess_ =
    ATOMIC_VAR_INIT(NULL);

pid_t GetParentPID() {
  NtQueryInformationProcess_t *NtQueryInformationProcess = NtQueryInformationProcess_;
  if (!NtQueryInformationProcess) {
    NtQueryInformationProcess = reinterpret_cast<NtQueryInformationProcess_t *>(
        GetProcAddress(GetModuleHandle(TEXT("ntdll.dll")),
                       _CRT_STRINGIZE(NtQueryInformationProcess)));
    NtQueryInformationProcess_ = NtQueryInformationProcess;
  }
  DWORD ppid = 0;
  PROCESS_BASIC_INFORMATION info;
  ULONG cb = sizeof(info);
  NTSTATUS status = NtQueryInformationProcess(GetCurrentProcess(), 0, &info, cb, &cb);
  if ((status >= 0 || status == STATUS_BUFFER_OVERFLOW) && cb >= sizeof(info)) {
    ppid = static_cast<DWORD>(reinterpret_cast<uintptr_t>(info.Reserved3));
  }
  pid_t result = 0;
  if (ppid > 0) {
    // For now, assume PPID = 1 (simulating the reassignment to "init" on Linux)
    result = 1;
    if (HANDLE parent = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, ppid)) {
      long long me_created, parent_created;
      FILETIME unused;
      if (GetProcessTimes(GetCurrentProcess(), reinterpret_cast<FILETIME *>(&me_created),
                          &unused, &unused, &unused) &&
          GetProcessTimes(parent, reinterpret_cast<FILETIME *>(&parent_created), &unused,
                          &unused, &unused)) {
        if (me_created >= parent_created) {
          // We verified the child is younger than the parent, so we know the parent
          // is still alive.
          // (Note that the parent can still die by the time this function returns,
          // but that race condition exists on POSIX too, which we're emulating here.)
          result = static_cast<pid_t>(ppid);
        }
      }
      CloseHandle(parent);
    }
  }
  return result;
}
#else
pid_t GetParentPID() { return getppid(); }
#endif  // #ifdef _WIN32

bool IsParentProcessAlive() { return GetParentPID() != 1; }

}  // namespace ray

namespace std {

bool equal_to<ray::Process>::operator()(const ray::Process &x,
                                        const ray::Process &y) const {
  using namespace ray;
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
  using namespace ray;
  return !value.IsNull() ? value.IsValid() ? hash<pid_t>()(value.GetId())
                                           : hash<void const *>()(value.Get())
                         : size_t();
}

}  // namespace std
