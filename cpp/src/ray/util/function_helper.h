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

#pragma once

#include <boost/dll.hpp>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <unordered_map>
#include "ray/core.h"

namespace ray {
namespace api {

class FunctionHelper {
 public:
  uintptr_t GetBaseAddress(std::string lib_name);

  static FunctionHelper &GetInstance() {
    static FunctionHelper functionHelper;
    return functionHelper;
  }

  std::shared_ptr<boost::dll::shared_library> LoadDll(const std::string &lib_name);
  std::function<msgpack::sbuffer(const std::string &,
                                 const std::vector<std::shared_ptr<::ray::RayObject>> &)>
  GetExecuteFunction(const std::string &lib_name);

 private:
  FunctionHelper() = default;
  ~FunctionHelper() = default;
  FunctionHelper(FunctionHelper const &) = delete;
  FunctionHelper(FunctionHelper &&) = delete;

  uintptr_t LoadLibrary(std::string lib_name);

  std::unordered_map<std::string, uintptr_t> loaded_library_;
  std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>> libraries_;
  std::unordered_map<
      std::string,
      std::function<msgpack::sbuffer(
          const std::string &, const std::vector<std::shared_ptr<::ray::RayObject>> &)>>
      funcs_;
};
}  // namespace api
}  // namespace ray