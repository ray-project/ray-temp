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

#include <ray/api/ray_exception.h>

#include <msgpack.hpp>

namespace ray {
namespace api {

class Serializer {
 public:
  template <typename T>
  static msgpack::sbuffer Serialize(const T &t) {
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, t);
    return buffer;
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size) {
    try {
      msgpack::unpacked unpacked;
      msgpack::unpack(unpacked, data, size);
      return unpacked.get().as<T>();
    } catch (std::exception &e) {
      throw RayException(std::string("unpack failed, reason: ") + e.what());
    } catch (...) {
      throw RayException("unpack failed, reason: unknown error");
    }
  }
};

}  // namespace api
}  // namespace ray