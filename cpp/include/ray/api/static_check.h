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

#include <ray/api/object_ref.h>

#include <boost/callable_traits.hpp>
#include <type_traits>

namespace ray {
namespace api {

template <typename T>
struct FilterArgType {
  using type = T;
};

template <typename T>
struct FilterArgType<ObjectRef<T>> {
  using type = T;
};

template <typename Function, typename... Args>
inline absl::enable_if_t<!std::is_member_function_pointer<Function>::value>
StaticCheck() {
  static_assert(std::is_same<std::tuple<typename FilterArgType<Args>::type...>,
                             boost::callable_traits::args_t<Function>>::value,
                "arguments not match");
}

template <typename Function, typename... Args>
inline absl::enable_if_t<std::is_member_function_pointer<Function>::value> StaticCheck() {
  using ActorType = boost::callable_traits::class_of_t<Function>;
  static_assert(
      std::is_same<std::tuple<ActorType &, typename FilterArgType<Args>::type...>,
                   boost::callable_traits::args_t<Function>>::value,
      "arguments not match");
}

}  // namespace api
}  // namespace ray
