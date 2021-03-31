
#pragma once

#include <ray/api/arguments.h>
#include <ray/api/exec_funcs.h>
#include <ray/api/object_ref.h>
#include "ray/core.h"

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

template <typename F>
class ActorTaskCaller {
 public:
  ActorTaskCaller() = default;

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
                  std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
      : runtime_(runtime), id_(id), ptr_(ptr), args_(std::move(args)) {}

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr)
      : runtime_(runtime), id_(id), ptr_(ptr) {}

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args... args) {
    using ActorType = boost::callable_traits::class_of_t<F>;
    using ReturnType = boost::callable_traits::return_type_t<F>;
    auto exe_func =
        ActorExecFunction<ReturnType, ActorType, typename FilterArgType<Args>::type...>;
    ptr_.exec_function_pointer = reinterpret_cast<uintptr_t>(exe_func);
    Arguments::WrapArgs(&args_, args...);
    auto returned_object_id = runtime_->CallActor(ptr_, id_, args_);
    return ObjectRef<ReturnType>(returned_object_id);
  }

 private:
  RayRuntime *runtime_;
  ActorID id_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

}  // namespace api
}  // namespace ray
