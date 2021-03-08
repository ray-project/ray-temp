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

// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
TaskCaller<ReturnType> Ray::Task(Func0<ReturnType> func) {
  return TaskInternal<ReturnType>(func, NormalExecFunction<ReturnType>);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
TaskCaller<ReturnType> Ray::Task(Func1<ReturnType, Arg1Type> func, Arg1Type arg1) {
  return TaskInternal<ReturnType>(func, NormalExecFunction<ReturnType, Arg1Type>, arg1);
}

template <typename ReturnType, typename Arg1Type>
TaskCaller<ReturnType> Ray::Task(Func1<ReturnType, Arg1Type> func,
                                 ObjectRef<Arg1Type> &arg1) {
  return TaskInternal<ReturnType>(func, NormalExecFunction<ReturnType, Arg1Type>, arg1);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 Arg1Type arg1, Arg2Type arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 ObjectRef<Arg1Type> &arg1, Arg2Type arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 Arg1Type arg1, ObjectRef<Arg2Type> &arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}