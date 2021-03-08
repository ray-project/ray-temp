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
template <typename ActorType>
ActorCreator<ActorType> Ray::Actor(CreateActorFunc0<ActorType> create_func) {
  return CreateActorInternal<ActorType>(create_func,
                                        CreateActorExecFunction<ActorType *>);
}

// 1 arg
template <typename ActorType, typename Arg1Type>
ActorCreator<ActorType> Ray::Actor(CreateActorFunc1<ActorType, Arg1Type> create_func,
                                   Arg1Type arg1) {
  return CreateActorInternal<ActorType>(
      create_func, CreateActorExecFunction<ActorType *, Arg1Type>, arg1);
}

template <typename ActorType, typename Arg1Type>
ActorCreator<ActorType> Ray::Actor(CreateActorFunc1<ActorType, Arg1Type> create_func,
                                   ObjectRef<Arg1Type> &arg1) {
  return CreateActorInternal<ActorType>(
      create_func, CreateActorExecFunction<ActorType *, Arg1Type>, arg1);
}

// 2 args
template <typename ActorType, typename Arg1Type, typename Arg2Type>
ActorCreator<ActorType> Ray::Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    Arg2Type arg2) {
  return CreateActorInternal<ActorType>(
      create_func, CreateActorExecFunction<ActorType *, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ActorType, typename Arg1Type, typename Arg2Type>
ActorCreator<ActorType> Ray::Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func,
    ObjectRef<Arg1Type> &arg1, Arg2Type arg2) {
  return CreateActorInternal<ActorType>(
      create_func, CreateActorExecFunction<ActorType *, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ActorType, typename Arg1Type, typename Arg2Type>
ActorCreator<ActorType> Ray::Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    ObjectRef<Arg2Type> &arg2) {
  return CreateActorInternal<ActorType>(
      create_func, CreateActorExecFunction<ActorType *, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ActorType, typename Arg1Type, typename Arg2Type>
ActorCreator<ActorType> Ray::Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func,
    ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2) {
  return CreateActorInternal<ActorType>(
      create_func, CreateActorExecFunction<ActorType *, Arg1Type, Arg2Type>, arg1, arg2);
}