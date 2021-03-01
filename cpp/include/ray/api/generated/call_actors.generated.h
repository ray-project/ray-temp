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
template <typename ReturnType, typename ActorType>
static ActorTaskCaller<ReturnType> Task(ActorFunc0<ActorType, ReturnType> actor_func,
                                        ActorHandle<ActorType> &actor);

// 1 arg
template <typename ReturnType, typename ActorType, typename Arg1Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ActorHandle<ActorType> &actor,
    Arg1Type arg1);

template <typename ReturnType, typename ActorType, typename Arg1Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ActorHandle<ActorType> &actor,
    ObjectRef<Arg1Type> &arg1);

// 2 args
template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, Arg1Type arg1, Arg2Type arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, ObjectRef<Arg1Type> &arg1, Arg2Type arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, Arg1Type arg1, ObjectRef<Arg2Type> &arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2);
