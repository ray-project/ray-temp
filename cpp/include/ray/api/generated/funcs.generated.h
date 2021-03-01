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
using Func0 = ReturnType (*)();

// 1 arg
template <typename ReturnType, typename Arg1Type>
using Func1 = ReturnType (*)(Arg1Type);

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
using Func2 = ReturnType (*)(Arg1Type, Arg2Type);
