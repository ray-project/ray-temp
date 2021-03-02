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

#include <gtest/gtest.h>
#include <ray/api.h>

#include <future>
#include <thread>

using namespace ray::api;

int Return1() { return 1; }
int Plus1(int x) { return x + 1; }

RAY_REGISTER(Plus1);

TEST(RayApiTest, RayRegister) {
  using namespace ray::api::internal;

  bool r = Router::Instance().RegisterRemoteFunction("Return1", Return1);
  EXPECT_TRUE(r);

  /// Duplicate register
  bool r1 = Router::Instance().RegisterRemoteFunction("Return1", Return1);
  EXPECT_TRUE(!r1);

  bool r2 = Router::Instance().RegisterRemoteFunction("Plus1", Plus1);
  EXPECT_TRUE(!r2);

  /// Find and call the registered function.
  auto args = std::make_tuple("Plus1", 1);
  auto buf = Serializer::Serialize(args);
  auto result_buf = Router::Instance().Route(buf.data(), buf.size());

  /// Deserialize result.
  auto response =
      Serializer::Deserialize<Response<int>>(result_buf.data(), result_buf.size());

  EXPECT_EQ(response.error_code, ErrorCode::OK);
  EXPECT_EQ(response.data, 2);

  /// Void function.
  auto buf1 = Serializer::Serialize(std::make_tuple("Return1"));
  auto result_buf1 = Router::Instance().Route(buf1.data(), buf1.size());
  auto response1 =
      Serializer::Deserialize<VoidResponse>(result_buf.data(), result_buf.size());
  EXPECT_EQ(response1.error_code, ErrorCode::OK);

  /// We should consider the driver so is not same with the worker so, and find the error
  /// reason.

  /// Not exist function.
  auto buf2 = Serializer::Serialize(std::make_tuple("Return11"));
  auto result_buf2 = Router::Instance().Route(buf2.data(), buf2.size());
  auto response2 =
      Serializer::Deserialize<VoidResponse>(result_buf2.data(), result_buf2.size());
  EXPECT_EQ(response2.error_code, ErrorCode::FAIL);
  EXPECT_FALSE(response2.error_msg.empty());

  /// Arguments not match.
  auto buf3 = Serializer::Serialize(std::make_tuple("Plus1", "invalid arguments"));
  auto result_buf3 = Router::Instance().Route(buf3.data(), buf3.size());
  auto response3 =
      Serializer::Deserialize<Response<int>>(result_buf3.data(), result_buf3.size());
  EXPECT_EQ(response3.error_code, ErrorCode::FAIL);
  EXPECT_FALSE(response3.error_msg.empty());
}
