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

#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

class GcsPubSubTest : public RedisServiceManagerForTest {
 protected:
  virtual void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    gcs::RedisClientOptions redis_client_options("127.0.0.1", REDIS_SERVER_PORT, "",
                                                 true);
    client_ = std::make_shared<gcs::RedisClient>(redis_client_options);
    RAY_CHECK_OK(client_->Connect(io_service_));
    pub_sub_ = std::make_shared<gcs::GcsPubSub>(client_);
  }

  virtual void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  void Subscribe(const std::string &channel, const std::string &id,
                 std::vector<std::string> &result) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    auto subscribe = [&result](const std::string &id, const std::string &data) {
      result.push_back(data);
    };
    RAY_CHECK_OK((pub_sub_->Subscribe(channel, id, subscribe, done)));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  void SubscribeAll(const std::string &channel,
                    std::vector<std::pair<std::string, std::string>> &result) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    auto subscribe = [&result](const std::string &id, const std::string &data) {
      result.push_back(std::make_pair(id, data));
    };
    RAY_CHECK_OK((pub_sub_->SubscribeAll(channel, subscribe, done)));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  bool Unsubscribe(const std::string &channel, const std::string &id) {
    return pub_sub_->Unsubscribe(channel, id).ok();
  }

  bool Publish(const std::string &channel, const std::string &id,
               const std::string &data) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    RAY_CHECK_OK((pub_sub_->Publish(channel, id, data, done)));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  template <typename Data>
  void WaitPendingDone(const std::vector<Data> &data, int expected_count) {
    auto condition = [&data, expected_count]() {
      return (int)data.size() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  std::shared_ptr<gcs::RedisClient> client_;
  const std::chrono::milliseconds timeout_ms_{60000};
  std::shared_ptr<gcs::GcsPubSub> pub_sub_;

 private:
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

TEST_F(GcsPubSubTest, TestPubSubApi) {
  std::string channel("channel");
  std::string id("id");
  std::string data("data");
  std::vector<std::pair<std::string, std::string>> all_result;

  SubscribeAll(channel, all_result);
  std::vector<std::string> result;
  Subscribe(channel, id, result);
  Publish(channel, id, data);

  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  Unsubscribe(channel, id);
  Publish(channel, id, data);
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);

  Subscribe(channel, id, result);
  Publish(channel, id, data);
  WaitPendingDone(result, 2);
  WaitPendingDone(all_result, 3);
}

TEST_F(GcsPubSubTest, TestMultithreading) {
  std::string channel("channel");
  auto count = std::make_shared<std::atomic<int>>(0);
  int size = 5;
  for (int index = 0; index < size; ++index) {
    std::stringstream ss;
    ss << index;
    auto id = ss.str();
    new std::thread([this, count, id, channel] {
      auto subscribe = [count](const std::string &id, const std::string &data) {
        ++(*count);
      };
      RAY_CHECK_OK(pub_sub_->Subscribe(channel, id, subscribe, nullptr));
    });
  }

  std::string data("data");
  for (int index = 0; index < size; ++index) {
    std::stringstream ss;
    ss << index;
    auto id = ss.str();
    new std::thread([this, channel, id, data] {
      RAY_CHECK_OK(pub_sub_->Publish(channel, id, data, nullptr));
    });
  }

  auto condition = [count, size]() { return count->load() == size; };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
