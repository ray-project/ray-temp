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

#ifndef RAY_GCS_REDIS_FAILURE_DETECTOR_H
#define RAY_GCS_REDIS_FAILURE_DETECTOR_H

#include <boost/asio.hpp>
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {
class RedisGcsClient;

/// GcsRedisFailureDetector is responsible for monitoring redis.
class GcsRedisFailureDetector {
 public:
  /// Create a GcsRedisFailureDetector.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param redis_context The redis context is used to ping redis.
  /// \param callback Callback that will be called when redis is detected as not alive.
  explicit GcsRedisFailureDetector(boost::asio::io_service &io_service,
                                   std::shared_ptr<RedisContext> redis_context,
                                   std::function<void()> callback);

  /// Start detecting redis.
  void Start();

 protected:
  /// A periodic timer that fires on every gcs detect period.
  void Tick();

  /// Schedule another tick after a short time.
  void ScheduleTick();

  /// Check that if redis is inactive.
  void DetectRedis();

 private:
  /// A redis context is used to ping redis.
  /// TODO(ffbin): We will use redis client later.
  std::shared_ptr<RedisContext> redis_context_;

  /// A timer that ticks every gcs_detect_timeout_milliseconds.
  boost::asio::deadline_timer detect_timer_;

  /// A function is called when redis is detected to be unavailable.
  std::function<void()> callback_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_REDIS_FAILURE_DETECTOR_H
