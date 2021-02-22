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

#include "ray/util/periodical_runner.h"

#include "ray/util/logging.h"

namespace ray {

PeriodicalRunner::PeriodicalRunner(boost::asio::io_service &io_service)
    : io_service_(io_service) {}

PeriodicalRunner::~PeriodicalRunner() {}

void PeriodicalRunner::RunFnPeriodically(std::function<void()> fn,
                                         boost::posix_time::milliseconds period) {
  auto timer = std::unique_ptr<boost::asio::deadline_timer>(
      new boost::asio::deadline_timer(io_service_));
  timers_.push_back(std::move(timer));
  DoRunFnPeriodically(fn, period, *timer);
}

void PeriodicalRunner::DoRunFnPeriodically(std::function<void()> fn,
                                           boost::posix_time::milliseconds period,
                                           boost::asio::deadline_timer &timer) {
  fn();
  timer.expires_from_now(period);
  timer.async_wait([this, fn, period, &timer](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // `operation_aborted` is set when `timer` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << error.message();
    DoRunFnPeriodically(fn, period, timer);
  });
}

}  // namespace ray
