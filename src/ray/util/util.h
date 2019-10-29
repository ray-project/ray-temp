#ifndef RAY_UTIL_UTIL_H
#define RAY_UTIL_UTIL_H

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <deque>
#include <iterator>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/status.h"

/// Return the number of milliseconds since the steady clock epoch. NOTE: The
/// returned timestamp may be used for accurately measuring intervals but has
/// no relation to wall clock time. It must not be used for synchronization
/// across multiple nodes.
///
/// TODO(rkn): This function appears in multiple places. It should be
/// deduplicated.
///
/// \return The number of milliseconds since the steady clock epoch.
inline int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline ray::Status boost_to_ray_status(const boost::system::error_code &error) {
  switch (error.value()) {
  case boost::system::errc::success:
    return ray::Status::OK();
  default:
    return ray::Status::IOError(strerror(error.value()));
  }
}

/// A helper function to split a string by whitespaces.
///
/// \param str The string with whitespaces.
///
/// \return A vector that contains strings split by whitespaces.
inline std::vector<std::string> SplitStrByWhitespaces(const std::string &str) {
  std::istringstream iss(str);
  std::vector<std::string> result(std::istream_iterator<std::string>{iss},
                                  std::istream_iterator<std::string>());
  return result;
}

class InitShutdownRAII {
 public:
  /// Type of the Shutdown function.
  using ShutdownFunc = void (*)();

  /// Create an instance of InitShutdownRAII which will call shutdown
  /// function when it is out of scope.
  ///
  /// \param init_func The init function.
  /// \param shutdown_func The shutdown function.
  /// \param args The arguments for the init function.
  template <class InitFunc, class... Args>
  InitShutdownRAII(InitFunc init_func, ShutdownFunc shutdown_func, Args &&... args)
      : shutdown_(shutdown_func) {
    init_func(args...);
  }

  /// Destructor of InitShutdownRAII which will call the shutdown function.
  ~InitShutdownRAII() {
    if (shutdown_ != nullptr) {
      shutdown_();
    }
  }

 private:
  ShutdownFunc shutdown_;
};

struct EnumClassHash {
  template <typename T>
  std::size_t operator()(T t) const {
    return static_cast<std::size_t>(t);
  }
};

/// unordered_map for enum class type.
template <typename Key, typename T>
using EnumUnorderedMap = std::unordered_map<Key, T, EnumClassHash>;

/// A helper function to fill random bytes into the `data`.
/// Warning: this is not fork-safe, we need to re-seed after that.
template <typename T>
void FillRandom(T *data) {
  RAY_CHECK(data != nullptr);
  auto randomly_seeded_mersenne_twister = []() {
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 seeded_engine(seed);
    return seeded_engine;
  };

  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = randomly_seeded_mersenne_twister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(dist(generator));
  }
}

namespace ray {

class EventCombiner {
 public:
  EventCombiner(boost::asio::thread_pool& pool) : executor_(pool.get_executor()) {};

  void post(std::function<void()> fn) {
    absl::MutexLock lock(&mu_);
    pending_.push_back(fn);
    TriggerBatchPost();
  }

 private:
  void TriggerBatchPost() EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (post_active_) {
      return;
    }
    post_active_ = true;
    boost::asio::post(executor_, [this]() {
      std::vector<std::function<void()>> to_post;
      while (true) {
        {
          absl::MutexLock lock(&mu_);
          while (!pending_.empty()) {
            to_post.push_back(pending_.front());
            pending_.pop_front();
          }
          if (to_post.empty()) {
            post_active_ = false;
            break;
          }
        }
        boost::asio::post(executor_, [this, to_post]() {
          for (auto& fn : to_post) {
            fn();
          }
        });
        to_post.clear();
      }
    });
  }

  boost::asio::executor executor_;
  absl::Mutex mu_;
  std::deque<std::function<void()>> pending_ GUARDED_BY(mu_);
  bool post_active_ GUARDED_BY(mu_) = false;
};
}  // namespace ray

#endif  // RAY_UTIL_UTIL_H
