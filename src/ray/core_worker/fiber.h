#ifndef RAY_CORE_WORKER_FIBER_H
#define RAY_CORE_WORKER_FIBER_H

#include <ray/util/logging.h>
#include <boost/fiber/all.hpp>
namespace ray {

/// Used by async actor mode. The fiber event will be used
/// from python to switch control among different coroutines.
/// Taken from boost::fiber examples
/// https://github.com/boostorg/fiber/blob/7be4f860e733a92d2fa80a848dd110df009a20e1/examples/wait_stuff.cpp#L115-L142
class FiberEvent {
 public:
  // Block the fiber until the event is notified.
  void Wait();

  // Notify the event and unblock all waiters.
  void Notify();

 private:
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  bool ready_ = false;
};

/// Used by async actor mode. The FiberRateLimiter is a barrier that
/// allows at most num fibers running at once. It implements the
/// semaphore data structure.
class FiberRateLimiter {
 public:
  FiberRateLimiter(int num);

  // Enter the semaphore. Wait for the value to be > 0 and decrement the value.
  void Acquire();

  // Exit the semaphore. Increment the value and notify other waiter.
  void Release();

 private:
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  int num_ = 1;
};

using FiberChannel = boost::fibers::unbuffered_channel<std::function<void()>>;

class FiberState {
 public:
  FiberState(int max_concurrency);

  void EnqueueFiber(std::function<void()> &&callback) {
    auto op_status = channel_.push([this, callback]() {
      rate_limiter_.Acquire();
      callback();
      rate_limiter_.Release();
    });
    RAY_CHECK(op_status == boost::fibers::channel_op_status::success);
  }

  ~FiberState();

 private:
  /// The fiber channel used to send task between the submitter thread
  /// (main direct_actor_trasnport thread) and the fiber_worker_thread_ (defined below)
  FiberChannel channel_;
  /// The fiber semaphore used to limit the number of concurrent fibers
  /// running at once.
  FiberRateLimiter rate_limiter_;
  /// The fiber event used to block fiber_runner_thread_ from shutdown.
  /// is_asyncio_ must be true.
  FiberEvent shutdown_worker_event_;
  /// The thread that runs all asyncio fibers. is_asyncio_ must be true.
  std::thread fiber_runner_thread_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_FIBER_H