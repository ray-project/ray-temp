#include "ray/gcs/redis_async_context.h"

extern "C" {
#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"
}

namespace ray {

namespace gcs {

Status RedisAsyncContext::RedisAsyncCommand(redisCallbackFn *fn, void *privdata,
                                            const char *format, ...) {
  va_list ap;
  va_start(ap, format);

  int ret_code = 0;
  {
    // Lock the obuf of redisContext for writing.
    std::lock_guard<std::mutex> lock(mutex_);
    ret_code = redisvAsyncCommand(redis_async_context_, fn, privdata, format, ap);
  }

  va_end(ap);

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(std::string(redis_async_context_->errstr));
  }
  RAY_CHECK(ret_code == REDIS_OK);
  return Status::OK();
}

Status RedisAsyncContext::RedisAsyncCommandArgv(redisCallbackFn *fn, void *privdata,
                                                int argc, const char **argv,
                                                const size_t *argvlen) {
  int ret_code = 0;
  {
    // Lock the obuf of redisContext for writing.
    std::lock_guard<std::mutex> lock(mutex_);
    ret_code =
        redisAsyncCommandArgv(redis_async_context_, fn, privdata, argc, argv, argvlen);
  }

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(std::string(redis_async_context_->errstr));
  }
  RAY_CHECK(ret_code == REDIS_OK);
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
