#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include "hiredis/hiredis.h"

#include "task.h"

task_spec *example_task(void) {
  task_id parent_task_id = globally_unique_id();
  function_id func_id = globally_unique_id();
  task_spec *task = start_construct_task_spec(parent_task_id, 0, func_id, 2, 1,
                                              0);
  task_args_add_ref(task, globally_unique_id());
  task_args_add_ref(task, globally_unique_id());
  return task;
}

task_instance *example_task_instance(void) {
  task_iid iid = globally_unique_id();
  task_spec *spec = example_task();
  task_instance *instance =
      make_task_instance(iid, spec, TASK_STATUS_WAITING, NIL_ID);
  free_task_spec(spec);
  return instance;
}

/* Flush redis. */
void flushall_redis() {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

/* Cleanup method for running tests with the greatest library.
 * Runs the test, then clears the Redis database. */
#define RUN_REDIS_TEST(test) \
  flushall_redis();          \
  RUN_TEST(test);            \
  flushall_redis();

#endif /* TEST_COMMON */
