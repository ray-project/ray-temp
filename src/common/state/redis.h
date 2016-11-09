#ifndef REDIS_H
#define REDIS_H

#include "db.h"
#include "object_table.h"
#include "task_table.h"

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "uthash.h"
#include "utarray.h"

typedef struct {
  /** Unique ID for this service. */
  client_id service_id;
  /** IP address and port of this service. */
  char *addr;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} service_cache_entry;

struct db_handle {
  /** String that identifies this client type. */
  char *client_type;
  /** Unique ID for this client within the type. */
  client_id client;
  /** Redis context for this global state store connection. */
  redisAsyncContext *context;
  /** Redis context for "subscribe" communication. Yes, we need a separate one
   *  for that, see https://github.com/redis/hiredis/issues/55. */
  redisAsyncContext *sub_context;
  /** The event loop this global state store connection is part of. */
  event_loop *loop;
  /** Index of the database connection in the event loop */
  int64_t db_index;
  /** Cache for the IP addresses of services. This is a hash table mapping
   *  client IDs to addresses. */
  service_cache_entry *service_cache;
  /** Redis context for synchronous connections. This should only be used very
   *  rarely, it is not asynchronous. */
  redisContext *sync_context;
  /** Data structure for callbacks that needs to be freed. */
  UT_array *callback_freelist;
};

void redis_object_table_get_entry(redisAsyncContext *c,
                                  void *r,
                                  void *privdata);

void object_table_lookup_callback(redisAsyncContext *c,
                                  void *r,
                                  void *privdata);

/*
 * ==== Redis object table functions ====
 */

/**
 * Lookup object table entry in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_lookup(table_callback_data *callback_data);

/**
 * Add a location entry to the object table in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_add(table_callback_data *callback_data);

/**
 * Subscribe to learn when a new object becomes available.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_object_table_subscribe(table_callback_data *callback_data);

/**
 * Add a new object to the object table in redis.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_result_table_add(table_callback_data *callback_data);

/**
 * Lookup the object in the object table in redis. The entry in
 * the object table contains metadata about the object.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_result_table_lookup(table_callback_data *callback_data);

/*
 * ==== Redis task table function =====
 */

/**
 * Get a task table entry, including the task spec and the task's scheduling
 * information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_get_task(table_callback_data *callback_data);

/**
 * Add a task table entry with a new task spec and the task's scheduling
 * information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_add_task(table_callback_data *callback_data);

/**
 * Update a task table entry with the task's scheduling information.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_update(table_callback_data *callback_data);

/**
 * Callback invoked when the reply from the task push command is received.
 *
 * @param c Redis context.
 * @param r Reply (not used).
 * @param privdata Data associated to the callback.
 * @return Void.
 */

void redis_task_table_publish_push_callback(redisAsyncContext *c,
                                            void *r,
                                            void *privdata);

/**
 * Callback invoked when the reply from the task publish command is received.
 *
 * @param c Redis context.
 * @param r Reply (not used).
 * @param privdata Data associated to the callback.
 * @return Void.
 */
void redis_task_table_publish_publish_callback(redisAsyncContext *c,
                                               void *r,
                                               void *privdata);

/**
 * Subscribe to updates of the task table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_task_table_subscribe(table_callback_data *callback_data);

/**
 * Subscribe to updates from the local scheduelr table.
 *
 * @param callback_data Data structure containing redis connection and timeout
 *        information.
 * @return Void.
 */
void redis_local_scheduler_table_subscribe(table_callback_data *callback_data);

#endif /* REDIS_H */
