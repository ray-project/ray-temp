#include "redismodule.h"

#include <string.h>
#include <stdlib.h>
#include "uthash.h"

#define OBJECT_INFO_PREFIX "OI:"
#define OBJECT_TABLE_PREFIX "OT:"
#define SUBSCRIPTION_TABLE_PREFIX "ST:"

#define CHECK_ERROR(STATUS, MESSAGE)                   \
  if ((STATUS) == REDISMODULE_ERR) {                   \
    return RedisModule_ReplyWithError(ctx, (MESSAGE)); \
  }


RedisModuleKey *OpenPrefixedKey(RedisModuleCtx *ctx,
                                const char *prefix,
                                RedisModuleString* keyname,
                                int mode) {
  size_t length;
  const char *value = RedisModule_StringPtrLen(keyname, &length);
  RedisModuleString *prefixed_keyname = RedisModule_CreateStringPrintf(ctx, "%s%*.*s", prefix, length, length, value);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, prefixed_keyname, mode);
  RedisModule_FreeString(ctx, prefixed_keyname);
  return key;
}

/* The object table has the following format:
 * "obj:(object id)" "hash" (hash of the object)
 * "obj:(object id)" "data_size" (size of the object)
 * "obj:(object id):set" (set of managers that have the object)
 */

int ObjectTableLookup_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argc);
  RedisModuleKey *key = OpenPrefixedKey(ctx, OBJECT_TABLE_PREFIX, argv[1], REDISMODULE_READ);

  CHECK_ERROR(RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE, REDISMODULE_POSITIVE_INFINITE, 1, 1),
              "Unable to initialize zset iterator");

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int num_results = 0;
  int has_next;
  do {
    RedisModuleString *curr = RedisModule_ZsetRangeCurrentElement(key, NULL);
    RedisModule_ReplyWithString(ctx, curr);
    has_next = RedisModule_ZsetRangeNext(key);
    num_results += 1;
  } while (has_next);
  RedisModule_ReplySetArrayLength(ctx, num_results);

  return REDISMODULE_OK;
}

/* This is called like
 * ray.object_table_add "obj:(object id)" (data_size) "hash" [manager list]
 */
int ObjectTableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argc);

  RedisModuleString *object_id = argv[1];
  RedisModuleString *data_size = argv[2];
  RedisModuleString *new_hash = argv[3];
  RedisModuleString *manager = argv[4];

  RedisModuleKey *key;
  key = OpenPrefixedKey(ctx, OBJECT_INFO_PREFIX, object_id, REDISMODULE_READ | REDISMODULE_WRITE);

  int keytype = RedisModule_KeyType(key);
  /* Check if this object was already registered and if the hashes agree. */
  if (keytype != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModuleString *existing_hash;
    RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, "hash", &existing_hash, NULL);
    if (RedisModule_StringCompare(existing_hash, new_hash) != 0) {
      RedisModule_CloseKey(key);
      return RedisModule_ReplyWithError(ctx, "object with this id already present with different hash");
    }
  }

  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "hash", new_hash, NULL);
  RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, "data_size", data_size, NULL);

  RedisModuleKey *table_key;
  table_key = OpenPrefixedKey(ctx, OBJECT_TABLE_PREFIX, object_id, REDISMODULE_READ | REDISMODULE_WRITE);

  /* Sets are not implemented yet, so we use ZSETs instead. */
  RedisModule_ZsetAdd(table_key, 0.0, manager, NULL);
  RedisModule_CloseKey(key);
  RedisModule_CloseKey(table_key);
  RedisModule_ReplyWithLongLong(ctx, RedisModule_GetSelectedDb(ctx));
  return REDISMODULE_OK;
}

int ObjectTableSubscribe_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

int ObjectInfoSubscribe_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

int ResultTableAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

int ResultTableLookup_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);

  if (RedisModule_Init(ctx, "ray", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_lookup", ObjectTableLookup_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /* TODO(pcm): What is "readonly" about? */
  if (RedisModule_CreateCommand(ctx, "ray.object_table_add", ObjectTableAdd_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_table_subscribe", ObjectTableSubscribe_RedisCommand, "pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.object_info_subscribe", ObjectInfoSubscribe_RedisCommand, "pubsub", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.result_table_add", ResultTableAdd_RedisCommand, "write", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "ray.result_table_lookup", ResultTableLookup_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
