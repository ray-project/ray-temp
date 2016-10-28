#include "object_table.h"
#include "redis.h"

void object_table_lookup(db_handle *db_handle,
                         object_id object_id,
                         retry_info *retry,
                         object_table_lookup_done_cb done_cb,
                         void *user_context) {
  init_table_callback(db_handle, object_id, NULL, retry, done_cb,
                      redis_object_table_lookup, user_context);
}

void object_table_add(db_handle *db_handle,
                      object_id object_id,
                      retry_info *retry,
                      object_table_done_cb done_cb,
                      void *user_context) {
  init_table_callback(db_handle, object_id, NULL, retry, done_cb,
                      redis_object_table_add, user_context);
}

void object_table_subscribe(
    db_handle *db_handle,
    object_id object_id,
    object_table_object_available_cb object_available_cb,
    void *subscribe_context,
    retry_info *retry,
    object_table_done_cb done_cb,
    void *user_context) {
  object_table_subscribe_data *sub_data =
      malloc(sizeof(object_table_subscribe_data));
  utarray_push_back(db_handle->callback_freelist, &sub_data);
  sub_data->object_available_cb = object_available_cb;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, object_id, sub_data, retry, done_cb,
                      redis_object_table_subscribe, user_context);
}
