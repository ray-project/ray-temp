#include <assert.h>

#include "plasma_protocol.h"
#include "io.h"

#include "format/plasma_builder.h"
#include "plasma.h"

#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

protocol_builder *make_protocol_builder(void) {
  protocol_builder *builder = malloc(sizeof(protocol_builder));
  CHECK(builder);
  flatcc_builder_init(builder);
  return builder;
}

void free_protocol_builder(protocol_builder *builder) {
  flatcc_builder_clear(builder);
  free(builder);
}

/**
 * Writes an array of object IDs into a flatbuffer buffer and return
 * the resulting vector.
 *
 * @params B Pointer to the flatbuffer builder.
 * @param object_ids Array of object IDs to be written.
 * @param num_objectsnum_objects number of element in the array.
 *
 * @return Reference to the flatbuffer string vector.
 */
flatbuffers_string_vec_ref_t object_ids_to_flatbuffer(flatcc_builder_t *B,
                                                      object_id object_ids[],
                                                      int64_t num_objects) {
  flatbuffers_string_vec_start(B);
  for (int i = 0; i < num_objects; i++) {
    flatbuffers_string_ref_t id = flatbuffers_string_create(B, (const char *) &object_ids[i].id[0], UNIQUE_ID_SIZE);
    flatbuffers_string_vec_push(B, id);
  }
  return flatbuffers_string_vec_end(B);
}

/**
 * Reads an array of object IDs from a flatbuffer vector.
 * 
 * @param object_id_vector Flatbuffer vector containing object IDs.
 * @param object_ids_ptr Pointer to array that will contain the object IDs. The
 *                       array is allocated by this function and must be freed
 *                       by the user.
 * @param num_objects Pointer to the number of objects, will be written by
 *                    this method.
 * @return Void.
 */
void object_ids_from_flatbuffer(flatbuffers_string_vec_t object_id_vector,
                                OUT object_id **object_ids_ptr,
                                OUT int64_t *num_objects) {
  *num_objects = flatbuffers_string_vec_len(object_id_vector);
  if (*num_objects == 0) {
    *object_ids_ptr = NULL;
    *num_objects = 0;
  }
  *object_ids_ptr = malloc((*num_objects) * sizeof(object_id));
  object_id *object_ids = *object_ids_ptr;
  for (int i = 0; i < *num_objects; i++) {
    memcpy(&object_ids[i].id[0], flatbuffers_string_vec_at(object_id_vector, i), UNIQUE_ID_SIZE);
  }
}

void object_ids_from_flatbuffer2(flatbuffers_string_vec_t object_id_vector,
                                 object_id object_ids[],
                                 int64_t num_objects) {
  CHECK(flatbuffers_string_vec_len(object_id_vector) == num_objects);
  for (int64_t i = 0; i < num_objects; ++i) {
    memcpy(&object_ids[i].id[0], flatbuffers_string_vec_at(object_id_vector, i), UNIQUE_ID_SIZE);
  }
}

/**
 * Finalize the flatbuffers and write a message with the result to a
 * file descriptor.
 *
 * @param B Pointer to the flatbuffer builder.
 * @param fd File descriptor the message gets written to.
 * @param message_type Type of the message that is written.
 *
 * @return Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int finalize_buffer_and_send(flatcc_builder_t *B, int fd, int message_type) {
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(B, &size);
  int r = write_message(fd, PLASMA_PROTOCOL_VERSION, message_type, size, buff);
  free(buff);
  flatcc_builder_reset(B);
  return r;
}

uint8_t *plasma_receive(int sock, int64_t message_type) {
  int64_t type;
  int64_t length;
  uint8_t *reply_data;
  read_message(sock, PLASMA_PROTOCOL_VERSION, &type, &length, &reply_data);
  CHECK(type == message_type);
  return reply_data;
}

int plasma_send_CreateRequest(int sock,
                              protocol_builder *B,
                              object_id object_id,
                              int64_t data_size,
                              int64_t metadata_size) {
  PlasmaCreateRequest_start_as_root(B);
  PlasmaCreateRequest_object_id_create(B, (const char *)&object_id.id[0], UNIQUE_ID_SIZE);
  PlasmaCreateRequest_data_size_add(B, data_size);
  PlasmaCreateRequest_metadata_size_add(B, metadata_size);
  PlasmaCreateRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaCreateRequest);
}

void plasma_read_CreateRequest(uint8_t *data,
                               object_id *object_id,
                               int64_t *data_size,
                               int64_t *metadata_size) {
  DCHECK(data);
	PlasmaCreateRequest_table_t req = PlasmaCreateRequest_as_root(data);
  *data_size = PlasmaCreateRequest_data_size(req);
  *metadata_size = PlasmaCreateRequest_metadata_size(req);
  flatbuffers_string_t id = PlasmaCreateRequest_object_id(req);
  DCHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
}

int plasma_send_CreateReply(int sock,
                            protocol_builder *B,
                            object_id object_id,
                            plasma_object *object,
                            int error_code) {
  PlasmaCreateReply_start_as_root(B);
  PlasmaCreateReply_object_id_create(B, (const char *)&object_id.id[0], UNIQUE_ID_SIZE);
  PlasmaCreateReply_plasma_object_create(B,
                                         object->handle.store_fd,
                                         object->handle.mmap_size,
                                         object->data_offset,
                                         object->data_size,
                                         object->metadata_offset,
                                         object->metadata_size);
  PlasmaCreateReply_error_add(B, error_code);
  PlasmaCreateReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaCreateReply);
}


void plasma_read_CreateReply(uint8_t *data,
                             object_id *object_id,
                             plasma_object *object,
                             int *error_code) {
  DCHECK(data);
  PlasmaCreateReply_table_t rep = PlasmaCreateReply_as_root(data);
  flatbuffers_string_t id = PlasmaCreateReply_object_id(rep);
  CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
  PlasmaObject_struct_t obj = PlasmaCreateReply_plasma_object(rep);
  object->handle.store_fd = PlasmaObject_segment_index(obj);
  object->handle.mmap_size = PlasmaObject_mmap_size(obj);
  object->data_offset = PlasmaObject_data_offset(obj);
  object->data_size = PlasmaObject_data_size(obj);
  object->metadata_offset = PlasmaObject_metadata_offset(obj);
  object->metadata_size = PlasmaObject_metadata_size(obj);
  *error_code = PlasmaCreateReply_error(rep);
}

#define DEFINE_SIMPLE_SEND_REQUEST(MESSAGE_NAME) \
  int plasma_send_##MESSAGE_NAME(int sock, \
                                 protocol_builder *B, \
                                 object_id object_id) { \
    Plasma##MESSAGE_NAME##_start_as_root(B); \
    Plasma##MESSAGE_NAME##_object_id_create(B, (const char *) &object_id.id[0], UNIQUE_ID_SIZE); \
    Plasma##MESSAGE_NAME##_end_as_root(B); \
    return finalize_buffer_and_send(B, sock, MessageType_Plasma##MESSAGE_NAME); \
  }

#define DEFINE_SIMPLE_READ_REQUEST(MESSAGE_NAME) \
  void plasma_read_##MESSAGE_NAME(uint8_t *data, object_id *object_id) { \
    DCHECK(data); \
    Plasma##MESSAGE_NAME##_table_t req = Plasma##MESSAGE_NAME##_as_root(data); \
    flatbuffers_string_t id = Plasma##MESSAGE_NAME##_object_id(req); \
    CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE); \
    memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE); \
  }

#define DEFINE_SIMPLE_SEND_REPLY(MESSAGE_NAME) \
  int plasma_send_##MESSAGE_NAME(int sock, \
                                 protocol_builder *B, \
                                 object_id object_id, \
                                 int error) { \
    Plasma##MESSAGE_NAME##_start_as_root(B); \
    Plasma##MESSAGE_NAME##_object_id_create(B, (const char *) &object_id.id[0], UNIQUE_ID_SIZE); \
    Plasma##MESSAGE_NAME##_error_add(B, error); \
    Plasma##MESSAGE_NAME##_end_as_root(B); \
    return finalize_buffer_and_send(B, sock, MessageType_Plasma##MESSAGE_NAME); \
  }

#define DEFINE_SIMPLE_READ_REPLY(MESSAGE_NAME) \
  void plasma_read_##MESSAGE_NAME(uint8_t *data, object_id *object_id, int *error) { \
    DCHECK(data); \
    Plasma##MESSAGE_NAME##_table_t req = Plasma##MESSAGE_NAME##_as_root(data); \
    flatbuffers_string_t id = Plasma##MESSAGE_NAME##_object_id(req); \
    CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE); \
    memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE); \
    *error = Plasma##MESSAGE_NAME##_error(req); \
  }

int plasma_send_SealRequest(int sock,
                            protocol_builder *B,
                            object_id object_id,
                            unsigned char *digest) {
  PlasmaSealRequest_start_as_root(B);
  PlasmaSealRequest_object_id_create(B, (const char *) &object_id.id[0], UNIQUE_ID_SIZE);
  PlasmaSealRequest_digest_create(B, digest, DIGEST_SIZE);
  PlasmaSealRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaSealRequest);
}

void plasma_read_SealRequest(uint8_t *data, object_id *object_id, unsigned char *digest) {
  DCHECK(data);
  PlasmaSealRequest_table_t req = PlasmaSealRequest_as_root(data);
  flatbuffers_string_t id = PlasmaSealRequest_object_id(req);
  CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
  flatbuffers_uint8_vec_t d = PlasmaSealRequest_digest(req);
  CHECK(flatbuffers_uint8_vec_len(d) == DIGEST_SIZE);
  memcpy(digest, d, DIGEST_SIZE);
}

DEFINE_SIMPLE_SEND_REPLY(SealReply);
DEFINE_SIMPLE_READ_REPLY(SealReply);

DEFINE_SIMPLE_SEND_REQUEST(ReleaseRequest);
DEFINE_SIMPLE_READ_REQUEST(ReleaseRequest);
DEFINE_SIMPLE_SEND_REPLY(ReleaseReply);
DEFINE_SIMPLE_READ_REPLY(ReleaseReply);

DEFINE_SIMPLE_SEND_REQUEST(DeleteRequest);
DEFINE_SIMPLE_READ_REQUEST(DeleteRequest);
DEFINE_SIMPLE_SEND_REPLY(DeleteReply);
DEFINE_SIMPLE_READ_REPLY(DeleteReply);

/* Plasma status message. */

int plasma_send_StatusRequest(int sock, protocol_builder *B, object_id object_ids[], int64_t num_objects) {
  PlasmaStatusRequest_start_as_root(B);
  PlasmaStatusRequest_object_ids_add(B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  PlasmaStatusRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaStatusRequest);
}

int64_t plasma_read_StatusRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  PlasmaGetRequest_table_t req = PlasmaGetRequest_as_root(data);
  return flatbuffers_string_vec_len(PlasmaGetRequest_object_ids(req));
}

void plasma_read_StatusRequest(uint8_t *data, object_id object_ids[], int64_t num_objects) {
  DCHECK(data);
  PlasmaStatusRequest_table_t req = PlasmaStatusRequest_as_root(data);
  object_ids_from_flatbuffer2(PlasmaStatusRequest_object_ids(req), object_ids, num_objects);
}

/* Plasma get message. */

/*
void plasma_read_GetRequest(uint8_t *data,
                            object_id** object_ids_ptr,
                            int64_t *num_objects) {
  DCHECK(data);
  PlasmaGetRequest_table_t req = PlasmaGetRequest_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetRequest_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);
}
*/

/* Plasma evict message. */

int plasma_send_EvictRequest(int sock,
                               protocol_builder *B,
                               int64_t num_bytes) {
  PlasmaEvictRequest_start_as_root(B);
  PlasmaEvictRequest_num_bytes_add(B, num_bytes);
  PlasmaEvictRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaEvictRequest);
}

void plasma_read_EvictRequest(uint8_t *data, int64_t *num_bytes) {
  DCHECK(data);
  PlasmaEvictRequest_table_t req = PlasmaEvictRequest_as_root(data);
  *num_bytes = PlasmaEvictRequest_num_bytes(req);
}

int plasma_send_EvictReply(int sock,
                               protocol_builder *B,
                               int64_t num_bytes) {
  PlasmaEvictReply_start_as_root(B);
  PlasmaEvictReply_num_bytes_add(B, num_bytes);
  PlasmaEvictReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaEvictReply);
}

void plasma_read_EvictReply(uint8_t *data, int64_t *num_bytes) {
  DCHECK(data);
  PlasmaEvictReply_table_t req = PlasmaEvictReply_as_root(data);
  *num_bytes = PlasmaEvictReply_num_bytes(req);
}

int plasma_send_GetRequest(int sock,
                           protocol_builder *B,
                           object_id object_ids[],
                           int64_t num_objects) {
  PlasmaGetRequest_start_as_root(B);
  PlasmaGetRequest_object_ids_add(B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  PlasmaGetRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaGetRequest);
}

void plasma_read_GetRequest(uint8_t *data,
                            object_id** object_ids_ptr,
                            int64_t *num_objects) {
  DCHECK(data);
  PlasmaGetRequest_table_t req = PlasmaGetRequest_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetRequest_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);
}

int plasma_send_GetReply(int sock,
                         protocol_builder *B,
                         object_id object_ids[],
                         plasma_object plasma_objects[],
                         int64_t num_objects) {
  PlasmaGetReply_start_as_root(B);

  flatbuffers_string_vec_ref_t ids = object_ids_to_flatbuffer(B, object_ids, num_objects);
  PlasmaGetReply_object_ids_add(B, ids);

  PlasmaObject_vec_start(B);
  for (int i = 0; i < num_objects; ++i) {
    plasma_object obj = plasma_objects[i];
    PlasmaObject_t plasma_obj;
    memset(&plasma_obj, 0, sizeof(PlasmaObject_t));
    plasma_obj.segment_index = obj.handle.store_fd;
    plasma_obj.mmap_size = obj.handle.mmap_size;
    plasma_obj.data_offset = obj.data_offset;
    plasma_obj.data_size = obj.data_size;
    plasma_obj.metadata_offset = obj.metadata_offset;
    plasma_obj.metadata_size = obj.metadata_size;
    PlasmaObject_vec_push(B, &plasma_obj);
  }
  PlasmaObject_vec_ref_t object_vec = PlasmaObject_vec_end(B);
  PlasmaGetReply_plasma_objects_add(B, object_vec);
  PlasmaGetReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaGetReply);
}

void plasma_read_GetReply(uint8_t *data,
                         object_id** object_ids_ptr,
                         plasma_object plasma_objects[],
                         int64_t *num_objects) {
  CHECK(data);
  PlasmaGetReply_table_t req = PlasmaGetReply_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetReply_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);

  memset(plasma_objects, 0, sizeof(plasma_object) * (*num_objects));
  PlasmaObject_vec_t plasma_objects_vector = PlasmaGetReply_plasma_objects(req);

  for (int i = 0; i < *num_objects; ++i) {
    PlasmaObject_struct_t obj = PlasmaObject_vec_at(plasma_objects_vector, i);
    plasma_objects[i].handle.store_fd = PlasmaObject_segment_index(obj);
    plasma_objects[i].handle.mmap_size = PlasmaObject_mmap_size(obj);
    plasma_objects[i].data_offset = PlasmaObject_data_offset(obj);
    plasma_objects[i].data_size = PlasmaObject_data_size(obj);
    plasma_objects[i].metadata_offset = PlasmaObject_metadata_offset(obj);
    plasma_objects[i].metadata_size = PlasmaObject_metadata_size(obj);
  }
}

/* Plasma fetch messages. */

int plasma_send_FetchRequest(int sock,
                           protocol_builder *B,
                           object_id object_ids[],
                           int64_t num_objects) {
  PlasmaFetchRequest_start_as_root(B);
  PlasmaFetchRequest_object_ids_add(B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  PlasmaFetchRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaFetchRequest);
}

void plasma_read_FetchRequest(uint8_t *data,
                            object_id** object_ids_ptr,
                            int64_t *num_objects) {
  DCHECK(data);
  PlasmaFetchRequest_table_t req = PlasmaFetchRequest_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaFetchRequest_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);
}

/* Plasma wait messages. */

int plasma_send_WaitRequest(int sock,
                            protocol_builder *B,
                            object_request object_requests[],
                            int num_requests,
                            int num_ready_objects,
                            int64_t timeout_ms) {
  PlasmaWaitRequest_start_as_root(B);
  ObjectRequest_vec_start(B);
  for (int i = 0; i < num_requests; ++i) {
    flatbuffers_string_ref_t id =
      flatbuffers_string_create(B, (const char *) &object_requests[i].object_id.id[0], UNIQUE_ID_SIZE);
    ObjectRequest_vec_push_create(B, id, (int32_t)object_requests[i].type);
  }
  ObjectRequest_vec_ref_t objreq_vec = ObjectRequest_vec_end(B);
  PlasmaWaitRequest_object_requests_add(B, objreq_vec);
  PlasmaWaitRequest_num_ready_objects_add(B, num_ready_objects);
  PlasmaWaitRequest_timeout_add(B, timeout_ms);
  PlasmaWaitRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaWaitRequest);
}

int plasma_read_WaitRequest_num_object_ids(uint8_t *data) {
  DCHECK(data);
  PlasmaWaitRequest_table_t req = PlasmaWaitRequest_as_root(data);
  return ObjectRequest_vec_len(PlasmaWaitRequest_object_requests(req));
}

void plasma_read_WaitRequest(uint8_t *data,
                             object_request object_requests[],
                             int num_object_ids,
                             int64_t *timeout_ms,
                             int *num_ready_objects) {
  DCHECK(data);
  PlasmaWaitRequest_table_t req = PlasmaWaitRequest_as_root(data);
  ObjectRequest_vec_t objreq_vec = PlasmaWaitRequest_object_requests(req);
  CHECK(num_object_ids == ObjectRequest_vec_len(objreq_vec));
  for (int i = 0; i < num_object_ids; i++) {
    ObjectRequest_table_t objreq = ObjectRequest_vec_at(objreq_vec, i);
    memcpy(&object_requests[i].object_id.id[0], ObjectRequest_object_id(objreq), UNIQUE_ID_SIZE);
    object_requests[i].type = ObjectRequest_type(objreq);
  }
  *timeout_ms = PlasmaWaitRequest_timeout(req);
  *num_ready_objects = PlasmaWaitRequest_num_ready_objects(req);
}

int plasma_send_WaitReply(int sock,
                          protocol_builder *B,
                          object_request object_requests[],
                          int num_ready_objects) {
  PlasmaWaitReply_start_as_root(B);
  ObjectReply_vec_start(B);
  for (int i = 0; i < num_ready_objects; ++i) {
    flatbuffers_string_ref_t id =
      flatbuffers_string_create(B, (const char *) &object_requests[i].object_id.id[0], UNIQUE_ID_SIZE);
    ObjectReply_vec_push_create(B, id, (int32_t)object_requests[i].status);
  }
  ObjectReply_vec_ref_t objreq_vec = ObjectReply_vec_end(B);
  PlasmaWaitReply_object_requests_add(B, objreq_vec);
  PlasmaWaitReply_num_ready_objects_add(B, num_ready_objects);
  PlasmaWaitReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaWaitReply);
}


void plasma_read_WaitReply(uint8_t *data,
                           object_request object_requests[],
                           int *num_ready_objects) {
  DCHECK(data);
  PlasmaWaitReply_table_t req = PlasmaWaitReply_as_root(data);
  ObjectReply_vec_t objreq_vec = PlasmaWaitReply_object_requests(req);
  // TODO (ion): This is risky, maybe num_ready_objects should contain length of object_request object_requests?
  *num_ready_objects = ObjectReply_vec_len(objreq_vec);
  for (int i = 0; i < *num_ready_objects; i++) {
    ObjectReply_table_t objreq = ObjectReply_vec_at(objreq_vec, i);
    memcpy(&object_requests[i].object_id.id[0], ObjectReply_object_id(objreq), UNIQUE_ID_SIZE);
    object_requests[i].status = ObjectReply_status(objreq);
  }
}

int plasma_send_SubscribeRequest(int sock, protocol_builder* B) {
  PlasmaSubscribeRequest_start_as_root(B);
  PlasmaSubscribeRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaSubscribeRequest);
}
