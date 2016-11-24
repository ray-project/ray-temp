//
// Created by Ion Stoica on 11/20/16.
//
#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "plasma.h"
#include "plasma_client.h"

SUITE(plasma_client_tests);


TEST plasma_status_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect("/tmp/store1", "/tmp/manager1");
  plasma_connection *plasma_conn2 = plasma_connect("/tmp/store2", "/tmp/manager2");
  object_id oid1 = {{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
  object_id oid2 = {{2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};

  /** Test for object non-existence */
  int status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_DOES_NOT_EXIST);

  /** Test for the object being in local Plasma store. */
  /** First cerate object */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);
  /** sleep to avoid race condition of Plasma Manager waiting for notification. */
  sleep(1);
  status = plasma_status(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_LOCAL);

  /** Test for object being remote. */
  status = plasma_status(plasma_conn2, oid1);
  ASSERT(status == PLASMA_OBJECT_REMOTE);

  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);
  PASS();
}

TEST plasma_fetch_remote_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect("/tmp/store1", "/tmp/manager1");
  plasma_connection *plasma_conn2 = plasma_connect("/tmp/store2", "/tmp/manager2");
  object_id oid1 = {{3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
  object_id oid2 = {{4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};

  /** Test for object non-existence */
  int status;

  /** No object in the system */
  status = plasma_fetch_remote(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_DOES_NOT_EXIST);

  /** Test for the object being in local Plasma store. */
  /** First cerate object */
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);

  /** Object with ID oid1 has been jsut inserted. On the next fetch we might either
   * find the object or not, depending on whether the Plasma Manager has received the
   * notification from the Plasma Store or not when fetch is execture. */
  status = plasma_fetch_remote(plasma_conn1, oid1);
  ASSERT((status == PLASMA_OBJECT_LOCAL) || (status == PLASMA_OBJECT_DOES_NOT_EXIST));

  /** Sleep to make sure Plasma Manager got the notification. */
  sleep(1);
  status = plasma_fetch_remote(plasma_conn1, oid1);
  ASSERT(status == PLASMA_OBJECT_LOCAL);

  /** Test for object being remote. */
  status = plasma_fetch_remote(plasma_conn2, oid1);
  ASSERT(status == PLASMA_OBJECT_REMOTE);

  /** Sleep to make sure the object has been fetched and it is
   * now stored in the local Plasma Store */
  sleep(1);
  status = plasma_fetch_remote(plasma_conn2, oid1);
  ASSERT(status == PLASMA_OBJECT_LOCAL);

  sleep(1);
  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);
  PASS();
}

void init_data_123(uint8_t *data, uint64_t size, uint8_t base) {
  for (int i = 0; i < size; i++) {
    data[i] = base + i;
  }
}

bool is_equal_data_123(uint8_t *data1, uint8_t *data2, uint64_t size) {
  for (int i = 0; i < size; i++) {
    if (data1[i] != data2[i]) {
      return false;
    };
  }
  return true;
}


TEST plasma_get_local_tests(void) {
  plasma_connection *plasma_conn = plasma_connect("/tmp/store1", "/tmp/manager1");
  object_id oid = {{10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
  object_buffer obj_buffer;

  /** Test for object non-existence */
  int status = plasma_get_local(plasma_conn, oid, &obj_buffer);
  ASSERT(status == false);

  /** Test for the object being in local Plasma store. */
  /** First cerate object */
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn, oid, data_size, metadata, metadata_size, &data);
  init_data_123(data, data_size, 0);
  plasma_seal(plasma_conn, oid);

  sleep(1);
  status = plasma_get_local(plasma_conn, oid, &obj_buffer);
  ASSERT(status == true);
  ASSERT(is_equal_data_123(data, obj_buffer.data, data_size) == true);

  sleep(1);
  plasma_disconnect(plasma_conn);
  PASS();
}


TEST plasma_wait_for_objects_tests(void) {
  plasma_connection *plasma_conn1 = plasma_connect("/tmp/store1", "/tmp/manager1");
  plasma_connection *plasma_conn2 = plasma_connect("/tmp/store2", "/tmp/manager2");
  object_id oid1 = {{21,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
  object_id oid2 = {{22,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}};
#define NUM_OBJ_REQUEST 2
#define WAIT_TIMEOUT_MS 1000
  object_request obj_requests[NUM_OBJ_REQUEST];

  obj_requests[0].object_id = oid1;
  obj_requests[0].type = PLASMA_OBJECT_ANYWHERE;
  obj_requests[1].object_id = oid2;
  obj_requests[1].type = PLASMA_OBJECT_ANYWHERE;

  printf(">>>>>> 1: "); object_requests_print(2, obj_requests);

  struct timeval start, end;
  gettimeofday(&start, NULL);
  int n = plasma_wait_for_objects(plasma_conn1, NUM_OBJ_REQUEST,
                                  obj_requests, NUM_OBJ_REQUEST,
                                  WAIT_TIMEOUT_MS);
  ASSERT(n == 0);
  gettimeofday(&end, NULL);
  float diff_ms = (end.tv_sec - start.tv_sec);
  diff_ms =(((diff_ms*1000000.) + end.tv_usec) - (start.tv_usec))/1000.;
  ASSERT(diff_ms > WAIT_TIMEOUT_MS);

  /** create and insert an object in plasma_conn1 */
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  uint8_t *data;
  plasma_create(plasma_conn1, oid1, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn1, oid1);

  sleep(1);
  n = plasma_wait_for_objects(plasma_conn1, NUM_OBJ_REQUEST,
                              obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS);
  ASSERT(n == 1);

  /** create and insert an object in plasma_conn1 */
  plasma_create(plasma_conn2, oid2, data_size, metadata, metadata_size, &data);
  plasma_seal(plasma_conn2, oid2);

  n = plasma_wait_for_objects(plasma_conn1, NUM_OBJ_REQUEST,
                              obj_requests, NUM_OBJ_REQUEST,
                              WAIT_TIMEOUT_MS);

  printf(">>>>>> 2: n = %d, duration (ms) = %f ", n, diff_ms);
  object_requests_print(2, obj_requests);

  sleep(1);

  plasma_disconnect(plasma_conn1);
  plasma_disconnect(plasma_conn2);
  PASS();
}


SUITE(plasma_client_tests) {
  RUN_TEST(plasma_status_tests);
  RUN_TEST(plasma_fetch_remote_tests);
  RUN_TEST(plasma_get_local_tests);
  RUN_TEST(plasma_wait_for_objects_tests);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_client_tests);
  GREATEST_MAIN_END();
}
