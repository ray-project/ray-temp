/* PLASMA CLIENT: Client library for using the plasma store and manager */

#ifdef _WIN32
#include <Win32_Interop/win32_types.h>
#endif

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <strings.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <netdb.h>
#include <poll.h>

#include "common.h"
#include "io.h"
#include "plasma.h"
#include "plasma_protocol.h"
#include "plasma_client.h"

/* C++ includes */
#include <deque>
#include <vector>
#include <thread>

extern "C" {
#include "sha256.h"
#include "fling.h"

#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

#define XXH64_DEFAULT_SEED 0
}

#define THREADPOOL_SIZE 8
#define BYTES_IN_MB (1 << 20)
static std::vector<std::thread> threadpool_(THREADPOOL_SIZE);

struct ClientMmapTableEntry {
  /** Key that uniquely identifies the  memory mapped file. In practice, we
   *  take the numerical value of the file descriptor in the object store. */
  int key;
  /** The result of mmap for this file descriptor. */
  uint8_t *pointer;
  /** The length of the memory-mapped file. */
  size_t length;
  /** The number of objects in this memory-mapped file that are currently being
   *  used by the client. When this count reaches zeros, we unmap the file. */
  int count;
};

struct ObjectInUseEntry {
  /** The ID of the object. This is used as the key in the hash table. */
  ObjectID object_id;
  /** A count of the number of times this client has called plasma_create or
   *  plasma_get on this object ID minus the number of calls to plasma_release.
   *  When this count reaches zero, we remove the entry from the objects_in_use
   *  and decrement a count in the relevant ClientMmapTableEntry. */
  int count;
  /** Cached information to read the object. */
  PlasmaObject object;
  /** A flag representing whether the object has been sealed. */
  bool is_sealed;
};

/** Configuration options for the plasma client. */
typedef struct {
  /** Number of release calls we wait until the object is actually released.
   *  This allows us to avoid invalidating the cpu cache on workers if objects
   *  are reused accross tasks. */
  int release_delay;
} plasma_client_config;

/** Information about a connection between a Plasma Client and Plasma Store.
 *  This is used to avoid mapping the same files into memory multiple times. */
struct PlasmaConnection {
  /** File descriptor of the Unix domain socket that connects to the store. */
  int store_conn;
  /** File descriptor of the Unix domain socket that connects to the manager. */
  int manager_conn;
  /** File descriptor of the Unix domain socket on which client receives event
   *  notifications for the objects it subscribes for when these objects are
   *  sealed either locally or remotely. */
  int manager_conn_subscribe;
  /** Buffer that holds memory for serializing plasma protocol messages. */
  protocol_builder *builder;
  /** Table of dlmalloc buffer files that have been memory mapped so far. This
   *  is a hash table mapping a file descriptor to a struct containing the
   *  address of the corresponding memory-mapped file. */
  std::unordered_map<int, ClientMmapTableEntry *> mmap_table;
  /** A hash table of the object IDs that are currently being used by this
   * client. */
  std::unordered_map<ObjectID, ObjectInUseEntry *, UniqueIDHasher> objects_in_use;
  /** Object IDs of the last few release calls. This is a deque and
   *  is used to delay releasing objects to see if they can be reused by
   *  subsequent tasks so we do not unneccessarily invalidate cpu caches.
   *  TODO(pcm): replace this with a proper lru cache using the size of the L3
   *  cache. */
  std::deque<ObjectID> release_history;
  /** The number of bytes in the combined objects that are held in the release
   *  history doubly-linked list. If this is too large then the client starts
   *  releasing objects. */
  int64_t in_use_object_bytes;
  /** Configuration options for the plasma client. */
  plasma_client_config config;
  /** The amount of memory available to the Plasma store. The client needs this
   *  information to make sure that it does not delay in releasing so much
   *  memory that the store is unable to evict enough objects to free up space.
   */
  int64_t store_capacity;
};

ClientMmapTableEntry *get_mmap_table_entry(PlasmaConnection *conn, int fd) {
  auto it = conn->mmap_table.find(fd);
  if (it == conn->mmap_table.end()) {
    return NULL;
  }
  return it->second;
}

ObjectInUseEntry *get_object_in_use_entry(PlasmaConnection *conn, ObjectID object_id) {
  auto it = conn->objects_in_use.find(object_id);
  if (it == conn->objects_in_use.end()) {
    return NULL;
  }
  return it->second;
}

/* If the file descriptor fd has been mmapped in this client process before,
 * return the pointer that was returned by mmap, otherwise mmap it and store the
 * pointer in a hash table. */
uint8_t *lookup_or_mmap(PlasmaConnection *conn,
                        int fd,
                        int store_fd_val,
                        int64_t map_size) {
  ClientMmapTableEntry *entry = get_mmap_table_entry(conn, store_fd_val);
  if (entry) {
    close(fd);
    return entry->pointer;
  } else {
    uint8_t *result = (uint8_t *) mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                                       MAP_SHARED, fd, 0);
    if (result == MAP_FAILED) {
      LOG_FATAL("mmap failed");
    }
    close(fd);
    ClientMmapTableEntry *entry = new ClientMmapTableEntry();
    entry->key = store_fd_val;
    entry->pointer = result;
    entry->length = map_size;
    entry->count = 0;
    conn->mmap_table[store_fd_val] = entry;
    return result;
  }
}

/* Get a pointer to a file that we know has been memory mapped in this client
 * process before. */
uint8_t *lookup_mmapped_file(PlasmaConnection *conn, int store_fd_val) {
  ClientMmapTableEntry *entry = get_mmap_table_entry(conn, store_fd_val);
  CHECK(entry);
  return entry->pointer;
}

void increment_object_count(PlasmaConnection *conn,
                            ObjectID object_id,
                            PlasmaObject *object,
                            bool is_sealed) {
  /* Increment the count of the object to track the fact that it is being used.
   * The corresponding decrement should happen in plasma_release. */
  ObjectInUseEntry *object_entry = get_object_in_use_entry(conn, object_id);
  if (object_entry == NULL) {
    /* Add this object ID to the hash table of object IDs in use. The
     * corresponding call to free happens in plasma_release. */
    object_entry = new ObjectInUseEntry();
    object_entry->object_id = object_id;
    object_entry->object = *object;
    object_entry->count = 0;
    object_entry->is_sealed = is_sealed;
    conn->objects_in_use[object_id] = object_entry;
    /* Increment the count of the number of objects in the memory-mapped file
     * that are being used. The corresponding decrement should happen in
     * plasma_release. */
    ClientMmapTableEntry *entry = get_mmap_table_entry(conn, object->handle.store_fd);
    CHECK(entry != NULL);
    CHECK(entry->count >= 0);
    /* Update the in_use_object_bytes. */
    conn->in_use_object_bytes +=
        (object_entry->object.data_size + object_entry->object.metadata_size);
    entry->count += 1;
  } else {
    CHECK(object_entry->count > 0);
  }
  /* Increment the count of the number of instances of this object that are
   * being used by this client. The corresponding decrement should happen in
   * plasma_release. */
  object_entry->count += 1;
}

int plasma_create(PlasmaConnection *conn,
                  ObjectID obj_id,
                  int64_t data_size,
                  uint8_t *metadata,
                  int64_t metadata_size,
                  uint8_t **data) {
  LOG_DEBUG("called plasma_create on conn %d with size %" PRId64
            " and metadata size %" PRId64,
            conn->store_conn, data_size, metadata_size);
  CHECK(plasma_send_CreateRequest(conn->store_conn, conn->builder, obj_id,
                                  data_size, metadata_size) >= 0);
  uint8_t *reply_data =
      plasma_receive(conn->store_conn, MessageType_PlasmaCreateReply);
  int error;
  ObjectID id;
  PlasmaObject object;
  plasma_read_CreateReply(reply_data, &id, &object, &error);
  free(reply_data);
  if (error != PlasmaError_OK) {
    LOG_DEBUG("returned from plasma_create with error %d", error);
    CHECK(error == PlasmaError_OutOfMemory ||
          error == PlasmaError_ObjectExists);
    return error;
  }
  /* If the CreateReply included an error, then the store will not send a file
   * descriptor. */
  int fd = recv_fd(conn->store_conn);
  CHECKM(fd >= 0, "recv not successful");
  CHECK(object.data_size == data_size);
  CHECK(object.metadata_size == metadata_size);
  /* The metadata should come right after the data. */
  CHECK(object.metadata_offset == object.data_offset + data_size);
  *data = lookup_or_mmap(conn, fd, object.handle.store_fd,
                         object.handle.mmap_size) +
          object.data_offset;
  /* If plasma_create is being called from a transfer, then we will not copy the
   * metadata here. The metadata will be written along with the data streamed
   * from the transfer. */
  if (metadata != NULL) {
    /* Copy the metadata to the buffer. */
    memcpy(*data + object.data_size, metadata, metadata_size);
  }
  /* Increment the count of the number of instances of this object that this
   * client is using. A call to plasma_release is required to decrement this
   * count. Cache the reference to the object. */
  increment_object_count(conn, obj_id, &object, false);
  /* We increment the count a second time (and the corresponding decrement will
   * happen in a plasma_release call in plasma_seal) so even if the buffer
   * returned by plasma_create goes out of scope, the object does not get
   * released before the call to plasma_seal happens. */
  increment_object_count(conn, obj_id, &object, false);
  return PlasmaError_OK;
}

void plasma_get(PlasmaConnection *conn,
                ObjectID object_ids[],
                int64_t num_objects,
                int64_t timeout_ms,
                ObjectBuffer object_buffers[]) {
  /* Fill out the info for the objects that are already in use locally. */
  bool all_present = true;
  for (int i = 0; i < num_objects; ++i) {
    ObjectInUseEntry *object_entry = get_object_in_use_entry(conn, object_ids[i]);
    if (object_entry == NULL) {
      /* This object is not currently in use by this client, so we need to send
       * a request to the store. */
      all_present = false;
      /* Make a note to ourselves that the object is not present. */
      object_buffers[i].data_size = -1;
    } else {
      PlasmaObject *object;
      /* NOTE: If the object is still unsealed, we will deadlock, since we must
       * have been the one who created it. */
      CHECKM(object_entry->is_sealed,
             "Plasma client called get on an unsealed object that it created");
      object = &object_entry->object;
      object_buffers[i].data =
          lookup_mmapped_file(conn, object->handle.store_fd);
      object_buffers[i].data = object_buffers[i].data + object->data_offset;
      object_buffers[i].data_size = object->data_size;
      object_buffers[i].metadata = object_buffers[i].data + object->data_size;
      object_buffers[i].metadata_size = object->metadata_size;
      /* Increment the count of the number of instances of this object that this
       * client is using. A call to plasma_release is required to decrement this
       * count. Cache the reference to the object. */
      increment_object_count(conn, object_ids[i], object, true);
    }
  }

  if (all_present) {
    return;
  }

  /* If we get here, then the objects aren't all currently in use by this
   * client, so we need to send a request to the plasma store. */
  CHECK(plasma_send_GetRequest(conn->store_conn, conn->builder, object_ids,
                               num_objects, timeout_ms) >= 0);
  uint8_t *reply_data =
      plasma_receive(conn->store_conn, MessageType_PlasmaGetReply);
  ObjectID *received_obj_ids =
      (ObjectID *) malloc(num_objects * sizeof(ObjectID));
  PlasmaObject *object_data =
      (PlasmaObject *) malloc(num_objects * sizeof(PlasmaObject));
  PlasmaObject *object;
  plasma_read_GetReply(reply_data, received_obj_ids, object_data, num_objects);
  free(reply_data);

  for (int i = 0; i < num_objects; ++i) {
    DCHECK(ObjectID_equal(received_obj_ids[i], object_ids[i]));
    object = &object_data[i];
    if (object_buffers[i].data_size != -1) {
      /* If the object was already in use by the client, then the store should
       * have returned it. */
      DCHECK(object->data_size != -1);
      /* We won't use this file descriptor, but the store sent us one, so we
       * need to receive it and then close it right away so we don't leak file
       * descriptors. */
      int fd = recv_fd(conn->store_conn);
      close(fd);
      CHECK(fd >= 0);
      /* We've already filled out the information for this object, so we can
       * just continue. */
      continue;
    }
    /* If we are here, the object was not currently in use, so we need to
     * process the reply from the object store. */
    if (object->data_size != -1) {
      /* The object was retrieved. The user will be responsible for releasing
       * this object. */
      int fd = recv_fd(conn->store_conn);
      CHECK(fd >= 0);
      object_buffers[i].data = lookup_or_mmap(conn, fd, object->handle.store_fd,
                                              object->handle.mmap_size);
      /* Finish filling out the return values. */
      object_buffers[i].data = object_buffers[i].data + object->data_offset;
      object_buffers[i].data_size = object->data_size;
      object_buffers[i].metadata = object_buffers[i].data + object->data_size;
      object_buffers[i].metadata_size = object->metadata_size;
      /* Increment the count of the number of instances of this object that this
       * client is using. A call to plasma_release is required to decrement this
       * count. Cache the reference to the object. */
      increment_object_count(conn, received_obj_ids[i], object, true);
    } else {
      /* The object was not retrieved. Make sure we already put a -1 here to
       * indicate that the object was not retrieved. The caller is not
       * responsible for releasing this object. */
      DCHECK(object_buffers[i].data_size == -1);
      object_buffers[i].data_size = -1;
    }
  }
  free(object_data);
  free(received_obj_ids);
}

/**
 * This is a helper method for implementing plasma_release. We maintain a buffer
 * of release calls and only perform them once the buffer becomes full (as
 * judged by the aggregate sizes of the objects). There may be multiple release
 * calls for the same object ID in the buffer. In this case, the first release
 * calls will not do anything. The client will only send a message to the store
 * releasing the object when the client is truly done with the object.
 *
 * @param conn The plasma connection.
 * @param object_id The object ID to attempt to release.
 */
void plasma_perform_release(PlasmaConnection *conn, ObjectID object_id) {
  /* Decrement the count of the number of instances of this object that are
   * being used by this client. The corresponding increment should have happened
   * in plasma_get. */
  ObjectInUseEntry *object_entry = get_object_in_use_entry(conn, object_id);
  CHECK(object_entry != NULL);
  object_entry->count -= 1;
  CHECK(object_entry->count >= 0);
  /* Check if the client is no longer using this object. */
  if (object_entry->count == 0) {
    /* Decrement the count of the number of objects in this memory-mapped file
     * that the client is using. The corresponding increment should have
     * happened in plasma_get. */
    int fd = object_entry->object.handle.store_fd;
    ClientMmapTableEntry *entry = get_mmap_table_entry(conn, fd);
    CHECK(entry != NULL);
    entry->count -= 1;
    CHECK(entry->count >= 0);
    /* If none are being used then unmap the file. */
    if (entry->count == 0) {
      munmap(entry->pointer, entry->length);
      /* Remove the corresponding entry from the hash table. */
      conn->mmap_table.erase(fd);
      delete entry;
    }
    /* Tell the store that the client no longer needs the object. */
    CHECK(plasma_send_ReleaseRequest(conn->store_conn, conn->builder,
                                     object_id) >= 0);
    /* Update the in_use_object_bytes. */
    conn->in_use_object_bytes -=
        (object_entry->object.data_size + object_entry->object.metadata_size);
    DCHECK(conn->in_use_object_bytes >= 0);
    /* Remove the entry from the hash table of objects currently in use. */
    conn->objects_in_use.erase(object_id);
    delete object_entry;
  }
}

void plasma_release(PlasmaConnection *conn, ObjectID obj_id) {
  /* Add the new object to the release history. */
  conn->release_history.push_front(obj_id);
  /* If there are too many bytes in use by the client or if there are too many
   * pending release calls, and there are at least some pending release calls in
   * the release_history list, then release some objects. */
  while ((conn->in_use_object_bytes >
              MIN(L3_CACHE_SIZE_BYTES, conn->store_capacity / 100) ||
          conn->release_history.size() > conn->config.release_delay) &&
         conn->release_history.size() > 0) {
    /* Perform a release for the object ID for the first pending release. */
    plasma_perform_release(conn, conn->release_history.back());
    /* Remove the last entry from the release history. */
    conn->release_history.pop_back();
  }
}

/* This method is used to query whether the plasma store contains an object. */
void plasma_contains(PlasmaConnection *conn, ObjectID obj_id, int *has_object) {
  /* Check if we already have a reference to the object. */
  ObjectInUseEntry *object_entry = get_object_in_use_entry(conn, obj_id);
  if (object_entry) {
    *has_object = 1;
  } else {
    /* If we don't already have a reference to the object, check with the store
     * to see if we have the object. */
    plasma_send_ContainsRequest(conn->store_conn, conn->builder, obj_id);
    uint8_t *reply_data =
        plasma_receive(conn->store_conn, MessageType_PlasmaContainsReply);
    ObjectID object_id2;
    plasma_read_ContainsReply(reply_data, &object_id2, has_object);
    free(reply_data);
  }
}

static void compute_block_hash(const unsigned char *data,
                               int64_t nbytes,
                               uint64_t *hash) {
  XXH64_state_t hash_state;
  XXH64_reset(&hash_state, XXH64_DEFAULT_SEED);
  XXH64_update(&hash_state, data, nbytes);
  *hash = XXH64_digest(&hash_state);
}

static inline bool compute_object_hash_parallel(XXH64_state_t *hash_state,
                                                const unsigned char *data,
                                                int64_t nbytes) {
  /* Note that this function will likely be faster if the address of data is
   * aligned on a 64-byte boundary. */
  const uint64_t num_threads = THREADPOOL_SIZE;
  uint64_t threadhash[num_threads + 1];
  const uint64_t data_address = reinterpret_cast<uint64_t>(data);
  const uint64_t num_blocks = nbytes / BLOCK_SIZE;
  const uint64_t chunk_size = (num_blocks / num_threads) * BLOCK_SIZE;
  const uint64_t right_address = data_address + chunk_size * num_threads;
  const uint64_t suffix = (data_address + nbytes) - right_address;
  /* Now the data layout is | k * num_threads * block_size | suffix | ==
   * | num_threads * chunk_size | suffix |, where chunk_size = k * block_size.
   * Each thread gets a "chunk" of k blocks, except the suffix thread. */

  for (int i = 0; i < num_threads; i++) {
    threadpool_[i] =
        std::thread(compute_block_hash,
                    reinterpret_cast<uint8_t *>(data_address) + i * chunk_size,
                    chunk_size, &threadhash[i]);
  }
  compute_block_hash(reinterpret_cast<uint8_t *>(right_address), suffix,
                     &threadhash[num_threads]);

  /* Join the threads. */
  for (auto &t : threadpool_) {
    if (t.joinable()) {
      t.join();
    }
  }

  XXH64_update(hash_state, (unsigned char *) threadhash, sizeof(threadhash));
  return true;
}

static uint64_t compute_object_hash(const ObjectBuffer &obj_buffer) {
  XXH64_state_t hash_state;
  XXH64_reset(&hash_state, XXH64_DEFAULT_SEED);
  if (obj_buffer.data_size >= BYTES_IN_MB) {
    compute_object_hash_parallel(&hash_state, (unsigned char *) obj_buffer.data,
                                 obj_buffer.data_size);
  } else {
    XXH64_update(&hash_state, (unsigned char *) obj_buffer.data,
                 obj_buffer.data_size);
  }
  XXH64_update(&hash_state, (unsigned char *) obj_buffer.metadata,
               obj_buffer.metadata_size);
  return XXH64_digest(&hash_state);
}

bool plasma_compute_object_hash(PlasmaConnection *conn,
                                ObjectID obj_id,
                                unsigned char *digest) {
  /* Get the plasma object data. We pass in a timeout of 0 to indicate that
   * the operation should timeout immediately. */
  ObjectBuffer obj_buffer;
  ObjectID obj_id_array[1] = {obj_id};
  uint64_t hash;

  plasma_get(conn, obj_id_array, 1, 0, &obj_buffer);
  /* If the object was not retrieved, return false. */
  if (obj_buffer.data_size == -1) {
    return false;
  }
  /* Compute the hash. */
  hash = compute_object_hash(obj_buffer);
  memcpy(digest, &hash, sizeof(hash));
  /* Release the plasma object. */
  plasma_release(conn, obj_id);
  return true;
}

void plasma_seal(PlasmaConnection *conn, ObjectID object_id) {
  /* Make sure this client has a reference to the object before sending the
   * request to Plasma. */
  ObjectInUseEntry *object_entry = get_object_in_use_entry(conn, object_id);
  CHECKM(object_entry != NULL,
         "Plasma client called seal an object without a reference to it");
  CHECKM(!object_entry->is_sealed,
         "Plasma client called seal an already sealed object");
  object_entry->is_sealed = true;
  /* Send the seal request to Plasma. */
  static unsigned char digest[DIGEST_SIZE];
  CHECK(plasma_compute_object_hash(conn, object_id, &digest[0]));
  CHECK(plasma_send_SealRequest(conn->store_conn, conn->builder, object_id,
                                &digest[0]) >= 0);
  /* We call plasma_release to decrement the number of instances of this object
   * that are currently being used by this client. The corresponding increment
   * happened in plasma_create and was used to ensure that the object was not
   * released before the call to plasma_seal. */
  plasma_release(conn, object_id);
}

void plasma_delete(PlasmaConnection *conn, ObjectID object_id) {
  /* TODO(rkn): In the future, we can use this method to give hints to the
   * eviction policy about when an object will no longer be needed. */
}

int64_t plasma_evict(PlasmaConnection *conn, int64_t num_bytes) {
  /* Send a request to the store to evict objects. */
  CHECK(plasma_send_EvictRequest(conn->store_conn, conn->builder, num_bytes) >=
        0);
  /* Wait for a response with the number of bytes actually evicted. */
  int64_t type;
  int64_t length;
  uint8_t *reply_data;
  read_message(conn->store_conn, &type, &length, &reply_data);
  int64_t num_bytes_evicted;
  plasma_read_EvictReply(reply_data, &num_bytes_evicted);
  free(reply_data);
  return num_bytes_evicted;
}

int plasma_subscribe(PlasmaConnection *conn) {
  int fd[2];
  /* TODO: Just create 1 socket, bind it to port 0 to find a free port, and
   * send the port number instead, and let the client connect. */
  /* Create a non-blocking socket pair. This will only be used to send
   * notifications from the Plasma store to the client. */
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  /* Make the socket non-blocking. */
  int flags = fcntl(fd[1], F_GETFL, 0);
  CHECK(fcntl(fd[1], F_SETFL, flags | O_NONBLOCK) == 0);
  /* Tell the Plasma store about the subscription. */
  CHECK(plasma_send_SubscribeRequest(conn->store_conn, conn->builder) >= 0);
  /* Send the file descriptor that the Plasma store should use to push
   * notifications about sealed objects to this client. */
  CHECK(send_fd(conn->store_conn, fd[1]) >= 0);
  close(fd[1]);
  /* Return the file descriptor that the client should use to read notifications
   * about sealed objects. */
  return fd[0];
}

PlasmaConnection *plasma_connect(const char *store_socket_name,
                                 const char *manager_socket_name,
                                 int release_delay) {
  /* Initialize the store connection struct */
  PlasmaConnection *result = new PlasmaConnection();
  result->store_conn = connect_ipc_sock_retry(store_socket_name, -1, -1);
  if (manager_socket_name != NULL) {
    result->manager_conn = connect_ipc_sock_retry(manager_socket_name, -1, -1);
  } else {
    result->manager_conn = -1;
  }
  result->builder = make_protocol_builder();
  result->config.release_delay = release_delay;
  result->in_use_object_bytes = 0;
  /* Send a ConnectRequest to the store to get its memory capacity. */
  plasma_send_ConnectRequest(result->store_conn, result->builder);
  uint8_t *reply_data =
      plasma_receive(result->store_conn, MessageType_PlasmaConnectReply);
  plasma_read_ConnectReply(reply_data, &result->store_capacity);
  free(reply_data);
  return result;
}

void plasma_disconnect(PlasmaConnection *conn) {
  /* NOTE: We purposefully do not finish sending release calls for objects in
   * use, so that we don't duplicate plasma_release calls (when handling a
   * SIGTERM, for example). */
  for (auto &it : conn->objects_in_use) {
    delete it.second;
  }
  conn->objects_in_use.clear();
  for (auto &it : conn->mmap_table) {
    delete it.second;
  }
  conn->mmap_table.clear();
  free_protocol_builder(conn->builder);
  /* Close the connections to Plasma. The Plasma store will release the objects
   * that were in use by us when handling the SIGPIPE. */
  close(conn->store_conn);
  if (conn->manager_conn >= 0) {
    close(conn->manager_conn);
  }
  delete conn;
}

bool plasma_manager_is_connected(PlasmaConnection *conn) {
  return conn->manager_conn >= 0;
}

#define h_addr h_addr_list[0]

void plasma_transfer(PlasmaConnection *conn,
                     const char *address,
                     int port,
                     ObjectID object_id) {
  CHECK(plasma_send_DataRequest(conn->manager_conn, conn->builder, object_id,
                                address, port) >= 0);
}

void plasma_fetch(PlasmaConnection *conn,
                  int num_object_ids,
                  ObjectID object_ids[]) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);
  CHECK(plasma_send_FetchRequest(conn->manager_conn, conn->builder, object_ids,
                                 num_object_ids) >= 0);
}

int get_manager_fd(PlasmaConnection *conn) {
  return conn->manager_conn;
}

int plasma_status(PlasmaConnection *conn, ObjectID object_id) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);

  plasma_send_StatusRequest(conn->manager_conn, conn->builder, &object_id, 1);
  uint8_t *reply_data =
      plasma_receive(conn->manager_conn, MessageType_PlasmaStatusReply);
  int object_status;
  plasma_read_StatusReply(reply_data, &object_id, &object_status, 1);
  free(reply_data);
  return object_status;
}

int plasma_wait(PlasmaConnection *conn,
                int num_object_requests,
                ObjectRequest object_requests[],
                int num_ready_objects,
                uint64_t timeout_ms) {
  CHECK(conn != NULL);
  CHECK(conn->manager_conn >= 0);
  CHECK(num_object_requests > 0);
  CHECK(num_ready_objects > 0);
  CHECK(num_ready_objects <= num_object_requests);

  for (int i = 0; i < num_object_requests; ++i) {
    CHECK(object_requests[i].type == PLASMA_QUERY_LOCAL ||
          object_requests[i].type == PLASMA_QUERY_ANYWHERE);
  }

  CHECK(plasma_send_WaitRequest(conn->manager_conn, conn->builder,
                                object_requests, num_object_requests,
                                num_ready_objects, timeout_ms) >= 0);
  uint8_t *reply_data =
      plasma_receive(conn->manager_conn, MessageType_PlasmaWaitReply);
  plasma_read_WaitReply(reply_data, object_requests, &num_ready_objects);
  free(reply_data);

  int num_objects_ready = 0;
  for (int i = 0; i < num_object_requests; ++i) {
    int type = object_requests[i].type;
    int status = object_requests[i].status;
    switch (type) {
    case PLASMA_QUERY_LOCAL:
      if (status == ObjectStatus_Local) {
        num_objects_ready += 1;
      }
      break;
    case PLASMA_QUERY_ANYWHERE:
      if (status == ObjectStatus_Local || status == ObjectStatus_Remote) {
        num_objects_ready += 1;
      } else {
        CHECK(status == ObjectStatus_Nonexistent);
      }
      break;
    default:
      LOG_FATAL("This code should be unreachable.");
    }
  }
  return num_objects_ready;
}
