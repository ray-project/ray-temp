#include "photon_algorithm.h"

#include <stdbool.h>
#include "utarray.h"
#include "utlist.h"

#include "state/task_table.h"
#include "state/local_scheduler_table.h"
#include "state/object_table.h"
#include "photon.h"
#include "photon_scheduler.h"

typedef struct task_queue_entry {
  /** The task that is queued. */
  task_spec *spec;
  struct task_queue_entry *prev;
  struct task_queue_entry *next;
} task_queue_entry;

/** A data structure used to track which objects are available locally and
 *  which objects are being actively fetched. */
typedef struct {
  /** Object id of this object. */
  object_id object_id;
  /** Handle for the uthash table. NOTE: This handle is used for both the
   *  scheduling algorithm state's local_objects and fetch_requests tables.
   *  We must reinforce the uthash invariant that the entry be in either one or
   *  neither tables. */
  UT_hash_handle hh;
} object_entry;

UT_icd task_queue_entry_icd = {sizeof(task_queue_entry *), NULL, NULL, NULL};

typedef struct {
  /** Object id of this object. */
  object_id object_id;
  UT_array *dependent_tasks;
  /** hh for the uthash table. */
  UT_hash_handle dependency_handle;
} object_dependency;

/** Part of the photon state that is maintained by the scheduling algorithm. */
struct scheduling_algorithm_state {
  /** An array of pointers to tasks that are waiting for dependencies. */
  task_queue_entry *waiting_task_queue;
  /** An array of pointers to tasks whose dependencies are ready but that are
   *  waiting to be assigned to a worker. */
  task_queue_entry *dispatch_task_queue;
  /** An array of worker indices corresponding to clients that are
   *  waiting for tasks. */
  UT_array *available_workers;
  /** A hash map of the objects that are available in the local Plasma store.
   *  The key is the object ID. This information could be a little stale. */
  object_entry *local_objects;
  /** A hash map of the objects that are currently being fetched by this local
   *  scheduler. The key is the object ID. Every
   *  LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS, a Plasma fetch request will
   *  be sent for every object ID in this table. */
  object_entry *fetch_requests;
  /** A hash map mapping object ID to queued tasks that are dependent on it.
   *  Objects in this hash table are not yet available locally and should be
   *  actively fetched. */
  object_dependency *object_dependencies;
};

scheduling_algorithm_state *make_scheduling_algorithm_state(void) {
  scheduling_algorithm_state *algorithm_state =
      malloc(sizeof(scheduling_algorithm_state));
  /* Initialize an empty hash map for the cache of local available objects. */
  algorithm_state->local_objects = NULL;
  algorithm_state->object_dependencies = NULL;
  /* Initialize the local data structures used for queuing tasks and workers. */
  algorithm_state->waiting_task_queue = NULL;
  algorithm_state->dispatch_task_queue = NULL;
  utarray_new(algorithm_state->available_workers, &ut_int_icd);
  /* Initialize the hash table of objects being fetched. */
  algorithm_state->fetch_requests = NULL;
  return algorithm_state;
}

void free_scheduling_algorithm_state(
    scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt, *tmp1;
  DL_FOREACH_SAFE(algorithm_state->waiting_task_queue, elt, tmp1) {
    DL_DELETE(algorithm_state->waiting_task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  DL_FOREACH_SAFE(algorithm_state->dispatch_task_queue, elt, tmp1) {
    DL_DELETE(algorithm_state->dispatch_task_queue, elt);
    free_task_spec(elt->spec);
    free(elt);
  }
  utarray_free(algorithm_state->available_workers);
  object_entry *available_obj, *tmp2;
  HASH_ITER(hh, algorithm_state->local_objects, available_obj, tmp2) {
    HASH_DELETE(hh, algorithm_state->local_objects, available_obj);
    free(available_obj);
  }
  object_dependency *dependency, *tmp_dependency;
  HASH_ITER(dependency_handle, algorithm_state->object_dependencies, dependency,
            tmp_dependency) {
    HASH_DELETE(dependency_handle, algorithm_state->object_dependencies,
                dependency);
    utarray_free(dependency->dependent_tasks);
    free(dependency);
  }
  object_entry *fetch_elt, *tmp_fetch_elt;
  HASH_ITER(hh, algorithm_state->fetch_requests, fetch_elt, tmp_fetch_elt) {
    HASH_DELETE(hh, algorithm_state->fetch_requests, fetch_elt);
    free(fetch_elt);
  }
  free(algorithm_state);
}

void provide_scheduler_info(local_scheduler_state *state,
                            scheduling_algorithm_state *algorithm_state,
                            local_scheduler_info *info) {
  task_queue_entry *elt;
  info->total_num_workers = utarray_len(state->workers);
  /* TODO(swang): Provide separate counts for tasks that are waiting for
   * dependencies vs tasks that are waiting to be assigned. */
  int waiting_task_queue_length;
  DL_COUNT(algorithm_state->waiting_task_queue, elt, waiting_task_queue_length);
  int dispatch_task_queue_length;
  DL_COUNT(algorithm_state->dispatch_task_queue, elt,
           dispatch_task_queue_length);
  info->task_queue_length =
      waiting_task_queue_length + dispatch_task_queue_length;
  info->available_workers = utarray_len(algorithm_state->available_workers);
}

/**
 * Record a queued task's missing object dependency.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param task_entry The task's queue entry.
 * @param obj_id The ID of the object that the task is dependent on.
 * @returns Void.
 */
void add_task_dependency(local_scheduler_state *state,
                         scheduling_algorithm_state *algorithm_state,
                         task_queue_entry *task_entry,
                         object_id obj_id) {
  object_dependency *entry;
  HASH_FIND(dependency_handle, algorithm_state->object_dependencies, &obj_id,
            sizeof(obj_id), entry);
  if (entry == NULL) {
    entry = malloc(sizeof(object_dependency));
    entry->object_id = obj_id;
    utarray_new(entry->dependent_tasks, &task_queue_entry_icd);
    HASH_ADD(dependency_handle, algorithm_state->object_dependencies, object_id,
             sizeof(entry->object_id), entry);
  }
  utarray_push_back(entry->dependent_tasks, &task_entry);
}

/**
 * Record a queued task's missing object dependencies.  The given task should
 * have at least one unsatisfied dependency.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param task_entry The task's queue entry.
 * @returns Void.
 */
void add_task_dependencies(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_queue_entry *task_entry) {
  task_spec *task = task_entry->spec;
  int64_t num_args = task_num_args(task);
  bool has_dependency = false;
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(task, i) == ARG_BY_REF) {
      object_id obj_id = task_arg_id(task, i);
      object_entry *entry;
      HASH_FIND(hh, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
                entry);
      if (entry == NULL) {
        /* If the entry is not yet available locally, record the dependency. */
        add_task_dependency(state, algorithm_state, task_entry, obj_id);
        has_dependency = true;
      }
    }
  }
  CHECK(has_dependency);
}

/**
 * Check if all of the remote object arguments for a task are available in the
 * local object store.
 *
 * @param algorithm_state The scheduling algorithm state.
 * @param task Task specification of the task to check.
 * @return bool This returns true if all of the remote object arguments for the
 *         task are present in the local object store, otherwise it returns
 *         false.
 */
bool can_run(scheduling_algorithm_state *algorithm_state, task_spec *task) {
  int64_t num_args = task_num_args(task);
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(task, i) == ARG_BY_REF) {
      object_id obj_id = task_arg_id(task, i);
      object_entry *entry;
      HASH_FIND(hh, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
                entry);
      if (entry == NULL) {
        /* The object is not present locally, so this task cannot be scheduled
         * right now. */
        return false;
      }
    }
  }
  return true;
}

/* TODO(rkn): This method will need to be changed to call reconstruct. */
int fetch_object_timeout_handler(event_loop *loop, timer_id id, void *context) {
  local_scheduler_state *state = context;
  object_entry *fetch_request, *tmp;
  HASH_ITER(hh, state->algorithm_state->fetch_requests, fetch_request, tmp) {
    plasma_fetch(state->plasma_conn, 1, &fetch_request->object_id);
  }
  return LOCAL_SCHEDULER_FETCH_TIMEOUT_MILLISECONDS;
}

void fetch_missing_dependencies(local_scheduler_state *state,
                                scheduling_algorithm_state *algorithm_state,
                                task_spec *spec) {
  int64_t num_args = task_num_args(spec);
  for (int i = 0; i < num_args; ++i) {
    if (task_arg_type(spec, i) == ARG_BY_REF) {
      object_id obj_id = task_arg_id(spec, i);
      object_entry *entry;
      HASH_FIND(hh, algorithm_state->local_objects, &obj_id, sizeof(obj_id),
                entry);
      if (entry == NULL) {
        /* The object is not present locally, fetch the object. */
        plasma_fetch(state->plasma_conn, 1, &obj_id);
        /* Create an entry and add it to the list of active fetch requests to
         * ensure that the fetch actually happens. */
        object_entry *fetch_req = malloc(sizeof(object_entry));
        fetch_req->object_id = obj_id;
        /* The fetch request will be moved to the hash table of locally
         * available objects in
         * handle_object_available when the object becomes available locally.
         * It will get freed if the object is subsequently removed locally. */
        HASH_ADD(hh, algorithm_state->fetch_requests, object_id,
                 sizeof(fetch_req->object_id), fetch_req);
      }
    }
  }
}

/**
 * Assign as many tasks from the dispatch queue as possible.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @return Void.
 */
void dispatch_tasks(local_scheduler_state *state,
                    scheduling_algorithm_state *algorithm_state) {
  /* Assign tasks while there are still tasks in the dispatch queue and
   * available workers. */
  while ((algorithm_state->dispatch_task_queue != NULL) &&
         (utarray_len(algorithm_state->available_workers) > 0)) {
    LOG_DEBUG("Dispatching task");
    /* Pop a task from the dispatch queue. */
    task_queue_entry *dispatched_task = algorithm_state->dispatch_task_queue;
    DL_DELETE(algorithm_state->dispatch_task_queue, dispatched_task);

    /* Get the last available worker in the available worker queue. */
    int *worker_index =
        (int *) utarray_back(algorithm_state->available_workers);
    /* Tell the available worker to execute the task. */
    assign_task_to_worker(state, dispatched_task->spec, *worker_index);
    /* Remove the available worker from the queue and free the struct. */
    utarray_pop_back(algorithm_state->available_workers);
    free_task_spec(dispatched_task->spec);
    free(dispatched_task);
  }
}

/**
 * A helper function to allocate a queue entry for a task specification and
 * push it onto a generic queue.
 *
 * @param state The state of the local scheduler.
 * @param task_queue A pointer to a task queue. NOTE: Because we are using
 *        utlist.h, we must pass in a pointer to the queue we want to append
 *        to. If we passed in the queue itself and the queue was empty, this
 *        would append the task to a queue that we don't have a reference to.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
task_queue_entry *queue_task(local_scheduler_state *state,
                             task_queue_entry **task_queue,
                             task_spec *spec,
                             bool from_global_scheduler) {
  /* Copy the spec and add it to the task queue. The allocated spec will be
   * freed when it is assigned to a worker. */
  task_queue_entry *elt = malloc(sizeof(task_queue_entry));
  elt->spec = (task_spec *) malloc(task_spec_size(spec));
  memcpy(elt->spec, spec, task_spec_size(spec));
  DL_APPEND((*task_queue), elt);

  /* The task has been added to a local scheduler queue. Write the entry in the
   * task table to notify others that we have queued it. */
  if (state->db != NULL) {
    task *task =
        alloc_task(spec, TASK_STATUS_QUEUED, get_db_client_id(state->db));
    if (from_global_scheduler) {
      /* If the task is from the global scheduler, it's already been added to
       * the task table, so just update the entry. */
      task_table_update(state->db, task, (retry_info *) &photon_retry, NULL,
                        NULL);
    } else {
      /* Otherwise, this is the first time the task has been seen in the system
       * (unless it's a resubmission of a previous task), so add the entry. */
      task_table_add_task(state->db, task, (retry_info *) &photon_retry, NULL,
                          NULL);
    }
  }

  return elt;
}

/**
 * Queue a task whose dependencies are missing. When the task's object
 * dependencies become available, the task will be moved to the dispatch queue.
 * If we have a connection to a plasma manager, begin trying to fetch the
 * dependencies.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
void queue_waiting_task(local_scheduler_state *state,
                        scheduling_algorithm_state *algorithm_state,
                        task_spec *spec,
                        bool from_global_scheduler) {
  LOG_DEBUG("Queueing task in waiting queue");
  /* Initiate fetch calls for any dependencies that are not present locally. */
  if (plasma_manager_is_connected(state->plasma_conn)) {
    fetch_missing_dependencies(state, algorithm_state, spec);
  }
  task_queue_entry *task_entry = queue_task(
      state, &algorithm_state->waiting_task_queue, spec, from_global_scheduler);
  /* If we're queueing this task in the waiting queue, there must be at least
   * one missing dependency, so record it. */
  add_task_dependencies(state, algorithm_state, task_entry);
}

/**
 * Queue a task whose dependencies are ready. When the task reaches the front
 * of the dispatch queue and workers are available, it will be assigned.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
void queue_dispatch_task(local_scheduler_state *state,
                         scheduling_algorithm_state *algorithm_state,
                         task_spec *spec,
                         bool from_global_scheduler) {
  LOG_DEBUG("Queueing task in dispatch queue");
  queue_task(state, &algorithm_state->dispatch_task_queue, spec,
             from_global_scheduler);
}

/**
 * Add the task to the proper local scheduler queue. This assumes that the
 * scheduling decision to place the task on this node has already been made,
 * whether locally or by the global scheduler.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to queue.
 * @param from_global_scheduler Whether or not the task was from a global
 *        scheduler. If false, the task was submitted by a worker.
 * @return Void.
 */
void queue_task_locally(local_scheduler_state *state,
                        scheduling_algorithm_state *algorithm_state,
                        task_spec *spec,
                        bool from_global_scheduler) {
  if (can_run(algorithm_state, spec)) {
    /* Dependencies are ready, so push the task to the dispatch queue. */
    queue_dispatch_task(state, algorithm_state, spec, from_global_scheduler);
  } else {
    /* Dependencies are not ready, so push the task to the waiting queue. */
    queue_waiting_task(state, algorithm_state, spec, from_global_scheduler);
  }
}

/**
 * Give a task to the global scheduler to schedule.
 *
 * @param state The scheduler state.
 * @param algorithm_state The scheduling algorithm state.
 * @param spec The task specification to schedule.
 * @return Void.
 */
void give_task_to_global_scheduler(local_scheduler_state *state,
                                   scheduling_algorithm_state *algorithm_state,
                                   task_spec *spec) {
  if (state->db == NULL || !state->global_scheduler_exists) {
    /* A global scheduler is not available, so queue the task locally. */
    queue_task_locally(state, algorithm_state, spec, false);
    return;
  }
  /* Pass on the task to the global scheduler. */
  DCHECK(state->global_scheduler_exists);
  task *task = alloc_task(spec, TASK_STATUS_WAITING, NIL_ID);
  DCHECK(state->db != NULL);
  task_table_add_task(state->db, task, (retry_info *) &photon_retry, NULL,
                      NULL);
}

void handle_task_submitted(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec) {
  /* If this task's dependencies are available locally, and if there is an
   * available worker, then assign this task to an available worker. If we
   * cannot assign the task to a worker immediately, we either queue the task in
   * the local task queue or we pass the task to the global scheduler. For now,
   * we pass the task along to the global scheduler if there is one. */
  if (can_run(algorithm_state, spec) &&
      (utarray_len(algorithm_state->available_workers) > 0)) {
    /* Dependencies are ready and there is an available worker, so dispatch the
     * task. */
    queue_dispatch_task(state, algorithm_state, spec, false);
  } else {
    /* Give the task to the global scheduler to schedule, if it exists. */
    give_task_to_global_scheduler(state, algorithm_state, spec);
  }

  /* Try to dispatch tasks, since we may have added one to the queue. */
  dispatch_tasks(state, algorithm_state);

  /* Update the result table, which holds mappings of object ID -> ID of the
   * task that created it. */
  if (state->db != NULL) {
    task_id task_id = task_spec_id(spec);
    for (int64_t i = 0; i < task_num_returns(spec); ++i) {
      object_id return_id = task_return(spec, i);
      result_table_add(state->db, return_id, task_id,
                       (retry_info *) &photon_retry, NULL, NULL);
    }
  }
}

void handle_task_scheduled(local_scheduler_state *state,
                           scheduling_algorithm_state *algorithm_state,
                           task_spec *spec) {
  /* This callback handles tasks that were assigned to this local scheduler by
   * the global scheduler, so we can safely assert that there is a connection
   * to the database. */
  DCHECK(state->db != NULL);
  DCHECK(state->global_scheduler_exists);
  /* Push the task to the appropriate queue. */
  queue_task_locally(state, algorithm_state, spec, true);
  dispatch_tasks(state, algorithm_state);
}

void handle_worker_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             int worker_index) {
  worker *available_worker =
      (worker *) utarray_eltptr(state->workers, worker_index);
  CHECK(available_worker->task_in_progress == NULL);
  for (int *p = (int *) utarray_front(algorithm_state->available_workers);
       p != NULL;
       p = (int *) utarray_next(algorithm_state->available_workers, p)) {
    DCHECK(*p != worker_index);
  }
  /* Add worker to the list of available workers. */
  utarray_push_back(algorithm_state->available_workers, &worker_index);
  LOG_DEBUG("Adding worker_index %d to available workers", worker_index);

  /* Try to dispatch tasks, since we now have available workers to assign them
   * to. */
  dispatch_tasks(state, algorithm_state);
}

void handle_object_available(local_scheduler_state *state,
                             scheduling_algorithm_state *algorithm_state,
                             object_id object_id) {
  /* Get the entry for this object from the active fetch request, or allocate
   * one if needed. */
  object_entry *entry;
  HASH_FIND(hh, algorithm_state->fetch_requests, &object_id, sizeof(object_id),
            entry);
  if (entry != NULL) {
    /* Remove the object from the active fetch requests. */
    HASH_DELETE(hh, algorithm_state->fetch_requests, entry);
  } else {
    /* Allocate a new object entry. Object entries will get freed if the object
     * is removed. */
    entry = (object_entry *) malloc(sizeof(object_entry));
    entry->object_id = object_id;
  }

  /* Add the entry to the set of locally available objects. */
  HASH_ADD(hh, algorithm_state->local_objects, object_id, sizeof(object_id),
           entry);

  /* Move any tasks whose object dependencies are now ready to the dispatch
   * queue. */
  object_dependency *elt;
  HASH_FIND(dependency_handle, algorithm_state->object_dependencies, &object_id,
            sizeof(object_id), elt);
  if (elt != NULL) {
    HASH_DELETE(dependency_handle, algorithm_state->object_dependencies, elt);
    task_queue_entry *task_entry = NULL;
    for (task_queue_entry **p =
             (task_queue_entry **) utarray_front(elt->dependent_tasks);
         p != NULL;
         p = (task_queue_entry **) utarray_next(elt->dependent_tasks, p)) {
      task_queue_entry *task_entry = *p;
      if (can_run(algorithm_state, task_entry->spec)) {
        LOG_DEBUG("Moved task to dispatch queue");
        DL_DELETE(algorithm_state->waiting_task_queue, task_entry);
        DL_APPEND(algorithm_state->dispatch_task_queue, task_entry);
      }
    }
    utarray_free(elt->dependent_tasks);
    free(elt);

    /* Try to dispatch tasks, since we may have added some from the waiting
     * queue. */
    dispatch_tasks(state, algorithm_state);
  }
}

void handle_object_removed(local_scheduler_state *state,
                           object_id removed_object_id) {
  /* Remove the object from the set of locally available objects. */
  scheduling_algorithm_state *algorithm_state = state->algorithm_state;
  object_entry *entry;
  HASH_FIND(hh, algorithm_state->local_objects, &removed_object_id,
            sizeof(removed_object_id), entry);
  if (entry != NULL) {
    HASH_DELETE(hh, algorithm_state->local_objects, entry);
    free(entry);
  }

  /* Track queued tasks that were dependent on this object. */
  task_queue_entry *elt, *tmp;
  /* Track the dependency for tasks that were in the waiting queue. */
  DL_FOREACH(algorithm_state->waiting_task_queue, elt) {
    task_spec *task = elt->spec;
    int64_t num_args = task_num_args(task);
    for (int i = 0; i < num_args; ++i) {
      if (task_arg_type(task, i) == ARG_BY_REF) {
        object_id arg_id = task_arg_id(task, i);
        if (object_ids_equal(arg_id, removed_object_id)) {
          add_task_dependency(state, algorithm_state, elt, removed_object_id);
        }
      }
    }
  }
  /* Track the dependency for tasks that were in the dispatch queue. Remove
   * these tasks from the dispatch queue and push them to the waiting queue. */
  DL_FOREACH_SAFE(algorithm_state->dispatch_task_queue, elt, tmp) {
    task_spec *task = elt->spec;
    int64_t num_args = task_num_args(task);
    for (int i = 0; i < num_args; ++i) {
      if (task_arg_type(task, i) == ARG_BY_REF) {
        object_id arg_id = task_arg_id(task, i);
        if (object_ids_equal(arg_id, removed_object_id)) {
          LOG_DEBUG("Moved task from dispatch queue back to waiting queue");
          DL_DELETE(algorithm_state->dispatch_task_queue, elt);
          DL_APPEND(algorithm_state->waiting_task_queue, elt);
          add_task_dependency(state, algorithm_state, elt, removed_object_id);
        }
      }
    }
  }
}

int num_waiting_tasks(scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt;
  int count;
  DL_COUNT(algorithm_state->waiting_task_queue, elt, count);
  return count;
}

int num_dispatch_tasks(scheduling_algorithm_state *algorithm_state) {
  task_queue_entry *elt;
  int count;
  DL_COUNT(algorithm_state->dispatch_task_queue, elt, count);
  return count;
}
