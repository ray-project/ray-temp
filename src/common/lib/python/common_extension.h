#ifndef COMMON_EXTENSION_H
#define COMMON_EXTENSION_H

#include <Python.h>
#include "marshal.h"
#include "structmember.h"

#include "common.h"

typedef uint8_t task_spec;
struct TaskBuilder;

extern PyObject *CommonError;

// clang-format off
typedef struct {
  PyObject_HEAD
  ObjectID object_id;
} PyObjectID;

typedef struct {
  PyObject_HEAD
  int64_t size;
  task_spec *spec;
} PyTask;
// clang-format on

extern PyTypeObject PyObjectIDType;

extern PyTypeObject PyTaskType;

/* Python module for pickling. */
extern PyObject *pickle_module;
extern PyObject *pickle_dumps;
extern PyObject *pickle_loads;

void init_pickle_module(void);

extern TaskBuilder *g_task_builder;

int PyStringToUniqueID(PyObject *object, ObjectID *object_id);

int PyObjectToUniqueID(PyObject *object, ObjectID *object_id);

PyObject *PyObjectID_make(ObjectID object_id);

PyObject *check_simple_value(PyObject *self, PyObject *args);

PyObject *PyTask_to_string(PyObject *, PyObject *args);
PyObject *PyTask_from_string(PyObject *, PyObject *args);

PyObject *compute_put_id(PyObject *self, PyObject *args);

PyObject *PyTask_make(task_spec *task_spec);

#endif /* COMMON_EXTENSION_H */
