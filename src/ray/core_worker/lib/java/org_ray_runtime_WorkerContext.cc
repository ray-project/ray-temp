#include "ray/core_worker/lib/java/org_ray_runtime_WorkerContext.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/lib/java/jni_helper.h"

inline ray::WorkerContext *GetWorkerContext(jlong nativeWorkerContext) {
  return reinterpret_cast<ray::WorkerContext *>(nativeWorkerContext);
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeCreateWorkerContext
 * Signature: (I[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_WorkerContext_nativeCreateWorkerContext(
    JNIEnv *env, jclass, jint workerType, jbyteArray jobId) {
  return reinterpret_cast<jlong>(
      new ray::WorkerContext(static_cast<ray::rpc::WorkerType>(workerType),
                             JavaByteArrayToUniqueId<ray::JobID>(env, jobId)));
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentTaskId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentTaskId(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  auto task_id = GetWorkerContext(nativeWorkerContext)->GetCurrentTaskID();
  return UniqueIDToJavaByteArray<ray::TaskID>(env, task_id);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeSetCurrentTask
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerContext_nativeSetCurrentTask(
    JNIEnv *env, jclass, jlong nativeWorkerContext, jbyteArray taskSpec) {
  jbyte *data = env->GetByteArrayElements(taskSpec, NULL);
  jsize size = env->GetArrayLength(taskSpec);
  ray::rpc::TaskSpec task_spec_message;
  task_spec_message.ParseFromArray(data, size);
  env->ReleaseByteArrayElements(taskSpec, data, JNI_ABORT);

  ray::TaskSpecification spec(task_spec_message);
  GetWorkerContext(nativeWorkerContext)->SetCurrentTask(spec);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentTask(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  auto spec = GetWorkerContext(nativeWorkerContext)->GetCurrentTask();
  if (!spec) {
    return nullptr;
  }

  auto task_message = spec->Serialize();
  jbyteArray result = env->NewByteArray(task_message.size());
  env->SetByteArrayRegion(
      result, 0, task_message.size(),
      reinterpret_cast<jbyte *>(const_cast<char *>(task_message.data())));
  return result;
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentJobId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentJobId(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  auto job_id = GetWorkerContext(nativeWorkerContext)->GetCurrentJobID();
  return UniqueIDToJavaByteArray<ray::JobID>(env, job_id);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  auto worker_id = GetWorkerContext(nativeWorkerContext)->GetWorkerID();
  return UniqueIDToJavaByteArray<ray::WorkerID>(env, worker_id);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetNextTaskIndex
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_WorkerContext_nativeGetNextTaskIndex(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  return GetWorkerContext(nativeWorkerContext)->GetNextTaskIndex();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetNextPutIndex
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_WorkerContext_nativeGetNextPutIndex(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  return GetWorkerContext(nativeWorkerContext)->GetNextPutIndex();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerContext_nativeDestroy(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  delete GetWorkerContext(nativeWorkerContext);
}

#ifdef __cplusplus
}
#endif
