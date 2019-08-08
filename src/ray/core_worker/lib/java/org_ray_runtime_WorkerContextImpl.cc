#include "ray/core_worker/lib/java/org_ray_runtime_WorkerContextImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

inline ray::WorkerContext &GetWorkerContextFromPointer(jlong nativeCoreWorkerPointer) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->Context();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentTaskType
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentTaskType(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  auto task_spec = GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentTask();
  RAY_CHECK(task_spec) << "Current task is not set.";
  return static_cast<int>(task_spec->GetMessage().type());
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentTaskId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentTaskId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const ray::TaskID &task_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentTaskID();
  return IdToJavaByteBuffer<ray::TaskID>(env, task_id);
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentJobId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentJobId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &job_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentJobID();
  return IdToJavaByteBuffer<ray::JobID>(env, job_id);
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &worker_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetWorkerID();
  return IdToJavaByteBuffer<ray::WorkerID>(env, worker_id);
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentActorId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentActorId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &actor_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentActorID();
  return IdToJavaByteBuffer<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
