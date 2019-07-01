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
      new ray::WorkerContext(static_cast<ray::WorkerType>(workerType),
                             UniqueIdFromJByteArray<ray::JobID>(env, jobId).GetId()));
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentTaskId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentTaskId(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  auto task_id = GetWorkerContext(nativeWorkerContext)->GetCurrentTaskID();
  return JByteArrayFromUniqueId<ray::TaskID>(env, task_id).GetJByteArray();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeSetCurrentTask
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerContext_nativeSetCurrentTask(
    JNIEnv *env, jclass, jlong nativeWorkerContext, jobject taskBuff, jint pos,
    jint taskSize) {
  auto data = reinterpret_cast<uint8_t *>(env->GetDirectBufferAddress(taskBuff)) + pos;
  ray::raylet::TaskSpecification spec(data, taskSize);
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
  flatbuffers::FlatBufferBuilder fbb;
  auto message = spec->ToFlatbuffer(fbb);
  fbb.Finish(message);
  auto task_message = flatbuffers::GetRoot<flatbuffers::String>(fbb.GetBufferPointer());

  jbyteArray result = env->NewByteArray(task_message->size());
  env->SetByteArrayRegion(
      result, 0, task_message->size(),
      reinterpret_cast<jbyte *>(const_cast<char *>(task_message->data())));
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
  return JByteArrayFromUniqueId<ray::JobID>(env, job_id).GetJByteArray();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass, jlong nativeWorkerContext) {
  auto worker_id = GetWorkerContext(nativeWorkerContext)->GetWorkerID();
  return JByteArrayFromUniqueId<ray::WorkerID>(env, worker_id).GetJByteArray();
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

#ifdef __cplusplus
}
#endif
