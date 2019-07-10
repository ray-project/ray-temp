#include "ray/core_worker/lib/java/org_ray_runtime_objectstore_ObjectInterface.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/object_interface.h"

inline ray::CoreWorkerObjectInterface *GetObjectInterface(jlong nativeObjectInterface) {
  return reinterpret_cast<ray::CoreWorkerObjectInterface *>(nativeObjectInterface);
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterface
 * Method:    nativeCreateObjectInterface
 * Signature: (JJLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_org_ray_runtime_objectstore_ObjectInterface_nativeCreateObjectInterface(
    JNIEnv *env, jclass, jlong nativeWorkerContext, jlong nativeRayletClient,
    jstring storeSocketName) {
  return reinterpret_cast<jlong>(new ray::CoreWorkerObjectInterface(
      *reinterpret_cast<ray::WorkerContext *>(nativeWorkerContext),
      *reinterpret_cast<std::unique_ptr<RayletClient> *>(nativeRayletClient),
      JavaStringToNativeString(env, storeSocketName)));
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterface
 * Method:    nativePut
 * Signature: (JLorg/ray/runtime/objectstore/RayObjectProxy;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_objectstore_ObjectInterface_nativePut__JLorg_ray_runtime_objectstore_RayObjectProxy_2(
    JNIEnv *env, jclass, jlong nativeObjectInterface, jobject obj) {
  ray::Status status;
  ray::ObjectID object_id = ReadJavaRayObjectProxy<ray::ObjectID>(
      env, obj,
      [nativeObjectInterface, &status](const std::shared_ptr<ray::RayObject> &rayObject) {
        RAY_CHECK(rayObject != nullptr);
        ray::ObjectID object_id;
        status = GetObjectInterface(nativeObjectInterface)->Put(*rayObject, &object_id);
        return object_id;
      });
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }
  return UniqueIDToJavaByteArray<ray::ObjectID>(env, object_id);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterface
 * Method:    nativePut
 * Signature: (J[BLorg/ray/runtime/objectstore/RayObjectProxy;)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_objectstore_ObjectInterface_nativePut__J_3BLorg_ray_runtime_objectstore_RayObjectProxy_2(
    JNIEnv *env, jclass, jlong nativeObjectInterface, jbyteArray objectId, jobject obj) {
  auto object_id = JavaByteArrayToUniqueId<ray::ObjectID>(env, objectId);
  auto status = ReadJavaRayObjectProxy<ray::Status>(
      env, obj,
      [nativeObjectInterface,
       &object_id](const std::shared_ptr<ray::RayObject> &rayObject) {
        RAY_CHECK(rayObject != nullptr);
        return GetObjectInterface(nativeObjectInterface)->Put(*rayObject, object_id);
      });
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterface
 * Method:    nativeGet
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_objectstore_ObjectInterface_nativeGet(
    JNIEnv *env, jclass, jlong nativeObjectInterface, jobject ids, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToUniqueId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status = GetObjectInterface(nativeObjectInterface)
                    ->Get(object_ids, (int64_t)timeoutMs, &results);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }
  return NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(env, results,
                                                                 ToJavaRayObjectProxy);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterface
 * Method:    nativeWait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_objectstore_ObjectInterface_nativeWait(
    JNIEnv *env, jclass, jlong nativeObjectInterface, jobject objectIds, jint numObjects,
    jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToUniqueId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<bool> results;
  auto status = GetObjectInterface(nativeObjectInterface)
                    ->Wait(object_ids, (int)numObjects, (int64_t)timeoutMs, &results);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    return env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
  });
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterface
 * Method:    nativeDelete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_objectstore_ObjectInterface_nativeDelete(
    JNIEnv *env, jclass, jlong nativeObjectInterface, jobject objectIds,
    jboolean localOnly, jboolean deleteCreatingTasks) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToUniqueId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  auto status = GetObjectInterface(nativeObjectInterface)
                    ->Delete(object_ids, (bool)localOnly, (bool)deleteCreatingTasks);
  ThrowRayExceptionIfNotOK(env, status);
}

#ifdef __cplusplus
}
#endif
