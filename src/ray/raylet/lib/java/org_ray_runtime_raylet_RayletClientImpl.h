/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_ray_runtime_raylet_RayletClientImpl */

#ifndef _Included_org_ray_runtime_raylet_RayletClientImpl
#define _Included_org_ray_runtime_raylet_RayletClientImpl
#ifdef __cplusplus
extern "C" {
#endif
#undef org_ray_runtime_raylet_RayletClientImpl_TASK_SPEC_BUFFER_SIZE
#define org_ray_runtime_raylet_RayletClientImpl_TASK_SPEC_BUFFER_SIZE 2097152L
/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeInit
 * Signature: (Ljava/lang/String;[BZ[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeInit(
    JNIEnv *, jclass, jstring, jbyteArray, jboolean, jbyteArray);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSubmitTask
 * Signature: (J[BLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSubmitTask(
    JNIEnv *, jclass, jlong, jbyteArray, jobject, jint, jint);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGetTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeGetTask(JNIEnv *, jclass, jlong);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeDestroy(JNIEnv *, jclass, jlong);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeFetchOrReconstruct
 * Signature: (J[[BZ[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeFetchOrReconstruct(JNIEnv *, jclass,
                                                                      jlong, jobjectArray,
                                                                      jboolean,
                                                                      jbyteArray);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyUnblocked
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyUnblocked(
    JNIEnv *, jclass, jlong, jbyteArray);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativePutObject
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativePutObject(
    JNIEnv *, jclass, jlong, jbyteArray, jbyteArray);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeWaitObject
 * Signature: (J[[BIIZ[B)[Z
 */
JNIEXPORT jbooleanArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeWaitObject(JNIEnv *, jclass, jlong,
                                                              jobjectArray, jint, jint,
                                                              jboolean, jbyteArray);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGenerateTaskId
 * Signature: ([B[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeGenerateTaskId(JNIEnv *, jclass,
                                                                  jbyteArray, jbyteArray,
                                                                  jint);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeFreePlasmaObjects
 * Signature: (J[[BZ)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeFreePlasmaObjects(JNIEnv *, jclass,
                                                                     jlong, jobjectArray,
                                                                     jboolean);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativePrepareCheckpoint
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativePrepareCheckpoint(JNIEnv *, jclass,
                                                                     jlong, jbyteArray);

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyActorResumedFromCheckpoint
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *, jclass, jlong, jbyteArray, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif
