/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_ray_runtime_WorkerContext */

#ifndef _Included_org_ray_runtime_WorkerContext
#define _Included_org_ray_runtime_WorkerContext
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentJobId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_WorkerContext_nativeGetCurrentJobId(JNIEnv *, jclass, jlong);

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_WorkerContext_nativeGetCurrentWorkerId(JNIEnv *, jclass, jlong);

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentActorId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_WorkerContext_nativeGetCurrentActorId(JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif
