/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_ray_runtime_ObjectInterface */

#ifndef _Included_org_ray_runtime_ObjectInterface
#define _Included_org_ray_runtime_ObjectInterface
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativePut
 * Signature: (JLorg/ray/runtime/proxyTypes/RayObjectProxy;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_ObjectInterface_nativePut__JLorg_ray_runtime_proxyTypes_RayObjectProxy_2(
    JNIEnv *, jclass, jlong, jobject);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativePut
 * Signature: (J[BLorg/ray/runtime/proxyTypes/RayObjectProxy;)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_ObjectInterface_nativePut__J_3BLorg_ray_runtime_proxyTypes_RayObjectProxy_2(
    JNIEnv *, jclass, jlong, jbyteArray, jobject);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativeGet
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_nativeGet(JNIEnv *, jclass,
                                                                         jlong, jobject,
                                                                         jlong);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativeWait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_nativeWait(JNIEnv *,
                                                                          jclass, jlong,
                                                                          jobject, jint,
                                                                          jlong);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativeGelete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_ObjectInterface_nativeDelete(JNIEnv *, jclass,
                                                                         jlong, jobject,
                                                                         jboolean,
                                                                         jboolean);

#ifdef __cplusplus
}
#endif
#endif
