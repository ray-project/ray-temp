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
 * Method:    put
 * Signature: (JLorg/ray/runtime/proxyTypes/RayObjectValueProxy;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_ObjectInterface_put__JLorg_ray_runtime_proxyTypes_RayObjectValueProxy_2(
    JNIEnv *, jclass, jlong, jobject);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    put
 * Signature: (J[BLorg/ray/runtime/proxyTypes/RayObjectValueProxy;)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_ObjectInterface_put__J_3BLorg_ray_runtime_proxyTypes_RayObjectValueProxy_2(
    JNIEnv *, jclass, jlong, jbyteArray, jobject);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    get
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_get(JNIEnv *, jclass,
                                                                   jlong, jobject, jlong);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    wait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_wait(JNIEnv *, jclass,
                                                                    jlong, jobject, jint,
                                                                    jlong);

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    delete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_ObjectInterface_delete(JNIEnv *, jclass,
                                                                   jlong, jobject,
                                                                   jboolean, jboolean);

#ifdef __cplusplus
}
#endif
#endif
