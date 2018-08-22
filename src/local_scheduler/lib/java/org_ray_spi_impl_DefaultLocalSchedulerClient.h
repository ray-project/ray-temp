/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_ray_spi_impl_DefaultLocalSchedulerClient */

#ifndef _Included_org_ray_spi_impl_DefaultLocalSchedulerClient
#define _Included_org_ray_spi_impl_DefaultLocalSchedulerClient
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _init
 * Signature: (Ljava/lang/String;[B[BZJ)J
 */
JNIEXPORT jlong JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1init(JNIEnv *,
                                                         jclass,
                                                         jstring,
                                                         jbyteArray,
                                                         jbyteArray,
                                                         jboolean,
                                                         jbyteArray,
                                                         jlong,
                                                         jboolean);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _submitTask
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1submitTask(JNIEnv *,
                                                               jclass,
                                                               jlong,
                                                               jbyteArray,
                                                               jobject,
                                                               jint,
                                                               jint,
                                                               jboolean);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _getTaskTodo
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1getTaskTodo(JNIEnv *,
                                                                jclass,
                                                                jlong,
                                                                jboolean);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _computePutId
 * Signature: (J[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1computePutId(JNIEnv *,
                                                                 jclass,
                                                                 jlong,
                                                                 jbyteArray,
                                                                 jint);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _destroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1destroy(JNIEnv *,
                                                            jclass,
                                                            jlong);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _task_done
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1task_1done(JNIEnv *,
                                                               jclass,
                                                               jlong);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _reconstruct_objects
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1reconstruct_1objects(
    JNIEnv *,
    jclass,
    jlong,
    jobjectArray,
    jboolean);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _notify_unblocked
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1notify_1unblocked(JNIEnv *,
                                                                      jclass,
                                                                      jlong);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _put_object
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1put_1object(JNIEnv *,
                                                                jclass,
                                                                jlong,
                                                                jbyteArray,
                                                                jbyteArray);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _waitObject
 * Signature: (J[[BIIZ)[Z
 */
JNIEXPORT jbooleanArray JNICALL
    Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1waitObject(JNIEnv *,
                                                                   jclass,
                                                                   jlong,
                                                                   jobjectArray,
                                                                   jint,
                                                                   jint,
                                                                   jboolean);

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _computeTaskId
 * Signature: ([B[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
    Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1computeTaskId(JNIEnv *,
                                                                      jclass,
                                                                      jbyteArray, jbyteArray,
                                                                      jint);

#ifdef __cplusplus
}
#endif
#endif
