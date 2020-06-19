// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class io_ray_runtime_RayNativeRuntime */

#ifndef _Included_io_ray_runtime_RayNativeRuntime
#define _Included_io_ray_runtime_RayNativeRuntime
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeInitialize
 * Signature:
 * (ILjava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;[BLio/ray/runtime/gcs/GcsClientOptions;ILjava/lang/String;Ljava/util/Map;)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeInitialize(
    JNIEnv *, jclass, jint, jstring, jint, jstring, jstring, jstring, jbyteArray, jobject,
    jint, jstring, jobject);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeRunTaskExecutor
 * Signature: (Lio/ray/runtime/task/TaskExecutor;)V
 */
JNIEXPORT void JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeRunTaskExecutor(JNIEnv *, jclass, jobject);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeShutdown
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeShutdown(JNIEnv *,
                                                                           jclass);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeSetResource
 * Signature: (Ljava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetResource(
    JNIEnv *, jclass, jstring, jdouble, jbyteArray);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeKillActor
 * Signature: ([BZ)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeKillActor(JNIEnv *,
                                                                            jclass,
                                                                            jbyteArray,
                                                                            jboolean);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeGetActorIdOfNamedActor
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeGetActorIdOfNamedActor(
    JNIEnv *, jclass, jstring);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeSetCoreWorker
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeSetCoreWorker(JNIEnv *, jclass, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif
