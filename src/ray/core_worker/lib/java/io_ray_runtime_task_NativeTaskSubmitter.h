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
/* Header for class io_ray_runtime_task_NativeTaskSubmitter */

#ifndef _Included_io_ray_runtime_task_NativeTaskSubmitter
#define _Included_io_ray_runtime_task_NativeTaskSubmitter
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeSubmitTask
 * Signature:
 * (Lio/ray/runtime/functionmanager/FunctionDescriptor;ILjava/util/List;ILio/ray/api/options/CallOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitTask(
    JNIEnv *, jclass, jobject, jint, jobject, jint, jobject);

/*
 * Class:     io_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeCreateActor
 * Signature:
 * (Lio/ray/runtime/functionmanager/FunctionDescriptor;ILjava/util/List;Lio/ray/api/options/ActorCreationOptions;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreateActor(JNIEnv *, jclass, jobject,
                                                               jint, jobject, jobject);

/*
 * Class:     io_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeSubmitActorTask
 * Signature:
 * ([BLio/ray/runtime/functionmanager/FunctionDescriptor;ILjava/util/List;ILio/ray/api/options/CallOptions;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeSubmitActorTask(JNIEnv *, jclass,
                                                                   jbyteArray, jobject,
                                                                   jint, jobject, jint,
                                                                   jobject);

/*
 * Class:     io_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeCreatePlacementGroup
 * Signature: (Lio/ray/api/options/PlacementGroupCreationOptions;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeCreatePlacementGroup(JNIEnv *, jclass,
                                                                        jobject);

/*
 * Class:     io_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeRemovePlacementGroup
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeRemovePlacementGroup(JNIEnv *, jclass,
                                                                        jbyteArray);

/*
 * Class:     io_ray_runtime_task_NativeTaskSubmitter
 * Method:    nativeWaitPlacementGroupReady
 * Signature: ([BI)Z
 */
JNIEXPORT jboolean JNICALL
Java_io_ray_runtime_task_NativeTaskSubmitter_nativeWaitPlacementGroupReady(JNIEnv *,
                                                                           jclass,
                                                                           jbyteArray,
                                                                           jint);

#ifdef __cplusplus
}
#endif
#endif
