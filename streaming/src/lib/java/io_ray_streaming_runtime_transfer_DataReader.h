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
/* Header for class io_ray_streaming_runtime_transfer_DataReader */

#ifndef _Included_io_ray_streaming_runtime_transfer_DataReader
#define _Included_io_ray_streaming_runtime_transfer_DataReader
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_ray_streaming_runtime_transfer_DataReader
 * Method:    createDataReaderNative
 * Signature:
 * (Lio/ray/streaming/runtime/transfer/ChannelCreationParametersBuilder;[[B[JJLjava/util/List;[BZ)J
 */
JNIEXPORT jlong JNICALL
Java_io_ray_streaming_runtime_transfer_DataReader_createDataReaderNative(
    JNIEnv *, jclass, jobject, jobjectArray, jlongArray, jlong, jobject, jbyteArray,
    jboolean);

/*
 * Class:     io_ray_streaming_runtime_transfer_DataReader
 * Method:    getBundleNative
 * Signature: (JJJJ)V
 */
JNIEXPORT void JNICALL Java_io_ray_streaming_runtime_transfer_DataReader_getBundleNative(
    JNIEnv *, jobject, jlong, jlong, jlong, jlong);

/*
 * Class:     io_ray_streaming_runtime_transfer_DataReader
 * Method:    getOffsetsInfoNative
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_io_ray_streaming_runtime_transfer_DataReader_getOffsetsInfoNative(JNIEnv *, jobject,
                                                                       jlong);

/*
 * Class:     io_ray_streaming_runtime_transfer_DataReader
 * Method:    stopReaderNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_ray_streaming_runtime_transfer_DataReader_stopReaderNative(
    JNIEnv *, jobject, jlong);

/*
 * Class:     io_ray_streaming_runtime_transfer_DataReader
 * Method:    closeReaderNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_ray_streaming_runtime_transfer_DataReader_closeReaderNative(JNIEnv *, jobject,
                                                                    jlong);

#ifdef __cplusplus
}
#endif
#endif
