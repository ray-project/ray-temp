// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/* DO NOT EDIT THIS FILE - it is machine generated */
#include "jni.h"
/* Header for class org_apache_arrow_plasma_PlasmaClientJNI */

#ifndef _Included_org_apache_arrow_plasma_PlasmaClientJNI
#define _Included_org_apache_arrow_plasma_PlasmaClientJNI
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    connect
 * Signature: (Ljava/lang/String;Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_connect(
    JNIEnv*, jclass, jstring, jstring, jint);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    disconnect
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_disconnect(JNIEnv*,
                                                                               jclass,
                                                                               jlong);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    create
 * Signature: (J[BI[B)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_create(
    JNIEnv*, jclass, jlong, jbyteArray, jint, jbyteArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    hash
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_plasma_PlasmaClientJNI_hash(JNIEnv*, jclass, jlong, jbyteArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    seal
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_seal(JNIEnv*, jclass,
                                                                         jlong,
                                                                         jbyteArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    release
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_release(JNIEnv*,
                                                                            jclass, jlong,
                                                                            jbyteArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    delete
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_delete(JNIEnv*,
                                                                           jclass, jlong,
                                                                           jbyteArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    get
 * Signature: (J[[BI)[[Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_get(
    JNIEnv*, jclass, jlong, jobjectArray, jint);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    contains
 * Signature: (J[B)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_arrow_plasma_PlasmaClientJNI_contains(JNIEnv*, jclass, jlong, jbyteArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    fetch
 * Signature: (J[[B)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_fetch(JNIEnv*, jclass,
                                                                          jlong,
                                                                          jobjectArray);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    wait
 * Signature: (J[[BII)[[B
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_wait(
    JNIEnv*, jclass, jlong, jobjectArray, jint, jint);

/*
 * Class:     org_apache_arrow_plasma_PlasmaClientJNI
 * Method:    evict
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_evict(JNIEnv*,
                                                                           jclass, jlong,
                                                                           jlong);

#ifdef __cplusplus
}
#endif
#endif
