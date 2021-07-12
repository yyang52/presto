#include "com_facebook_presto_orc_OrcReaderJNI.h"
#include <stdio.h>

JNIEXPORT jboolean JNICALL Java_com_facebook_presto_orc_OrcReaderJNI_hasNext(JNIEnv* env,
                                                                       jclass cls,
                                                                       jlong readerPtr) {
  //ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
 // return reader->hasNext();
 return true;
}

JNIEXPORT jint JNICALL Java_com_facebook_presto_orc_OrcReaderJNI_readBatch(
    JNIEnv* env, jclass cls, jlong readerPtr, jint batchSize, jlongArray buffers,
    jlongArray nulls) {
  //ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  //jsize buffersLen = env->GetArrayLength(buffers);
  //jsize nullsLen = env->GetArrayLength(nulls);
 // assert(buffersLen == nullsLen);

  //jlong* buffersPtr = env->GetLongArrayElements(buffers, 0);
  //jlong* nullsPtr = env->GetLongArrayElements(nulls, 0);

  //int ret = reader->readBatch(batchSize, buffersPtr, nullsPtr);

  //env->ReleaseLongArrayElements(buffers, buffersPtr, 0);
  //env->ReleaseLongArrayElements(nulls, nullsPtr, 0);
  //return ret;
  return 0;
}

JNIEXPORT jint JNICALL Java_com_facebook_presto_orc_OrcReaderJNI_processBlocks
  (JNIEnv* env, jclass cls, jlong readerPtr, jint positionCount) {
    // some real logic...

    printf("processing within JNI...\n");
    fflush(stdout);
    int ret = positionCount / 2 + 1;

    return ret;
}