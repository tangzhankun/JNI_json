#include "FpgaJsonParser.h"
#include "unsafeRow.h"

JNIEXPORT jboolean JNICALL Java_FpgaJsonParser_booleanMethod
  (JNIEnv *env, jobject obj, jboolean boolean) {
  return !boolean;
}
JNIEXPORT jbyteArray JNICALL Java_FpgaJsonParser_parseJson
  (JNIEnv *env, jobject obj, jstring str) {
  const char* aStr = env->GetStringUTFChars(str, 0);
  //we want return two UnsafeRow of "{123, hello, json}\n{456, hello, fpga}", total 82 bytes
  jbyteArray ret = env->NewByteArray(6);
  //jbyte unsafeRows = populateUnsafeRows();
  jbyte unsafeRows[] = {'1','2','3','4','5','6'};
  env->SetByteArrayRegion(ret, 0, 6, unsafeRows);
  return ret; 
}

jbyte populateUnsafeRows(){
  
}

