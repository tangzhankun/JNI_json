#include "FpgaJsonParser.h"
#include "unsafeRow.h"
using namespace std;

jbyte* populateUnsafeRows(int count){
 return create_fake_row(count);  
}

JNIEXPORT jboolean JNICALL Java_FpgaJsonParser_booleanMethod
  (JNIEnv *env, jobject obj, jboolean boolean) {
  return !boolean;
}

JNIEXPORT jboolean JNICALL Java_FpgaJsonParser_setSchema
  (JNIEnv *env, jobject, jstring schemaFieldNames, jintArray schemaFieldTypes) {
  const char* fieldNames = env->GetStringUTFChars(schemaFieldNames, 0);
  jint* fieldTypes = env->GetIntArrayElements(schemaFieldTypes, 0); 
  return true;
}


JNIEXPORT jbyteArray JNICALL Java_FpgaJsonParser_parseJson
  (JNIEnv *env, jobject obj, jstring json_str)  {
  cout<<"hello jni"<<endl;
  const char* jsonStr = env->GetStringUTFChars(json_str, 0);
  //we want return two UnsafeRow of "{123, hello, json}\n{456, hello, fpga}", total 82 bytes
  int count = 10;
  jbyteArray ret = env->NewByteArray(10*42);
  jbyte *unsafeRows = populateUnsafeRows(count);
  //jbyte unsafeRows[] = {'1','2','3','4','5','6'};
  env->SetByteArrayRegion(ret, 0, 10*42, unsafeRows);
  return ret; 
}


