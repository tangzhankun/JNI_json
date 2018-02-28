#include "org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl.h"
#include "unsafeRow.h"
using namespace std;

jbyte* populateUnsafeRows(int count){
 return create_fake_row(count);  
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_setSchema
  (JNIEnv *env, jobject obj, jstring schemaFieldNames, jintArray schemaFieldTypes) {
  cerr<<"call setSchema[JNI] - this method set schema"<<endl;
  const char* fieldNames = env->GetStringUTFChars(schemaFieldNames, 0);
  jint* fieldTypes = env->GetIntArrayElements(schemaFieldTypes, 0); 
  return true;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_parseJson
  (JNIEnv *env, jobject obj, jstring json_str) {
  cerr<<"call parseJson[JNI] - this method return byteArray"<<endl;
  const char* jsonStr = env->GetStringUTFChars(json_str, 0);
  //we want return two UnsafeRow of "{123, hello, json}\n{456, hello, fpga}", total 82 bytes
  int count = 10;
  jbyteArray ret = env->NewByteArray(20);
  jbyte *unsafeRows = populateUnsafeRows(count);
  //jbyte unsafeRows[] = {'1','2','3','4','5','6'};
  env->SetByteArrayRegion(ret, 0, 20, unsafeRows);
  return ret; 
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_parseJson2
  (JNIEnv *env, jobject obj, jstring json_str) {

  cerr<<"call parseJson2[JNI] - this method return long pointer address(not implemented)"<<endl;
  const char* jsonStr = env->GetStringUTFChars(json_str, 0);
  return -1;
}

JNIEXPORT void JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_close
  (JNIEnv *, jobject) {
  cerr<<"call close[JNI] - this method should do some clean up"<<endl;
}


int main() {
  create_fake_row(1);
  return 0;
}
