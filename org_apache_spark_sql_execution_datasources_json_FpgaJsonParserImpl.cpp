#include "org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl.h"
#include "unsafeRow.h"
using namespace std;

jbyte* populateUnsafeRows(int count, int& buffer_size){
 return create_fake_row(count, buffer_size);
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
  int count = 10;
  int buffer_size = 0;
  jbyte *unsafeRows = populateUnsafeRows(count, buffer_size);
  cerr<<"unsafeRow buffer size is " << buffer_size << endl;
  jbyteArray ret = env->NewByteArray(buffer_size);
  env->SetByteArrayRegion(ret, 0, buffer_size, unsafeRows);
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
  int size = 0;
  create_fake_row(1, size);
  return 0;
}
