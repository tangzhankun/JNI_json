#include "org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl.h"
#include "unsafeRow.h"
#include "wasai/libgendma.h"
using namespace std;
// device fd of /dev/wasai0
static int fpga_fd;
static int MAX_FIELDS = 4;

jint throwException( JNIEnv *env, char *message ) {
    jclass exClass;
    char *className = "java/lang/IllegalStateException";
    exClass = env->FindClass(className);
    if (exClass == NULL) {
      cerr<<"unknown error happens when find java class IllegalStateException!"<<endl;
    }
    return env->ThrowNew(exClass, message );
}


jbyte* populateUnsafeRows(int count, int& buffer_size){
  return create_fake_row(count, buffer_size);
}

int init_accelerator(bool use_hardware) {
  if (use_hardware) {
    fpga_fd = wasai_init("/dev/wasai0");
    if (fpga_fd == -1) {
      return 0;
    }
    return 1; 
  } else {
    return 1;
  }
}

int fourCharstoInt(char* buffer) {
  int ret = int((unsigned char)(buffer[0])) << 24 |
            (unsigned char)(buffer[1]) << 16 |
            (unsigned char)(buffer[2]) << 8 |
            (unsigned char)(buffer[3]);
  cerr<<"converting str:"<<buffer[0]<<buffer[1]<<buffer[2]<<buffer[3]<<",to:"<<std::hex<<ret<<endl;
  return ret;
}

//As "wasai_setjsonkey" required, each field name string should be converted to four int ascii values
int convertStringToAscii(char* str, int str_size, unsigned int* fourAssicii, int numbercount) {
  if (str_size > 4*numbercount) {
    cerr<<"Only less than 16 bytes(four int values) are supported for field name. Extra chars will be ignored"<<endl;
  }
  //split str into <numbercount> int values
  char* buff = str + str_size - 1;
  int j = 0;
  for (int i = numbercount -1; i >= 0; i--) {
    if(str_size <= j*4) {
      break;// no more chars in str
    }
    fourAssicii[i] = fourCharstoInt(buff-3);
    buff-=4;
    j++;
  }
}


void set_schema(const char* fieldNames, jint size, jint* fieldTypes) {
 //split fieldNames str with ","
 // only support 4 field
 //wasai_setschema(fpga_fd, 0x1111);
  int field_index = 0;
  char* pch = strtok( const_cast<char *>(fieldNames), ",");
  int number_count = 4;
  unsigned int* fourAscii = new unsigned int[number_count];
  memset(fourAscii, 0, number_count*4);
  while (pch != NULL && field_index < MAX_FIELDS) {
    cerr<<"fieldName"<<field_index<<":"<<pch<<endl;
    //we need to transform the fieldName to four HEX value
    convertStringToAscii(pch, strlen(pch), fourAscii, number_count);
    cerr<<"ascii parameters:"<<std::hex<<fourAscii[0]<<" "<<std::hex<<fourAscii[1]<<" "<<std::hex<<fourAscii[2]<<" "<<std::hex<<fourAscii[3]<<endl;
    //call wasai_setschema and setjsonkey API
    pch = strtok(NULL, ",");
    field_index++;
  }
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_setSchema
  (JNIEnv *env, jobject obj, jstring schemaFieldNames, jintArray schemaFieldTypes) {
  cerr<<"call setSchema[JNI] - this method try init FPGA devices and set schema"<<endl;
  if (!init_accelerator(false)) {
    cerr<<"Accelerator hadware is not ready!"<<endl;
    throwException(env, "Accelerator cannot be initialized!\n");
  }
  const char* fieldNames = env->GetStringUTFChars(schemaFieldNames, 0);
  jint fieldNamesStrsize = env->GetStringLength(schemaFieldNames);
  jint* fieldTypes = env->GetIntArrayElements(schemaFieldTypes, 0); 
  set_schema(fieldNames, fieldNamesStrsize, fieldTypes);
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
