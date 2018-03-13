#include "org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl.h"
#include "unsafeRow.h"
#include "wasai/libgendma.h"
#include <bitset>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

#define RESULT_SIZE 1024*1024*1024
#define MAX_FIELDS 4
#define USE_FPGA_FLAG true
#define FPGA_FD_PATH "/dev/wasai0"
static int fpga_fd;

jint throwException( JNIEnv *env, char *message ) {
    jclass exClass;
    char *className = "java/lang/IllegalStateException";
    exClass = env->FindClass(className);
    if (exClass == NULL) {
      cerr<<"[JNI]unknown error happens when find java class IllegalStateException!"<<endl;
    }
    return env->ThrowNew(exClass, message );
}


signed char* populateUnsafeRows(int count, long& buffer_size, bool useFPGAFLAG, const char* jsonStr, jint jsonStrSize){
  if (false == useFPGAFLAG) {
    return create_fake_row_from_bin_file(0,buffer_size);
  } else {
    // dma transfer to FPGA and get row back
    //unsigned char* unsafeRows = new unsigned char[RESULT_SIZE];
    unsigned char* unsafeRows = malloc(RESULT_SIZE);
    memset(unsafeRows, 0, RESULT_SIZE);
    wasai_dma_transfer_without_file(fpga_fd, jsonStr, jsonStrSize);
    wasai_read_row(fpga_fd, RESULT_SIZE, &unsafeRows);
    buffer_size = wasa_row_total(fpga_fd);
    return unsafeRows;
  }
}



int init_accelerator(bool use_hardware) {
  if (use_hardware) {
    fpga_fd = wasai_init(FPGA_FD_PATH);
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
  //cerr<<"[JNI]converting str:"<<buffer[0]<<buffer[1]<<buffer[2]<<buffer[3]<<",to:"<<std::hex<<"0x"<<ret<<endl;
  return ret;
}

//As "wasai_setjsonkey" required, each field name string should be converted to four int ascii values
int convertStringToAscii(char* str, int str_size, unsigned int* fourAssicii, int numbercount) {
  memset(fourAssicii, 0, numbercount*sizeof(int));
  if (str_size > 4*numbercount) {
    cerr<<"[JNI]Only less than 16 bytes(four int values) are supported for field name. Extra chars will be ignored"<<endl;
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

unsigned int getFieldTypeBits(jint fieldcount, jint* fieldTypes) {
  bitset<MAX_FIELDS> bits;
  bits.set();
  for (int i = 0; i < fieldcount; i++) {
    //cerr<<"[JNI]fieldTypes["<<i<<"]"<<fieldTypes[i]<<endl;
    if ((int)(fieldTypes[i]) != StringType) {
      //cerr<<"[JNI]set it to 0"<<endl;
      bits.set(i+1, 0);
    }
  }
  return (unsigned int)(bits.to_ulong());
}


void set_schema(const char* fieldNames, jint strSize, jint* fieldTypes) {
 //split fieldNames str with ","
 // only support 4 field
  int typeBits = getFieldTypeBits(MAX_FIELDS, fieldTypes);
  //cerr<<"[JNI]typeBits is: 0b"<<std::bitset<MAX_FIELDS>(typeBits)<<endl;

  wasai_setschema(fpga_fd, typeBits);
  int field_index = 0;
  char* pch = strtok( const_cast<char *>(fieldNames), ",");
  int number_count = 4;
  unsigned int* fourAscii = new unsigned int[number_count];
  memset(fourAscii, 0, number_count*4);
/*
  while (pch != NULL && field_index < MAX_FIELDS) {
    cerr<<"[JNI]fieldName"<<field_index<<":"<<pch<<endl;
    //we need to transform the fieldName to four HEX value
    convertStringToAscii(pch, strlen(pch), fourAscii, number_count);
    //cerr<<"[JNI]ascii parameters:0x"<<std::hex<<fourAscii[0]<<" "<<std::hex<<fourAscii[1]<<" "<<std::hex<<fourAscii[2]<<" "<<std::hex<<fourAscii[3]<<endl;
    //call wasai_setschema and setjsonkey API
    int index_to_set = field_index;
    cerr<<"[JNI]wasai_setjsonkey(fpga_fd,"<< index_to_set <<", "<<std::hex<<fourAscii[0]<<", "<<std::hex<<fourAscii[1]<<", "<<std::hex<<fourAscii[2]<<", "<<std::hex<<fourAscii[3]<<")"<<endl;
    //wasai_setjsonkey(fpga_fd, index_to_set, fourAscii[0], fourAscii[1], fourAscii[2], fourAscii[3]);
    pch = strtok(NULL, ",");
    field_index++;
  }
*/
  wasai_setjsonkey(fpga_fd, 0, 0x0, 0x0, 0x414343, 0x5f4e4252);
  wasai_setjsonkey(fpga_fd, 1, 0x0, 0x4f42494c, 0x4c494e47, 0x5f544944);
  wasai_setjsonkey(fpga_fd, 2, 0x0, 0x4e42494c, 0x4c494e47, 0x5f544944);
  wasai_setjsonkey(fpga_fd, 3, 0x0, 0x0, 0x4f504552, 0x5f544944);
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_setSchema
  (JNIEnv *env, jobject obj, jstring schemaFieldNames, jintArray schemaFieldTypes) {
  //cerr<<"[JNI]call setSchema - this method try init FPGA devices and set schema"<<endl;
  if (false == USE_FPGA_FLAG) {
    //cerr<<"[JNI]Fake row data generation doesn't needs to set_schema"<<endl;
    return true;
  }

  if (!init_accelerator(USE_FPGA_FLAG)) {
    //cerr<<"[JNI]Accelerator hadware is not ready!"<<endl;
    throwException(env, "Accelerator cannot be initialized!\n");
  }
  const char* fieldNames = env->GetStringUTFChars(schemaFieldNames, 0);
  //cerr<<"Got fieldNames from scala: "<<fieldNames<<endl;
  jint fieldNamesStrsize = env->GetStringLength(schemaFieldNames);
  jint* fieldTypes = env->GetIntArrayElements(schemaFieldTypes, 0); 
  set_schema(fieldNames, fieldNamesStrsize, fieldTypes);
  return true;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_parseJson
  (JNIEnv *env, jobject obj, jstring json_str) {
  //cerr<<"[JNI]call parseJson - this method return byteArray"<<endl;
  const char* jsonStr = env->GetStringUTFChars(json_str, 0);
  jint jsonStrSize = env->GetStringLength(json_str);
  int count = 10;
  long buffer_size = 0;
  // dma transfer to FPGA and get row back
  // buff = malloc(RESULT_SIZE);
  // wasai_dma_transfer_without_file(fpga_fd, jsonStr, jsonStrSize);
  // wasai_read_row(fpga_fd, RESULT_SIZE, &buff)

  //fake rows
  jbyte *unsafeRows = populateUnsafeRows(count, buffer_size, USE_FPGA_FLAG, jsonStr, jsonStrSize);
  //cerr<<"[JNI]unsafeRow buffer size is "<< buffer_size << endl;
  jbyteArray ret = env->NewByteArray(buffer_size);
  env->SetByteArrayRegion(ret, 0, buffer_size, unsafeRows);
  return ret; 
}
JNIEXPORT jlongArray JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_parseJson2
  (JNIEnv *env, jobject obj, jstring json_str) {
  //cerr<<"[JNI]call parseJson2 - this method return long pointer address and size"<<endl;
  const char* jsonStr = env->GetStringUTFChars(json_str, 0);
  jint jsonStrSize = env->GetStringLength(json_str);
  int count = 10;
  long buffer_size = 0;
  signed char *unsafeRows = populateUnsafeRows(count, buffer_size, USE_FPGA_FLAG, jsonStr, jsonStrSize);
  jlongArray ret = env->NewLongArray(2);
  jlong address = (jlong)((void*)(unsafeRows));
  jlong* addr = &address;
  jlong* total_size = &buffer_size;
  //cerr<<"[JNI]the buffer addr is "<<std::dec<<address<<endl;
  //cerr<<"[JNI]unsafeRow buffer size is "<<std::dec<< *total_size << endl;
  env->SetLongArrayRegion(ret, 0, 1, addr);
  env->SetLongArrayRegion(ret, 1, 1, total_size);
  if (true == USE_FPGA_FLAG) {
    //wasai_destroy(fpga_fd);
  }
  return ret;
}

JNIEXPORT void JNICALL Java_org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl_close
  (JNIEnv *, jobject) {
  //cerr<<"[JNI]call close - this method should do some clean up"<<endl;
}


int main() {
  long buffer_size = 0;
  create_fake_row_from_bin_file(0, buffer_size);
  return 0;
}
