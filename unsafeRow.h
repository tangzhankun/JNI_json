#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <map>
#include <cstring>
#include <stdint.h>
using namespace std;
typedef enum json_schema_type
{
  BooleanType =  1,
  ShortType,
  IntegerType,
  LongType,
  FloatType,
  DoubleType,
  StringType,
} json_schema_type_t;

typedef struct json_schema_field
{
  std::string name;
  json_schema_type_t type;
  bool nullable;
} json_schema_field_t;

typedef struct unsafe_row
{
  int fields;
  int nullbits_bytes;
  long total_bytes;
  signed char *row;
} unsafe_row_t;

typedef std::vector<json_schema_field_t> json_schema_t;

typedef std::vector<std::string> jsons_t;

typedef std::vector<unsafe_row_t> unsafe_rows;

void unsafe_row_init(unsafe_row_t &row, json_schema_t &schema)
{
  row.fields = schema.size();
  row.nullbits_bytes = ((row.fields + 63) / 64) * 8;
  row.total_bytes = (8 * row.fields + row.nullbits_bytes);

  return;
}

void unsafe_row_set_nullbit(unsafe_row_t &row, int index, bool nullable)
{
  int offset = index / 8;
  row.row[offset] |= nullable << (index % 8);

  return;
}
/*
signed char* create_fake_row_with_row_size(int count, int& buffer_size) { 
  unsafe_row_t row;
  row.row = new signed char[4096];
  memset(row.row, 0, 4096);
  json_schema_t schema;
  json_schema_field_t f1 = {"ID", IntegerType, true};
  json_schema_field_t f2 = {"TEXT", StringType, true};
  schema.push_back(f1);
  schema.push_back(f2);
  unsafe_row_init(row, schema);
  std::string value = "hello,json";
  //123
  static signed char ret[] = {16,0,0,0,0,0,0,0,0,0,0,0,123,0,0,0,0,0,0,0};
  //123  hello,json
  static signed char ret2[] = {40,0,0,0,0,0,0,0,0,0,0,0,123,0,0,0,0,0,0,0,10,0,0,0,24,0,0,0,104,101,108,108,111,44,106,115,111,110,0,0,0,0,0,0};
  int current_row_pos = 0;
  const int header_size = 4;
  for(int i = 1; i<=count; i++){
    int id = i;
    int index = 0;
    current_row_pos += header_size;
    //null bits
    row.row[current_row_pos] |= 0;
    *(int32_t *)(row.row + current_row_pos + row.nullbits_bytes + 8 * index) = id;
    index++;
    *(uint32_t*)(row.row + current_row_pos + row.nullbits_bytes + 8 * index) = value.length();
    *(uint32_t*)(row.row + current_row_pos + row.nullbits_bytes + 8 * index + 4) = row.total_bytes;
    memcpy(row.row + current_row_pos + row.total_bytes, value.c_str(), value.length());
    row.total_bytes += (value.length() + 7) / 8 * 8;
    *(int32_t *)(row.row + current_row_pos - header_size) = row.total_bytes;//set row_size header
    current_row_pos += row.total_bytes;
  }
  buffer_size = current_row_pos;
  return row.row;
}
*/

// only support maximum 4 fields( double/String). String with fixed length 128 bytes
// DDDD 40 bytes
// DDDS 168 bytes
// DDSS 296 bytes
// DSSS 424 bytes
// SSSS 552 bytes
signed char* create_fake_row_without_row_size(int count, long& buffer_size) { 
  unsafe_row_t row;
  row.row = new signed char[4096];
  memset(row.row, 0, 4096);
  json_schema_t schema;
  json_schema_field_t f1 = {"ID", IntegerType, true};
  json_schema_field_t f2 = {"ID_1", IntegerType, true};
  json_schema_field_t f3 = {"ID_2", IntegerType, true};
  json_schema_field_t f4 = {"TEXT", StringType, true};
  schema.push_back(f1);
  schema.push_back(f2);
  schema.push_back(f3);
  schema.push_back(f4);
  unsafe_row_init(row, schema);
  std::string value[] = {"hello,a simple json string", "hello, this is a simple text", "This is a string", "This is a text"};
  int current_row_pos = 0;
  row.total_bytes = 0;
  cerr<<"[JNI]current row size is:"<<std::dec<<row.total_bytes<<endl;
  for(int i = 1; i<=count; i++){
    int id = i;
    int index = 0;
    //null bits
    row.row[current_row_pos] |= 0;
    *(int32_t *)(row.row + current_row_pos + row.nullbits_bytes + 8 * index) = id;
    index++;

    *(int32_t *)(row.row + current_row_pos + row.nullbits_bytes + 8 * index) = id;
    index++;

    *(int32_t *)(row.row + current_row_pos + row.nullbits_bytes + 8 * index) = id;
    index++;

    *(uint32_t*)(row.row + current_row_pos + row.nullbits_bytes + 8 * index) = value[i%4].length();
    *(uint32_t*)(row.row + current_row_pos + row.nullbits_bytes + 8 * index + 4) = (8 * schema.size() + 8);
    memcpy(row.row + current_row_pos + 8 * schema.size() + 8, value[i%4].c_str(), value[i%4].length());
    row.total_bytes += (8*schema.size() + 8 + 128);
    cerr<<"[JNI]current row_size is:"<<row.total_bytes<<endl;
    current_row_pos += (8*schema.size() + 8 + 128);
  }
  buffer_size = row.total_bytes;
  return row.row;
}
