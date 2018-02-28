#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <map>
#include <cstring>
#include <stdint.h>
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
  int total_bytes;
  signed char *row;
} unsafe_row_t;

typedef std::vector<json_schema_field_t> json_schema_t;

typedef std::vector<std::string> jsons_t;

typedef std::vector<unsafe_row_t> unsafe_rows;

void unsafe_row_init(unsafe_row_t &row, json_schema_t &schema)
{
  row.fields = schema.size();
  row.nullbits_bytes = ((row.fields + 63) / 64) * 8;
  row.total_bytes = 8 * row.fields + row.nullbits_bytes;

  return;
}

void unsafe_row_set_nullbit(unsafe_row_t &row, int index, bool nullable)
{
  int offset = index / 8;
  row.row[offset+4] |= nullable << (index % 8);

  return;
}

signed char* create_fake_row(int count) {

  //unsafe_row_t row;
  //json_schema_t schema;
  //json_schema_field_t f1 = {"ID", IntegerType, true};
  //schema.push_back(f1);
  //unsafe_row_init(row, schema);
  //std::string value = "hello, json";
  static signed char ret[] = {16,0,0,0,0,0,0,0,0,0,0,0,123,0,0,0,0,0,0,0};
 
  //for(int i = 1; i<=count; i++){
  //  int id = 123;
  //  int index = 0;
  //  *(int32_t *)row.row = 16;// first int indicate row size is 16
  //  row.row[4] |= 0;
  //  *(int32_t *)(row.row + row.nullbits_bytes + 8 * index) = id;
  //  index++;
  //  *(uint32_t*)(row.row + row.nullbits_bytes + 8 * index) = value.length();
  //  *(uint32_t*)(row.row + row.nullbits_bytes + 8 * index + 4) = row.total_bytes;
  //  memcpy(row.row + row.total_bytes, value.c_str(), value.length());
  //  row.total_bytes += (value.length() + 7) / 8 * 8;
  //}
  return ret;
}
