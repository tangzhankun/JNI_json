#include <string>
#include <vector>

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
  char *row;
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
  row.row[offset] |= nullable << (index % 8);

  return;
}

unsafe_rows create_fake_row(int count) {
  return NULL;
}
