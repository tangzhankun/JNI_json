#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <map>

#define     RAPIDJSON_HAS_STDSTRING    1

#include <rapidjson/document.h>

using namespace std;
using namespace rapidjson;

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

typedef std::vector<json_schema_field_t> json_schema_t;

typedef std::vector<std::string> jsons_t;

typedef struct unsafe_row
{
    int fields;
    int nullbits_bytes;
    int total_bytes;
    char *row;
} unsafe_row_t;

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

jsons_t load_json(std::string file)
{
    jsons_t v;

    std::ifstream ifs(file);
    for (std::string line; std::getline(ifs, line); )
        v.push_back(line);

    return v; 
}

void jsons_to_unsafe_row(json_schema_t const&schema, jsons_t const& jsons, unsafe_row_t &row)
{
    for (auto const& json : jsons)
    {
        Document document;
        document.Parse(json.c_str());
        //if (document.ParseInsitu(json.c_str()).HasParseError())
        {
        }
        
        int index = 0;
        for (auto const& field : schema) 
        {
            string name = field.name;
            if (document.HasMember(name))
            {
                if (document[name].IsNull())
                {
                    unsafe_row_set_nullbit(row, index, true);
                }
                else
                {
                    json_schema_type_t type = field.type;
                    if (type == IntegerType)
                    {
                        int value = document[name].GetInt();
                        *(int32_t *)(row.row + row.nullbits_bytes + 8 * index) = value;
                    }
                    else if (type == LongType)
                    {
                        int64_t value = document[name].GetInt64();
                        *(int64_t *)(row.row + row.nullbits_bytes + 8 * index) = value;
                    }
                    else if (type == FloatType)
                    {
                        double value = document[name].GetDouble();
                        *(float *)(row.row + row.nullbits_bytes + 8 * index) = value;
                    }
                    else if (type == DoubleType)
                    {
                        double value = document[name].GetDouble();
                        *(double *)(row.row + row.nullbits_bytes + 8 * index) = value;
                    }
                    else if (type == StringType)
                    {
                        string value = document[name].GetString();
                        *(uint32_t*)(row.row + row.nullbits_bytes + 8 * index) = value.length();
                        *(uint32_t*)(row.row + row.nullbits_bytes + 8 * index + 4) = row.total_bytes;
                        memcpy(row.row + row.total_bytes, value.c_str(), value.length());
                        row.total_bytes += (value.length() + 7) / 8 * 8;
                    }
                    else if (type == ShortType)
                    {
                        int16_t value = document[name].GetInt();
                        *(int16_t *)(row.row + row.nullbits_bytes + 8 * index) = value;
                    }
                    else if (type == BooleanType)
                    {
                        bool value = document[name].GetBool();
                        *(int8_t *)(row.row + row.nullbits_bytes + 8 * index) = value;
                    }
                    unsafe_row_set_nullbit(row, index, false);
                }
            }
            else
            {
                unsafe_row_set_nullbit(row, index, true);
                memset(row.row + row.nullbits_bytes + 8 * index, 0x0, 8);
            }
            index++;
        }
    }

    return;
}

int main(int argc, char **argv)
{
    unsafe_row_t row;
    json_schema_t schema;

    row.row = new char[4096];

    schema.push_back({"BOOLEAN_ID", BooleanType, true});
    schema.push_back({"SHORT_ID", ShortType, true});
    schema.push_back({"INT_ID", IntegerType, true});
    schema.push_back({"LONG_ID", LongType, true});
    schema.push_back({"FLOAT_ID", FloatType, true});
    schema.push_back({"DOUBLE_ID", DoubleType, true});
    schema.push_back({"STRING_ID", StringType, true});

    unsafe_row_init(row, schema);

    jsons_t jsons = load_json(argc > 1 ? argv[1] : "test.json");

    jsons_to_unsafe_row(schema, jsons, row);

    ofstream out("unsaferow.bin", ofstream::binary);    
    out.write (row.row, row.total_bytes);
    out.close();

    delete [] row.row;

    return 0;
}
