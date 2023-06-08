#include "schema.h"
#include <vector>
#include <string>
#include <cstring>
#include <stdio.h>
#include <fstream>


// initialize the schema from the file formatted as follows:
// [n (num_attributes)]
// [field0_name] [field0_type] [field0_size]
// [field1_name] [field1_type] [field1_size]
// ...
// [fieldn_name] [fieldn_type] [fieldn_size]
void Schema::read_from_file(const std::string& schema_file_path) {
    std::ifstream fp;
    fp.open(schema_file_path.c_str(), std::ios::in);
    std::string line;
    std::getline(fp, line);
    this->num_attributes = std::stoul(line.c_str());
    this->total_size = 0;
    uint32_t start = 0;
    uint32_t end;
    uint32_t i = 0;
    this->attribute_names.resize(this->num_attributes);
    this->attribute_types.resize(this->num_attributes);
    this->attribute_sizes.resize(this->num_attributes);
    while (std::getline(fp, line) && i < this->num_attributes) {
        // read attribute name
        end = line.find(" ", start);
        this->attribute_names[i] = line.substr(start, end - start);
        start = end + 1;
        // read attribute type
        end = line.find(" ", start);
        this->attribute_types[i] = static_cast<ATTRIBUTE_TYPE>(std::stoul(line.substr(start, end - start).c_str()));
        start = end + 1;
        // read attribute size
        this->attribute_sizes[i] = std::stoul(line.substr(start).c_str());
        this->total_size += this->attribute_sizes[i];
        i++;
	start = 0;
    }
    fp.close();
}


void Line2byteArray(std::string line, char* buff, const Schema& schema, const char separator) {
    memset(buff, 0, schema.total_size);
    uint32_t start = 0;
    uint32_t end;
    std::string tmp_s;
    char* dest = buff;

    uint64_t tmp_uint64_t_value = 0;
    uint32_t tmp_uint32_t_value = 0;
    long long tmp_long_long_value = 0;
    long tmp_long_value = 0;
    float tmp_float_value = 0;
    double tmp_double_value = 0;
    std::string tmp_string_value = "";

    for(uint32_t i = 0; i < schema.num_attributes; i++) {
        end = line.find(separator,start);
        tmp_s = line.substr(start, end - start);
        if (schema.attribute_types[i] == INT) {
            if (schema.attribute_sizes[i] == 8) {
                tmp_long_long_value = std::stoll(tmp_s);
                std::memcpy(dest, static_cast<char*>(static_cast<void*>(&tmp_long_long_value)), schema.attribute_sizes[i]);
            } else if (schema.attribute_sizes[i] == 4) {
                tmp_long_value = std::stol(tmp_s);
                std::memcpy(dest, static_cast<char*>(static_cast<void*>(&tmp_long_value)), schema.attribute_sizes[i]);
            }
        } else if (schema.attribute_types[i] == UINT) {
            if (schema.attribute_sizes[i] == 8) {
                tmp_uint64_t_value = std::stoull(tmp_s);
                std::memcpy(dest, static_cast<char*>(static_cast<void*>(&tmp_uint64_t_value)), 8);
            } else if (schema.attribute_sizes[i] == 4) {
                tmp_uint32_t_value = std::stoul(tmp_s);
                std::memcpy(dest, static_cast<char*>(static_cast<void*>(&tmp_uint32_t_value)), 4);
            }
        } else if (schema.attribute_types[i] == FLOAT) {
            if (schema.attribute_sizes[i] == 8) {
                tmp_double_value = std::stod(tmp_s);
                std::memcpy(dest, static_cast<char*>(static_cast<void*>(&tmp_double_value)), 8);
            } else if (schema.attribute_sizes[i] == 4) {
                tmp_float_value = std::stof(tmp_s);
                std::memcpy(dest, static_cast<char*>(static_cast<void*>(&tmp_float_value)), 4);
            }
        } else if (schema.attribute_types[i]  == STRING || schema.attribute_types[i] == DATE) {
            tmp_string_value = tmp_s;
            std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
        }

        dest += schema.attribute_sizes[i];
        start = end + 1;  
    }

    return;
}

void ByteArray2line(char* buff, std::string & line, const Schema& schema, const char separator) {
    line.clear();
    char* src = buff;
    
    uint64_t tmp_uint64_t_value = 0;
    uint32_t tmp_uint32_t_value = 0;
    long long tmp_long_long_value = 0;
    long tmp_long_value = 0;
    float tmp_float_value = 0;
    double tmp_double_value = 0;
    std::string tmp_string_value = "";
    for (uint32_t i = 0; i < schema.num_attributes; i++) {
        if (schema.attribute_types[i] == INT) {
            if (schema.attribute_sizes[i] == 8) {
                std::memcpy(static_cast<char*>(static_cast<void*>(&tmp_long_long_value)), src, schema.attribute_sizes[i]);
                line += std::to_string(tmp_long_long_value);
            } else if (schema.attribute_sizes[i] == 4) {
                std::memcpy(static_cast<char*>(static_cast<void*>(&tmp_long_value)), src, schema.attribute_sizes[i]);
                line += std::to_string(tmp_long_value);
            }
        } else if (schema.attribute_types[i] == UINT) {
            if (schema.attribute_sizes[i] == 8) {
                std::memcpy(static_cast<char*>(static_cast<void*>(&tmp_uint64_t_value)), src, schema.attribute_sizes[i]);
                line += std::to_string(tmp_uint64_t_value);
            } else if (schema.attribute_sizes[i] == 4) {
                std::memcpy(static_cast<char*>(static_cast<void*>(&tmp_uint32_t_value)), src, schema.attribute_sizes[i]);
                line += std::to_string(tmp_uint32_t_value);
            }
        } else if (schema.attribute_types[i] == FLOAT) {
            if (schema.attribute_sizes[i] == 8) {
                std::memcpy(static_cast<char*>(static_cast<void*>(&tmp_double_value)), src, schema.attribute_sizes[i]);
                line += std::to_string(tmp_double_value);
            } else if (schema.attribute_sizes[i] == 4) {
                std::memcpy(static_cast<char*>(static_cast<void*>(&tmp_float_value)), src, schema.attribute_sizes[i]);
                line += std::to_string(tmp_float_value);
            }
        } else if (schema.attribute_types[i]  == STRING || schema.attribute_types[i] == DATE) {
            line += std::string(src, schema.attribute_sizes[i]);
        } 
        line += std::to_string(separator);
        src += schema.attribute_sizes[i];
    }
}
