#include "schema.h"
#include <vector>
#include <algorithm>
#include <string>
#include <cstring>
#include <stdio.h>
#include <iostream>
#include <fstream>


size_t findNextSeparatorPos(const std::string line, uint32_t start, char separator) {
    bool quote = false;
    for (uint32_t i = start; i < line.length(); i++) {
        if (line[i] == separator && !quote) {
            return i;
        }
        if (line[i] == '"' && (i > 0 && line[i - 1] != '\\')) {
            quote = !quote;
        }
    }
    return std::string::npos;
}

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
    uint32_t start = 0;
    uint32_t end;
    std::getline(fp, line);

    // read num_attributes
    end = line.find(" ", start);
    this->num_attributes = std::stoul(line.substr(start, end - start));
    this->join_key_idx = std::stoul(line.substr(end));
    this->total_size = 0;
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
    if (this->join_key_idx != 0) {
        std::iter_swap(this->attribute_names.begin(), this->attribute_names.begin() + this->join_key_idx);
        std::iter_swap(this->attribute_types.begin(), this->attribute_types.begin() + this->join_key_idx);
        std::iter_swap(this->attribute_sizes.begin(), this->attribute_sizes.begin() + this->join_key_idx);
    }
    fp.close();
}


void ByteArray2String(const std::string & raw_str, std::string & result_string, const ATTRIBUTE_TYPE & attribute_type, uint16_t attribute_size) {
    result_string.clear();
    if (attribute_type == STRING || attribute_type == DATE) {
        result_string = raw_str;
        return;
    }

   uint64_t tmp_uint64_t_key = 0;
   uint32_t tmp_uint32_t_key = 0;
   long long tmp_long_long_key = 0;
   long tmp_long_key = 0;
   float tmp_float_key = 0;
   double tmp_double_key = 0;

   if (attribute_type == INT) {
       if (attribute_size == 8) {
           memcpy(&tmp_long_long_key, raw_str.c_str(), 8);
           result_string = std::to_string(tmp_long_long_key);	
       } else if (attribute_size == 4) {
           memcpy(&tmp_long_key, raw_str.c_str(), 4);
           result_string = std::to_string(tmp_long_key);	
       }
   } else if (attribute_type == UINT) {
       if (attribute_size == 8) {
           memcpy(&tmp_uint64_t_key, raw_str.c_str(), 8);
           result_string = std::to_string(tmp_uint64_t_key);	
       } else if (attribute_size == 4) {
           memcpy(&tmp_uint32_t_key, raw_str.c_str(), 4);
           result_string = std::to_string(tmp_uint32_t_key);	
       }
   } else if (attribute_type == FLOAT) {
       if (attribute_size == 8) {
           memcpy(&tmp_double_key, raw_str.c_str(), 8);
           result_string = std::to_string(tmp_double_key);	
       } else if (attribute_size == 4) {
           memcpy(&tmp_float_key, raw_str.c_str(), 4);
           result_string = std::to_string(tmp_float_key);	
       }
   }
}


void String2ByteArray(const std::string & raw_str, char* buff, const ATTRIBUTE_TYPE & attribute_type, uint16_t attribute_size) {
    memset(buff, 0, attribute_size);
    if (raw_str.size() == 0) return; 
    // empty string (we fulfill the bytestring which means parsing the empty string could end with 0)
    // we may assume 0 is a special value that indicates a NULL value for this field for simplicity
    uint64_t tmp_uint64_t_value = 0; 
    uint32_t tmp_uint32_t_value = 0;
    long long tmp_long_long_value = 0;
    long tmp_long_value = 0;
    float tmp_float_value = 0;
    double tmp_double_value = 0;
    std::string tmp_string_value = "";

    if (attribute_type == INT) {
            if (attribute_size == 8) {
                tmp_long_long_value = std::stoll(raw_str);
                std::memcpy(buff, static_cast<char*>(static_cast<void*>(&tmp_long_long_value)), 8);
            } else if (attribute_size == 4) {
                tmp_long_value = std::stol(raw_str);
                std::memcpy(buff, static_cast<char*>(static_cast<void*>(&tmp_long_value)), 4);
            }
    } else if (attribute_type == UINT) {
            if (attribute_size == 8) {
                tmp_uint64_t_value = std::stoull(raw_str);
                std::memcpy(buff, static_cast<char*>(static_cast<void*>(&tmp_uint64_t_value)), 8);
            } else if (attribute_size == 4) {
                tmp_uint32_t_value = std::stoul(raw_str);
                std::memcpy(buff, static_cast<char*>(static_cast<void*>(&tmp_uint32_t_value)), 4);
            }
    } else if (attribute_type == FLOAT) {
            if (attribute_size == 8) {
                tmp_double_value = std::stod(raw_str);
                std::memcpy(buff, static_cast<char*>(static_cast<void*>(&tmp_double_value)), 8);
            } else if (attribute_size == 4) {
                tmp_float_value = std::stof(raw_str);
                std::memcpy(buff, static_cast<char*>(static_cast<void*>(&tmp_float_value)), 4);
            }
    } else if (attribute_type  == STRING || attribute_type == DATE) {
            std::memcpy(buff, raw_str.c_str(), raw_str.size());
    }

}



void Line2ByteArray(std::string line, char* buff, const Schema& schema, const char separator) {
    memset(buff, 0, schema.total_size);
    uint32_t start = 0;
    uint32_t end;
    char* dest = buff;

    uint32_t tmp_start, tmp_end;
    uint32_t i = 0;
    while (i < schema.join_key_idx) {
        end = findNextSeparatorPos(line, start, separator);
        start = end + 1;
        i++;
    }
    end = line.find(separator,start);
    // join key meta info has been swapped with the first field
    String2ByteArray(line.substr(start, end - start), dest, schema.attribute_types[0], schema.attribute_sizes[0]);	
    dest += schema.attribute_sizes[0];
    start = 0;
    // ensure the join key is written in the first field in a byte array
    for(i = 0; i < schema.num_attributes; i++) {
        end = findNextSeparatorPos(line, start, separator);
        if (i == schema.join_key_idx) {
            start = end + 1;  
	        continue;
	    } else if (i == 0) {
	        String2ByteArray(line.substr(start, end - start), dest, schema.attribute_types[schema.join_key_idx], schema.attribute_sizes[schema.join_key_idx]);
            dest += schema.attribute_sizes[schema.join_key_idx];
	    } else {
            String2ByteArray(line.substr(start, end - start), dest, schema.attribute_types[i], schema.attribute_sizes[i]);	
            dest += schema.attribute_sizes[i];
	    }
        start = end + 1;  
    }

    return;
}

void ByteArray2Line(char* buff, std::string & line, const Schema& schema, const char separator) {
    line.clear();
    char* src = buff;
     
    std::string tmp_string_value = "";
    for (uint32_t i = 0; i < schema.num_attributes; i++) {
	ByteArray2String(std::string(src, schema.attribute_sizes[i]), tmp_string_value, schema.attribute_types[i], schema.attribute_sizes[i]);
	line += tmp_string_value + std::to_string(separator);  
        src += schema.attribute_sizes[i];
    }
}
