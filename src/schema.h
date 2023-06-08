#ifndef SCHEMA_H
#define SCHEMA_H
#include <string>
#include <stdlib.h>
#include <vector>

enum ATTRIBUTE_TYPE {INT = 0x1U, UINT = 0x2U, FLOAT = 0x3U, STRING = 0x4U, DATE = 0x5U};

class Date {
public:
    uint16_t year;
    uint8_t month;
    uint8_t day;
    Date(const std::string & date) { // YYYY-MM-DD
        year = atoi(date.substr(0, 4).c_str());
        month = atoi(date.substr(5, 2).c_str());
        day = atoi(date.substr(8, 2).c_str());
    }

    bool operator<(const Date & date_to_compare) {
	if (date_to_compare.year < year) {
		return false;
	}else if (date_to_compare.year > year){
		return true;
	}else if (date_to_compare.month < month) {
		return false;
	}else if (date_to_compare.month > month) {
		return true;
	}else {
		return date_to_compare.day > day;
	}	
    }
};

class Schema {
public:
    std::vector<std::string> attribute_names;
    std::vector<ATTRIBUTE_TYPE> attribute_types;
    std::vector<std::uint16_t> attribute_sizes;
    uint16_t num_attributes;
    uint32_t total_size;
    void read_from_file(const std::string& file_path);
    void operator = (const Schema& tmp_schema) {
        num_attributes = tmp_schema.num_attributes;
        total_size = tmp_schema.total_size;
        attribute_names = tmp_schema.attribute_names;
        attribute_types = tmp_schema.attribute_types;
        attribute_sizes = tmp_schema.attribute_sizes;
    }
};

void Line2byteArray(std::string line, char* buff, const Schema& schema, const char separator);
void ByteArray2line(char* buff, std::string & line, const Schema& schema, const char separator);

#endif