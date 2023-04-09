
#ifndef TPCH_DATA_H
#define TPCH_DATA_H
#include <string>
#include <stdlib.h>

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

// TPC-H relations (lineitem, orders)
struct Lineitem {
    uint64_t l_orderkey;
    uint64_t l_partkey;
    uint64_t l_suppkey;
    uint64_t l_linenumber; 
    double l_quantity; 
    double l_extendedprice; 
    double l_discount; 
    double l_tax; 
    char l_returnflag; 
    char l_linestatus;
    char l_shipdate[10]; 
    char l_commitdate[10]; 
    char l_receiptdate[10]; 
    char l_shipinstruct[25]; 
    char l_shipmode[10]; 
    char l_comment[101];
    void line2byteArray(std::string line, char* buff);
    
};

struct Orders {
    uint64_t o_orderkey; 
    uint64_t o_custkey; 
    char o_orderstatus; 
    double o_totalprice; 
    char o_orderdate[10]; 
    char o_orderpriority[15]; 
    char o_clerk[15]; 
    int o_shippriority; 
    char o_comment[101];
};

void LineitemLine2byteArray(std::string line, char* buff);
void OrdersLine2byteArray(std::string line, char* buff);
void JoinedByteArray2line(char* buff, std::string & result);

#endif
