
#ifndef TPCH_DATA_H
#define TPCH_DATA_H
#include <string>
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
