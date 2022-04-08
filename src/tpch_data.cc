#include "tpch_data.h"
#include <vector>
#include <string>
#include <cstring>
#include <stdio.h>

void LineitemLine2byteArray(std::string line, char* buff){
    memset(buff, 0, sizeof(Lineitem));
    uint32_t start = 0;
    uint32_t end;
    std::string tmp_s;
    char* dest = buff;
    // read l_orderkey
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    uint64_t l_orderkey = std::stoull(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_orderkey)), 8);
    dest += 8;
    start = end + 1;  
    // read l_partkey
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    uint64_t l_partkey = std::stoull(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_partkey)), 8);
    dest += 8;
    start = end + 1;  

    // read l_suppkey
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    uint64_t l_suppkey = std::stoull(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_suppkey)), 8);
    dest += 8;
    start = end + 1;  

    // read l_linenumber
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    uint64_t l_linenumber = std::stoull(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_linenumber)), 8);
    dest += 8;
    start = end + 1; 

    // read l_quantity;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    double l_quantity = std::stod(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_quantity)), 8);
    dest += 8;
    start = end + 1; 

    // read l_extendedprice;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    double l_extendedprice = std::stod(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_extendedprice)), 8);
    dest += 8;
    start = end + 1;

    // read l_discount;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    double l_discount = std::stod(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_discount)), 8);
    dest += 8;
    start = end + 1;

    // read l_tax;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    double l_tax = std::stod(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&l_tax)), 8);
    dest += 8;
    start = end + 1;

    // read l_returnflag;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    char l_returnflag = tmp_s[0];
    std::memcpy(dest, &l_returnflag, 1);
    dest += 1;
    start = end + 1;

    // read l_linestatus;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    char l_linestatus = tmp_s[0];
    std::memcpy(dest, &l_linestatus, 1);
    dest += 1;
    start = end + 1;

    // read l_shipdate;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 10;
    start = end + 1;

    // read l_commitdate;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 10;
    start = end + 1;

    // read l_receiptdate;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 10;
    start = end + 1;

    // read l_shipinstruct;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(),tmp_s.size());
    dest += 25;
    start = end + 1;

    // read l_shipmode;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 10;
    start = end + 1;

    // read l_comment;
    tmp_s = line.substr(start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 101;
    start = end + 1;

    return;
}
void OrdersLine2byteArray(std::string line, char* buff){
    memset(buff, 0, sizeof(Orders));
    uint32_t start = 0;
    uint32_t end;
    std::string tmp_s;
    char* dest = buff;
    // read o_orderkey
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    uint64_t o_orderkey = std::stoull(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&o_orderkey)), 8);
    dest += 8;
    start = end + 1;  
    // read o_custkey
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    uint64_t o_custkey = std::stoull(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&o_custkey)), 8);
    dest += 8;
    start = end + 1;  

    // read o_orderstatus;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    char o_orderstatus = tmp_s[0];
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&o_orderstatus)), 1);
    dest += 1;
    start = end + 1; 

    // read o_totalprice;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    double o_totalprice = std::stod(tmp_s);
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&o_totalprice)), 8);
    dest += 8;
    start = end + 1;

    // read o_orderdate;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 10;
    start = end + 1;

    // read o_orderpriority;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 15;
    start = end + 1;

    // read o_clerk;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 15;
    start = end + 1;

    // read o_shippriority;
    end = line.find("|",start);
    tmp_s = line.substr(start, end-start);
    int o_shippriority = std::stoi(tmp_s.c_str());
    std::memcpy(dest, static_cast<char*>(static_cast<void*>(&o_shippriority)), 4);
    dest += 4;
    start = end + 1;

    // read o_comment;
    tmp_s = line.substr(start);
    std::memcpy(dest, tmp_s.c_str(), tmp_s.size());
    dest += 101;
    start = end + 1;

    return;
}

void JoinedByteArray2line(char* buff, std::string & result){
    result.clear();
    char* src = buff;
    Lineitem l_instance;
    Orders o_instance;

    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_orderkey)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_orderkey) + "|";
    
    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_partkey)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_partkey) + "|";
     
    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_suppkey)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_suppkey) + "|";

    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_linenumber)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_linenumber) + "|";


    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_quantity)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_quantity) + "|";

    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_extendedprice)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_extendedprice) + "|";

    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_discount)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_discount) + "|";

    memcpy(static_cast<char*>(static_cast<void*>(&l_instance.l_tax)), src, 8);
    src += 8;
    result += std::to_string(l_instance.l_tax) + "|";

    result += (*src) + "|";
    src += 1;

    result += (*src) + "|";
    src += 1;

    result += std::string(src, 10) + "|";
    src += 10;

    result += std::string(src, 10) + "|";
    src += 10;

    result += std::string(src, 10) + "|";
    src += 10;

    result += std::string(src, 25) + "|";
    src += 25;

    result += std::string(src, 10) + "|";
    src += 10;

    result += std::string(src, 101) + "|";
    src += 101;

    memcpy(static_cast<char*>(static_cast<void*>(&o_instance.o_orderkey)), src, 8);
    src += 8;
    result += std::to_string(o_instance.o_orderkey) + "|";

    memcpy(static_cast<char*>(static_cast<void*>(&o_instance.o_custkey)), src, 8);
    src += 8;
    result += std::to_string(o_instance.o_custkey) + "|";

    result += (*src) + "|";
    src += 1;


    memcpy(static_cast<char*>(static_cast<void*>(&o_instance.o_totalprice)), src, 8);
    src += 8;
    result += std::to_string(o_instance.o_totalprice) + "|";

    result += std::string(src, 10) + "|";
    src += 10;

    result += std::string(src, 15) + "|";
    src += 15;

    memcpy(static_cast<char*>(static_cast<void*>(&o_instance.o_shippriority)), src, 8);
    src += 8;
    result += std::to_string(o_instance.o_shippriority) + "|";

    result += std::string(src, 101) + "|";
    src += 101;

}
