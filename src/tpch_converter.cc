#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <fstream>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#include "args.hxx"
#include "parameters.h"
#include "tpch_data.h"

bool csv2dat_flag;
std::string lineitem_input_path;
std::string lineitem_output_path;
std::string orders_input_path;
std::string orders_output_path;
std::string join_dat_input_path;
std::string join_dat_output_path;
std::string workload_dis_output_path;

int parse_arguments(int argc, char *argv[]);
void csv2dat();
void dat2csv();


int main(int argc, char* argv[]) {
   if(parse_arguments(argc, argv)){
        exit(1);
   }
   if(csv2dat_flag){
       csv2dat();
   }else{
       dat2csv();
   }
   return 0;
}


int parse_arguments(int argc, char *argv[]){
    args::ArgumentParser parser("tpch_data_converter", "");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::Group group1(parser, "This group specifies input/output path names", args::Group::Validators::DontCare);

    args::Group converter_type_group(parser, "The is the group of converter (exclusive):", args::Group::Validators::Xor);
    args::Flag csv2dat_cv_type_cmd(converter_type_group, "CSV->DAT", "Convertiing CSV into dat file", {"CSV2DAT"});
    args::Flag dat2csv_cv_type_cmd(converter_type_group, "DAT->DAT", "Convertiing Joined DAT into csv file", {"DAT2CSV"});

    args::ValueFlag<std::string> lineitem_input_path_cmd(group1, "path", "the default lineitem input path [def: ./linitem.csv]", {"lineitem-input"});
    args::ValueFlag<std::string> lineitem_output_path_cmd(group1, "path", "the default lineitem output path [def: ./linitem.dat]", {"lineitem-output"});
    args::ValueFlag<std::string> orders_input_path_cmd(group1, "path", "the default orders input path [def: ./orders.csv]", {"orders-input"});
    args::ValueFlag<std::string> orders_output_path_cmd(group1, "path", "the default orders output path [def: ./orders.dat]", {"orders-output"});
    args::ValueFlag<std::string> join_dat_input_path_cmd(group1, "path", "the default join dat input path [def: ./join-output.dat]", {"join-dat-input"});
    args::ValueFlag<std::string> join_dat_output_path_cmd(group1, "path", "the default join dat output path [def: ./join-output.csv]", {"join-dat-output"});
    args::ValueFlag<std::string> workload_dis_path_cmd(group1, "path", "the default workload distribution output path [def: ./workload-dis.txt]", {"join-dat-input"});


    try {
        parser.ParseCLI(argc, argv);
    }
    catch (args::Help&) {
        std::cout << parser;
        exit(0);
        // return 0;
     }
     catch (args::ParseError& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
	return 1;
     }
     catch (args::ValidationError& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
	return 1;
     }

     if(csv2dat_cv_type_cmd){
	csv2dat_flag = true;
     }else if(dat2csv_cv_type_cmd){
	csv2dat_flag = false;
     }

     lineitem_input_path = lineitem_input_path_cmd ? args::get(lineitem_input_path_cmd) : "./lineitem.csv";
     orders_input_path = orders_input_path_cmd ? args::get(orders_input_path_cmd) : "./orders.csv";
     join_dat_input_path = join_dat_input_path_cmd ? args::get(join_dat_input_path_cmd) : "./join-output.dat";
     lineitem_output_path = lineitem_output_path_cmd ? args::get(lineitem_output_path_cmd) : "./lineitem.dat";
     orders_output_path = orders_output_path_cmd ? args::get(orders_output_path_cmd) : "./orders.dat";
     join_dat_output_path = join_dat_output_path_cmd ? args::get(join_dat_output_path_cmd) : "./join-output.csv";
     workload_dis_output_path = workload_dis_path_cmd ? args::get(workload_dis_path_cmd) : "./workload-dis.txt";

    return 0;
}


void csv2dat(){
    std::unordered_map<uint64_t, uint64_t>* key2multiplicities = new std::unordered_map<uint64_t, uint64_t> ();
    uint32_t end = 0;
    int tuples_per_page;
    int tuples_counter_in_a_page = 0;
    int left_table_size = 0;
    int right_table_size = 0;
    char buffer[DB_PAGE_SIZE];
    memset(buffer, 0, DB_PAGE_SIZE);
    buffer[DB_PAGE_SIZE] = '\0';


    std::ifstream orders_input_fp;
    std::ofstream orders_output_fp;
    // read orders table;
    std::string line;
    uint64_t orderkey = 0;
    uint32_t orders_entry_size = sizeof(Orders);
    orders_input_fp.open(orders_input_path.c_str(), std::ifstream::binary);
    orders_output_fp.open(orders_output_path.c_str(), std::ofstream::binary);
    tuples_per_page = DB_PAGE_SIZE/orders_entry_size;
    while(std::getline(orders_input_fp, line)){
        end = line.find("|");
	orderkey = std::stoull(line.substr(0, end));
        key2multiplicities->emplace(std::make_pair(orderkey, 0U));
	left_table_size++;
	OrdersLine2byteArray(line, buffer + tuples_counter_in_a_page*orders_entry_size);
	tuples_counter_in_a_page++;
	if(tuples_counter_in_a_page == tuples_per_page){
	   orders_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
	   memset(buffer, 0, DB_PAGE_SIZE);
	}
    }
    if(tuples_counter_in_a_page > 0){
	   orders_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
    }
    orders_output_fp.flush();
    orders_output_fp.close();
    orders_input_fp.close();

    memset(buffer, 0, DB_PAGE_SIZE);
    line.clear();
    std::ifstream lineitem_input_fp;
    std::ofstream lineitem_output_fp;
    // read lineitem table;
    uint32_t lineitem_entry_size = sizeof(Lineitem);
    lineitem_input_fp.open(lineitem_input_path.c_str(), std::ifstream::binary);
    lineitem_output_fp.open(lineitem_output_path.c_str(), std::ofstream::binary);
    tuples_per_page = DB_PAGE_SIZE/lineitem_entry_size;
    while(std::getline(lineitem_input_fp, line)){
        end = line.find("|");
	orderkey = std::stoull(line.substr(0, end));
	if(key2multiplicities->find(orderkey) == key2multiplicities->end()){
	    key2multiplicities->at(orderkey) = 1;
	}else{
	    key2multiplicities->at(orderkey)++;
	}
	right_table_size++;
	LineitemLine2byteArray(line, buffer + tuples_counter_in_a_page*lineitem_entry_size);
	tuples_counter_in_a_page++;
	if(tuples_counter_in_a_page == tuples_per_page){
	   lineitem_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
	   memset(buffer, 0, DB_PAGE_SIZE);
	}
    }
    if(tuples_counter_in_a_page > 0){
	   lineitem_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
    }
    lineitem_output_fp.flush();
    lineitem_output_fp.close();
    lineitem_input_fp.close();

    std::ofstream fp;
    fp.open(workload_dis_output_path.c_str());
    fp << left_table_size << " " << right_table_size << " 8 " << orders_entry_size << " " << lineitem_entry_size << std::endl;

    std::vector<std::pair<uint64_t, uint64_t> > key_multiplicity_to_be_sorted;
    for(auto it = key2multiplicities->begin(); it != key2multiplicities->end(); it++){
	key_multiplicity_to_be_sorted.push_back(std::make_pair(it->second, it->first));
    }
    std::sort(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());
    char bytes[8];
    for(auto i = key_multiplicity_to_be_sorted.size(); i > 0; i--){
        //memcpy(bytes, static_cast<const char*>(static_cast<const void*>(&(key_multiplicity_to_be_sorted[i-1].second ))), 8);
	fp << key_multiplicity_to_be_sorted[i-1].second << " " << key_multiplicity_to_be_sorted[i-1].first << std::endl;
    }
    fp.flush();
    fp.close();
}


void dat2csv(){
    int entry_size = sizeof(Orders) + sizeof(Lineitem) - 8;
    int K = 8;
    int tuples_per_page = DB_PAGE_SIZE/entry_size;

    char buffer[DB_PAGE_SIZE];
    int read_flags = O_RDONLY;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int fp_join_dat_input = open(join_dat_input_path.c_str(), read_flags, read_mode);
    ssize_t read_bytes = 0;

    std::ofstream fp_join_dat_output;
    fp_join_dat_output.open(join_dat_output_path.c_str());
    std::string tmp;
    while(true){
        memset(buffer, 0, DB_PAGE_SIZE);
        read_bytes = read(fp_join_dat_input, buffer, DB_PAGE_SIZE);
	if(read_bytes <= 0) break;
	for(auto i = 0; i < tuples_per_page; i++){
	   tmp.clear();
	   JoinedByteArray2line(buffer + i*entry_size, tmp); 
	   fp_join_dat_output << tmp << std::endl;
	}
    }
    fp_join_dat_output.flush();
    fp_join_dat_output.close();
    close(fp_join_dat_input);
}
