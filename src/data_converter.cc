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
#include "schema.h"

bool csv2dat_flag;
std::string right_table_input_path;
std::string right_table_output_path;
std::string right_table_schema_path;
std::string left_table_input_path;
std::string left_table_output_path;
std::string left_table_schema_path;
std::string join_dat_input_path;
std::string join_dat_output_path;
std::string workload_dis_output_path;
char separator;

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

    args::ValueFlag<std::string> right_table_input_path_cmd(group1, "path", "the default right table input path [def: ./right-table.csv]", {"right-table-input-path"});
    args::ValueFlag<std::string> right_table_output_path_cmd(group1, "path", "the default right table output path [def: ./right-table.dat]", {"right-table-output-path"});
    args::ValueFlag<std::string> right_table_schema_path_cmd(group1, "path", "the default right table schema path [def: ./right-table-schema.txt]", {"right-table-schema-path"});
    args::ValueFlag<std::string> left_table_input_path_cmd(group1, "path", "the default left table input path [def: ./left-table.csv]", {"left-table-input-path"});
    args::ValueFlag<std::string> left_table_output_path_cmd(group1, "path", "the default left table output path [def: ./left-table.dat]", {"left-table-output-path"});
    args::ValueFlag<std::string> left_table_schema_path_cmd(group1, "path", "the default left table schema path [def: ./left-table-schema.txt]", {"left-table-schema-path"});
    args::ValueFlag<std::string> join_dat_input_path_cmd(group1, "path", "the default join dat input path [def: ./join-output.dat]", {"join-dat-input-path"});
    args::ValueFlag<std::string> join_dat_output_path_cmd(group1, "path", "the default join dat output path [def: ./join-output.csv]", {"join-dat-output-path"});
    args::ValueFlag<std::string> workload_dis_path_cmd(group1, "path", "the default workload distribution output path [def: ./workload-dis.txt]", {"join-dat-input-path"});
    args::ValueFlag<char> separator_cmd(group1, "separator", "the separator to parse csv file []def: '|']", {"sep"});


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

    right_table_schema_path = right_table_schema_path_cmd ? args::get(right_table_schema_path_cmd) : "./right-table-schema.txt";
    left_table_schema_path = left_table_schema_path_cmd ? args::get(left_table_schema_path_cmd) : "./left-table-schema.txt";
    right_table_input_path = right_table_input_path_cmd ? args::get(right_table_input_path_cmd) : "./right-table.csv";
    left_table_input_path = left_table_input_path_cmd ? args::get(left_table_input_path_cmd) : "./left-table.csv";
    join_dat_input_path = join_dat_input_path_cmd ? args::get(join_dat_input_path_cmd) : "./join-output.dat";
    right_table_output_path = right_table_output_path_cmd ? args::get(right_table_output_path_cmd) : "./right-table.dat";
    left_table_output_path = left_table_output_path_cmd ? args::get(left_table_output_path_cmd) : "./left-table.dat";
    join_dat_output_path = join_dat_output_path_cmd ? args::get(join_dat_output_path_cmd) : "./join-output.csv";
    workload_dis_output_path = workload_dis_path_cmd ? args::get(workload_dis_path_cmd) : "./workload-dis.txt";
    separator = separator_cmd ? args::get(separator_cmd) : '|';

    return 0;
}


void csv2dat(){
    std::unordered_map<uint64_t, uint64_t>* key2multiplicities = new std::unordered_map<uint64_t, uint64_t> ();
    uint32_t start = 0;
    uint32_t end = 0;
    int tuples_per_page;
    int tuples_counter_in_a_page = 0;
    int left_table_size = 0;
    int right_table_size = 0;
    char buffer[DB_PAGE_SIZE];
    memset(buffer, 0, DB_PAGE_SIZE);
    buffer[DB_PAGE_SIZE] = '\0';

    Schema left_table_schema;
    left_table_schema.read_from_file(left_table_schema_path);
    Schema right_table_schema;
    right_table_schema.read_from_file(right_table_schema_path);


    std::ifstream left_table_input_fp;
    std::ofstream left_table_output_fp;
    // read left_table table;
    std::string line;
    uint64_t left_table_join_key = 0;
    uint64_t right_table_join_key = 0;
    uint32_t left_table_entry_size = left_table_schema.total_size;
    left_table_input_fp.open(left_table_input_path.c_str(), std::ifstream::binary);
    left_table_output_fp.open(left_table_output_path.c_str(), std::ofstream::binary);
    tuples_per_page = DB_PAGE_SIZE/left_table_entry_size;
    uint16_t field_idx = 0;
    while(std::getline(left_table_input_fp, line)){
	field_idx = 0;
	start = 0;
	while (field_idx < left_table_schema.join_key_idx) {
            end = line.find(separator, start);
	    start = end + 1;
	    field_idx++;
	}
        end = line.find(separator, start);
	left_table_join_key = std::stoull(line.substr(start, end - start));
        key2multiplicities->emplace(std::make_pair(left_table_join_key, 0U));
	left_table_size++;
	Line2byteArray(line, buffer + tuples_counter_in_a_page*left_table_entry_size, left_table_schema, separator);
	tuples_counter_in_a_page++;
	if(tuples_counter_in_a_page == tuples_per_page){
	   left_table_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
	   memset(buffer, 0, DB_PAGE_SIZE);
	}
    }
    if(tuples_counter_in_a_page > 0){
	   left_table_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
    }
    left_table_output_fp.flush();
    left_table_output_fp.close();
    left_table_input_fp.close();

    memset(buffer, 0, DB_PAGE_SIZE);
    line.clear();
    std::ifstream right_table_input_fp;
    std::ofstream right_table_output_fp;
    // read right_table table;
    uint32_t right_table_entry_size = right_table_schema.total_size;
    right_table_input_fp.open(right_table_input_path.c_str(), std::ifstream::binary);
    right_table_output_fp.open(right_table_output_path.c_str(), std::ofstream::binary);
    tuples_per_page = DB_PAGE_SIZE/right_table_entry_size;
    while(std::getline(right_table_input_fp, line)){
	field_idx = 0;
	start = 0;
	while (field_idx < right_table_schema.join_key_idx) {
            end = line.find(separator, start);
	    start = end + 1;
	    field_idx++;
	}
        end = line.find(separator, start);
	right_table_join_key = std::stoull(line.substr(start, end - start));
	if(key2multiplicities->find(right_table_join_key) == key2multiplicities->end()){
	    key2multiplicities->at(right_table_join_key) = 1;
	}else{
	    key2multiplicities->at(right_table_join_key)++;
	}
	right_table_size++;
	Line2byteArray(line, buffer + tuples_counter_in_a_page*right_table_entry_size, right_table_schema, separator);
	tuples_counter_in_a_page++;
	if(tuples_counter_in_a_page == tuples_per_page){
	   right_table_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
	   memset(buffer, 0, DB_PAGE_SIZE);
	}
    }
    if(tuples_counter_in_a_page > 0){
	   right_table_output_fp.write((char*)&buffer[0], DB_PAGE_SIZE);
	   tuples_counter_in_a_page = 0;
    }
    right_table_output_fp.flush();
    right_table_output_fp.close();
    right_table_input_fp.close();

    std::ofstream fp;
    fp.open(workload_dis_output_path.c_str());
    fp << left_table_size << " " << right_table_size << " " << left_table_schema.attribute_sizes[left_table_schema.join_key_idx] << " " << left_table_entry_size << " " << right_table_entry_size << std::endl;

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
    Schema left_table_schema;
    left_table_schema.read_from_file(left_table_schema_path);
    Schema right_table_schema;
    right_table_schema.read_from_file(right_table_schema_path);
    

    // assuming the first key in left table is the join key, initialize the joined schema
    int entry_size = left_table_schema.total_size + right_table_schema.total_size - left_table_schema.attribute_sizes[0];
    Schema joined_schema = right_table_schema;
    joined_schema.num_attributes += left_table_schema.num_attributes - 1;
    joined_schema.total_size = entry_size;
    joined_schema.attribute_names.insert(joined_schema.attribute_names.end(), left_table_schema.attribute_names.begin() + 1, left_table_schema.attribute_names.end());
    joined_schema.attribute_types.insert(joined_schema.attribute_types.end(), left_table_schema.attribute_types.begin() + 1, left_table_schema.attribute_types.end());
    joined_schema.attribute_sizes.insert(joined_schema.attribute_sizes.end(), left_table_schema.attribute_sizes.begin() + 1, left_table_schema.attribute_sizes.end());

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
	   ByteArray2line(buffer + i*entry_size, tmp, joined_schema, separator); 
	   fp_join_dat_output << tmp << std::endl;
	}
    }
    fp_join_dat_output.flush();
    fp_join_dat_output.close();
    close(fp_join_dat_input);
}
